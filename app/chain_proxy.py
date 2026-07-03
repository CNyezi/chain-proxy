#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import dataclasses
import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import yaml


DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CONFIG_PATH = DATA_DIR / "config.yaml"
SUB_CACHE_PATH = DATA_DIR / "subscription.yaml"
MIHOMO_BIN = os.getenv("MIHOMO_BIN", "/usr/local/bin/mihomo")

SUB_URL = os.getenv("SUB_URL", "")
SOCKS_SERVER = os.getenv("SOCKS_SERVER", "")
SOCKS_PORT = int(os.getenv("SOCKS_PORT", "0"))
SOCKS_USERNAME = os.getenv("SOCKS_USERNAME", "")
SOCKS_PASSWORD = os.getenv("SOCKS_PASSWORD", "")
MIXED_PORT = int(os.getenv("MIXED_PORT", "17898"))
CONTROLLER_PORT = 9090
CONTROLLER_SECRET = os.getenv("CONTROLLER_SECRET", "")
SUB_UPDATE_INTERVAL = int(os.getenv("SUB_UPDATE_INTERVAL", "3600"))
TEST_URL = os.getenv("CHAIN_TEST_URL", "https://api.anthropic.com/")
WATCH_INTERVAL = int(os.getenv("WATCH_INTERVAL_SEC", "60"))
TIMEOUT_MS = int(os.getenv("WATCH_TIMEOUT_MS", "5000"))
WATCH_CONCURRENCY = int(os.getenv("WATCH_CONCURRENCY", "8"))
WATCH_PRECHECK_LIMIT = int(os.getenv("WATCH_PRECHECK_LIMIT", "12"))
WATCH_EXPLORATION_SLOTS = int(os.getenv("WATCH_EXPLORATION_SLOTS", "2"))
SWITCH_MARGIN_MS = int(os.getenv("SWITCH_MARGIN_MS", "150"))
SWITCH_STABLE_ROUNDS = max(1, int(os.getenv("SWITCH_STABLE_ROUNDS", "2")))
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.35"))
FAIL_PENALTY_MS = int(os.getenv("FAIL_PENALTY_MS", "1000"))
FAIL_COOLDOWN_ROUNDS = int(os.getenv("FAIL_COOLDOWN_ROUNDS", "3"))
MIHOMO_LOG_LEVEL = os.getenv("MIHOMO_LOG_LEVEL", "info")
SUB_FETCH_PROXY = os.getenv("SUB_FETCH_PROXY", "")
EXCLUDE_PROXY_KEYWORDS = tuple(
    item.strip() for item in os.getenv("EXCLUDE_PROXY_KEYWORDS", "").split(",") if item.strip()
)
DNS_NAMESERVERS = os.getenv("DNS_NAMESERVERS", "223.5.5.5,119.29.29.29,1.1.1.1")
DNS_PROXY_SERVER_NAMESERVERS = os.getenv(
    "DNS_PROXY_SERVER_NAMESERVERS",
    DNS_NAMESERVERS,
)
ENABLE_TCP_CONCURRENT = os.getenv("ENABLE_TCP_CONCURRENT", "true").lower() in {"1", "true", "yes", "on"}
ENABLE_KEEP_ALIVE = os.getenv("ENABLE_KEEP_ALIVE", "true").lower() in {"1", "true", "yes", "on"}
KEEP_ALIVE_IDLE_SEC = int(os.getenv("KEEP_ALIVE_IDLE_SEC", "30"))
UNIFIED_DELAY = os.getenv("UNIFIED_DELAY", "true").lower() in {"1", "true", "yes", "on"}

CONTROLLER_URL = f"http://127.0.0.1:{CONTROLLER_PORT}"
CHAIN_GROUP = "CHAIN"
FINAL_PREFIX = "FINAL_SOCKS5::"

stop_event = threading.Event()
reload_event = threading.Event()
mihomo_lock = threading.Lock()
mihomo_proc: subprocess.Popen[bytes] | None = None


def log(message: str) -> None:
    print(f"[chain-proxy] {message}", flush=True)


def env_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def require_env() -> None:
    missing = [
        key
        for key, value in {
            "SUB_URL": SUB_URL,
            "SOCKS_SERVER": SOCKS_SERVER,
            "SOCKS_PORT": SOCKS_PORT,
            "SOCKS_USERNAME": SOCKS_USERNAME,
            "SOCKS_PASSWORD": SOCKS_PASSWORD,
            "CONTROLLER_SECRET": CONTROLLER_SECRET,
        }.items()
        if not value
    ]
    if missing:
        raise SystemExit(f"Missing required env: {', '.join(missing)}")


def fetch_subscription() -> bytes:
    handlers: list[Any] = []
    if SUB_FETCH_PROXY:
        handlers.append(urllib.request.ProxyHandler({"http": SUB_FETCH_PROXY, "https": SUB_FETCH_PROXY}))
    opener = urllib.request.build_opener(*handlers)
    req = urllib.request.Request(SUB_URL, headers={"User-Agent": "chain-proxy/1.0"})
    with opener.open(req, timeout=max(10, TIMEOUT_MS / 1000)) as resp:
        return resp.read()


def unique_proxy_names(proxies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, int] = {}
    result: list[dict[str, Any]] = []
    for proxy in proxies:
        if not isinstance(proxy, dict) or not proxy.get("name") or not proxy.get("type"):
            continue
        item = dict(proxy)
        base = str(item["name"])
        count = seen.get(base, 0)
        seen[base] = count + 1
        if count:
            item["name"] = f"{base} #{count + 1}"
        else:
            item["name"] = base
        result.append(item)
    return result


def chain_name(index: int, first_hop: str) -> str:
    digest = hashlib.sha1(first_hop.encode("utf-8")).hexdigest()[:8]
    return f"{FINAL_PREFIX}{index:03d}::{digest}::{first_hop}"


def build_config(subscription: bytes) -> tuple[dict[str, Any], int]:
    loaded = yaml.safe_load(subscription.decode("utf-8", errors="replace"))
    if not isinstance(loaded, dict):
        raise ValueError("subscription is not a Clash YAML object")
    first_hops = [
        proxy
        for proxy in unique_proxy_names(loaded.get("proxies", []))
        if not any(keyword in proxy["name"] for keyword in EXCLUDE_PROXY_KEYWORDS)
    ]
    if not first_hops:
        raise ValueError("subscription contains no usable proxies after filters")

    all_proxies = list(first_hops)
    chain_proxies: list[str] = []
    for index, proxy in enumerate(first_hops, 1):
        name = chain_name(index, proxy["name"])
        chain_proxies.append(name)
        all_proxies.append(
            {
                "name": name,
                "type": "socks5",
                "server": SOCKS_SERVER,
                "port": SOCKS_PORT,
                "username": SOCKS_USERNAME,
                "password": SOCKS_PASSWORD,
                "udp": False,
                "dialer-proxy": proxy["name"],
            }
        )

    dns_config = {
        "enable": True,
        "listen": "0.0.0.0:1053",
        "enhanced-mode": "fake-ip",
        "nameserver": env_list(DNS_NAMESERVERS),
    }
    proxy_server_nameservers = env_list(DNS_PROXY_SERVER_NAMESERVERS)
    if proxy_server_nameservers:
        dns_config["proxy-server-nameserver"] = proxy_server_nameservers

    config = {
        "mixed-port": MIXED_PORT,
        "bind-address": "*",
        "allow-lan": True,
        "mode": "rule",
        "log-level": MIHOMO_LOG_LEVEL,
        "tcp-concurrent": ENABLE_TCP_CONCURRENT,
        "unified-delay": UNIFIED_DELAY,
        "external-controller": f"0.0.0.0:{CONTROLLER_PORT}",
        "secret": CONTROLLER_SECRET,
        "dns": dns_config,
        "proxies": all_proxies,
        "proxy-groups": [{"name": CHAIN_GROUP, "type": "select", "proxies": chain_proxies}],
        "rules": [f"MATCH,{CHAIN_GROUP}"],
    }
    if ENABLE_KEEP_ALIVE:
        config["keep-alive-idle"] = KEEP_ALIVE_IDLE_SEC
    return config, len(chain_proxies)


def write_config(subscription: bytes) -> bool:
    config, count = build_config(subscription)
    rendered = yaml.safe_dump(config, allow_unicode=True, sort_keys=False).encode("utf-8")
    previous = CONFIG_PATH.read_bytes() if CONFIG_PATH.exists() else b""
    if rendered == previous:
        log(f"subscription unchanged, chain candidates={count}")
        return False
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SUB_CACHE_PATH.write_bytes(subscription)
    CONFIG_PATH.write_bytes(rendered)
    log(f"wrote mihomo config, chain candidates={count}")
    return True


def controller_request(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Authorization": f"Bearer {CONTROLLER_SECRET}"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(CONTROLLER_URL + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=max(3, TIMEOUT_MS / 1000)) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8")) if raw else {}


def quote_name(name: str) -> str:
    return urllib.parse.quote(name, safe="")


def wait_for_controller() -> None:
    while not stop_event.is_set():
        try:
            controller_request("GET", "/proxies")
            return
        except Exception:
            time.sleep(0.5)


def start_mihomo() -> None:
    global mihomo_proc
    with mihomo_lock:
        if mihomo_proc and mihomo_proc.poll() is None:
            return
        mihomo_proc = subprocess.Popen([MIHOMO_BIN, "-f", str(CONFIG_PATH)])
        log(f"mihomo started pid={mihomo_proc.pid}")


def stop_mihomo() -> None:
    global mihomo_proc
    with mihomo_lock:
        proc = mihomo_proc
        if not proc or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
        log("mihomo stopped")


def restart_mihomo() -> None:
    stop_mihomo()
    start_mihomo()


def subscription_loop() -> None:
    while not stop_event.is_set():
        try:
            changed = write_config(fetch_subscription())
            if changed:
                reload_event.set()
        except Exception as exc:
            log(f"subscription update failed: {exc}")
        stop_event.wait(SUB_UPDATE_INTERVAL)


def get_chain_candidates() -> list[str]:
    group = controller_request("GET", f"/proxies/{quote_name(CHAIN_GROUP)}")
    return [name for name in group.get("all", []) if isinstance(name, str) and name.startswith(FINAL_PREFIX)]


def delay_candidate(name: str) -> tuple[str, int | None]:
    # This delay is measured on the expanded chain proxy, not on the first-hop
    # subscription proxy. The request path is:
    # mihomo -> first hop -> final SOCKS5 -> TEST_URL.
    path = (
        f"/proxies/{quote_name(name)}/delay"
        f"?timeout={TIMEOUT_MS}&url={urllib.parse.quote(TEST_URL, safe='')}"
    )
    try:
        data = controller_request("GET", path)
        delay = data.get("delay")
        return name, int(delay) if delay is not None else None
    except Exception as exc:
        log(f"candidate failed name={name!r} err={exc}")
        return name, None


@dataclasses.dataclass
class ChainStats:
    ewma_ms: float | None = None
    failures: int = 0
    cooldown_rounds: int = 0


@dataclasses.dataclass
class ChainDecision:
    selected: str
    score_ms: int | None
    best: str
    best_score_ms: int | None
    switched: bool


class ChainSelector:
    def __init__(self) -> None:
        self.stats: dict[str, ChainStats] = {}
        self.current: str | None = None
        self.faster_rounds = 0
        self.explore_cursor = 0

    def pick_candidates(self, candidates: list[str]) -> list[str]:
        if WATCH_PRECHECK_LIMIT <= 0 or len(candidates) <= WATCH_PRECHECK_LIMIT:
            return candidates

        active = [name for name in candidates if self.stats.get(name, ChainStats()).cooldown_rounds <= 0]
        if not active:
            active = candidates

        def rank(name: str) -> tuple[float, str]:
            stats = self.stats.get(name)
            if not stats or stats.ewma_ms is None:
                return float("inf"), name
            return stats.ewma_ms + stats.failures * FAIL_PENALTY_MS, name

        known = [name for name in active if self.stats.get(name, ChainStats()).ewma_ms is not None]
        unknown = [name for name in active if self.stats.get(name, ChainStats()).ewma_ms is None]
        picked: list[str] = []
        if self.current in active:
            picked.append(self.current)

        exploration_slots = max(0, min(WATCH_EXPLORATION_SLOTS, WATCH_PRECHECK_LIMIT - len(picked)))
        if unknown and exploration_slots:
            start = self.explore_cursor % len(unknown)
            rotated = unknown[start:] + unknown[:start]
            picked.extend(name for name in rotated[:exploration_slots] if name not in picked)
            self.explore_cursor = (start + exploration_slots) % len(unknown)

        picked.extend(name for name in sorted(known, key=rank) if name not in picked)
        picked.extend(name for name in unknown if name not in picked)
        return picked[:WATCH_PRECHECK_LIMIT]

    def record_round(self, scores: dict[str, int | None]) -> ChainDecision:
        for stats in self.stats.values():
            if stats.cooldown_rounds > 0:
                stats.cooldown_rounds -= 1

        for name, score in scores.items():
            stats = self.stats.setdefault(name, ChainStats())
            if score is None:
                stats.failures += 1
                stats.cooldown_rounds = max(stats.cooldown_rounds, FAIL_COOLDOWN_ROUNDS)
                continue
            stats.failures = 0
            stats.cooldown_rounds = 0
            stats.ewma_ms = (
                float(score)
                if stats.ewma_ms is None
                else EWMA_ALPHA * float(score) + (1.0 - EWMA_ALPHA) * stats.ewma_ms
            )

        ranked = [
            (self._effective_score(name), name)
            for name, stats in self.stats.items()
            if stats.ewma_ms is not None and stats.cooldown_rounds <= 0
        ]
        if not ranked:
            ranked = [
                (self._effective_score(name), name)
                for name, stats in self.stats.items()
                if stats.ewma_ms is not None
            ]
        if not ranked:
            raise ValueError("no healthy chain candidates")

        ranked.sort(key=lambda item: (item[0], item[1]))
        best_score, best_name = ranked[0]
        current_score = self._effective_score(self.current) if self.current else None
        must_switch = self.current is None or current_score is None
        if not must_switch and best_name != self.current and best_score + SWITCH_MARGIN_MS < current_score:
            self.faster_rounds += 1
        else:
            self.faster_rounds = 0

        should_switch = must_switch or self.faster_rounds >= SWITCH_STABLE_ROUNDS
        if should_switch:
            self.current = best_name
            self.faster_rounds = 0

        selected = self.current or best_name
        selected_score = self._effective_score(selected)
        return ChainDecision(
            selected=selected,
            score_ms=round(selected_score) if selected_score is not None else None,
            best=best_name,
            best_score_ms=round(best_score),
            switched=should_switch,
        )

    def _effective_score(self, name: str | None) -> float | None:
        if name is None:
            return None
        stats = self.stats.get(name)
        if not stats or stats.ewma_ms is None:
            return None
        return stats.ewma_ms + stats.failures * FAIL_PENALTY_MS


def select_chain(name: str) -> None:
    controller_request("PUT", f"/proxies/{quote_name(CHAIN_GROUP)}", {"name": name})


def watchdog_loop() -> None:
    selector = ChainSelector()
    wait_for_controller()
    log("watchdog started")
    while not stop_event.is_set():
        try:
            candidates = get_chain_candidates()
            if not candidates:
                log("watchdog found no chain candidates")
                stop_event.wait(WATCH_INTERVAL)
                continue

            candidates = selector.pick_candidates(candidates)
            workers = max(1, min(WATCH_CONCURRENCY, len(candidates)))
            scores: dict[str, int | None] = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(delay_candidate, name) for name in candidates]
                for future in concurrent.futures.as_completed(futures):
                    name, score = future.result()
                    scores[name] = score

            if not scores:
                log("watchdog found no healthy chain candidates")
                stop_event.wait(WATCH_INTERVAL)
                continue

            decision = selector.record_round(scores)
            if decision.switched:
                select_chain(decision.selected)
                log(
                    "selected "
                    f"chain_score_ms={decision.score_ms} best_ms={decision.best_score_ms} "
                    f"name={decision.selected!r}"
                )
            else:
                select_chain(decision.selected)
                log(
                    "kept "
                    f"chain_score_ms={decision.score_ms} best_ms={decision.best_score_ms} "
                    f"name={decision.selected!r}"
                )
        except Exception as exc:
            log(f"watchdog loop error: {exc}")
            wait_for_controller()
        stop_event.wait(WATCH_INTERVAL)


def signal_handler(signum: int, _frame: Any) -> None:
    log(f"received signal={signum}")
    stop_event.set()


def main() -> None:
    require_env()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    changed = write_config(fetch_subscription())
    log(f"initial config ready changed={changed}")
    start_mihomo()

    subscription_thread = threading.Thread(target=subscription_loop, daemon=True)
    watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
    subscription_thread.start()
    watchdog_thread.start()

    while not stop_event.is_set():
        if reload_event.is_set():
            reload_event.clear()
            restart_mihomo()
            wait_for_controller()
        with mihomo_lock:
            proc = mihomo_proc
        if proc and proc.poll() is not None:
            log(f"mihomo exited code={proc.returncode}, restarting")
            start_mihomo()
        stop_event.wait(1)

    stop_mihomo()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"fatal: {exc}")
        sys.exit(1)
