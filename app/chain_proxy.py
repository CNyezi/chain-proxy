#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
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
TEST_URL = os.getenv("CHAIN_TEST_URL", "https://claude.ai/")
WATCH_INTERVAL = int(os.getenv("WATCH_INTERVAL_SEC", "60"))
TIMEOUT_MS = int(os.getenv("WATCH_TIMEOUT_MS", "6000"))
WATCH_CONCURRENCY = int(os.getenv("WATCH_CONCURRENCY", "16"))
SWITCH_MARGIN_MS = int(os.getenv("SWITCH_MARGIN_MS", "150"))
MIHOMO_LOG_LEVEL = os.getenv("MIHOMO_LOG_LEVEL", "info")
SUB_FETCH_PROXY = os.getenv("SUB_FETCH_PROXY", "")

CONTROLLER_URL = f"http://127.0.0.1:{CONTROLLER_PORT}"
CHAIN_GROUP = "CHAIN"
FINAL_PREFIX = "FINAL_SOCKS5::"

stop_event = threading.Event()
reload_event = threading.Event()
mihomo_lock = threading.Lock()
mihomo_proc: subprocess.Popen[bytes] | None = None


def log(message: str) -> None:
    print(f"[chain-proxy] {message}", flush=True)


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
    first_hops = unique_proxy_names(loaded.get("proxies", []))
    if not first_hops:
        raise ValueError("subscription contains no usable proxies")

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

    config = {
        "mixed-port": MIXED_PORT,
        "bind-address": "*",
        "allow-lan": True,
        "mode": "rule",
        "log-level": MIHOMO_LOG_LEVEL,
        "external-controller": f"0.0.0.0:{CONTROLLER_PORT}",
        "secret": CONTROLLER_SECRET,
        "dns": {
            "enable": True,
            "listen": "0.0.0.0:1053",
            "enhanced-mode": "fake-ip",
            "nameserver": ["1.1.1.1", "8.8.8.8"],
        },
        "proxies": all_proxies,
        "proxy-groups": [{"name": CHAIN_GROUP, "type": "select", "proxies": chain_proxies}],
        "rules": [f"MATCH,{CHAIN_GROUP}"],
    }
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


def select_chain(name: str) -> None:
    controller_request("PUT", f"/proxies/{quote_name(CHAIN_GROUP)}", {"name": name})


def watchdog_loop() -> None:
    current: str | None = None
    current_score: int | None = None
    wait_for_controller()
    log("watchdog started")
    while not stop_event.is_set():
        try:
            candidates = get_chain_candidates()
            if not candidates:
                log("watchdog found no chain candidates")
                stop_event.wait(WATCH_INTERVAL)
                continue

            workers = max(1, min(WATCH_CONCURRENCY, len(candidates)))
            scores: list[tuple[int, str]] = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(delay_candidate, name) for name in candidates]
                for future in concurrent.futures.as_completed(futures):
                    name, score = future.result()
                    if score is not None:
                        scores.append((score, name))

            if not scores:
                log("watchdog found no healthy chain candidates")
                stop_event.wait(WATCH_INTERVAL)
                continue

            scores.sort(key=lambda item: item[0])
            best_score, best_name = scores[0]
            healthy = {name for _, name in scores}
            should_switch = (
                current is None
                or current_score is None
                or current not in healthy
                or best_score + SWITCH_MARGIN_MS < current_score
            )

            if should_switch:
                select_chain(best_name)
                current, current_score = best_name, best_score
                log(f"selected chain_delay_ms={best_score} name={best_name!r}")
            else:
                select_chain(current)
                current_score = next((score for score, name in scores if name == current), current_score)
                log(f"kept chain_delay_ms={current_score} best_ms={best_score} name={current!r}")
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
