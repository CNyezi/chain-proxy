from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
sys.path.insert(0, str(APP_DIR))


def load_chain_proxy(**env: str):
    keys = [
        "CHAIN_TEST_URL",
        "DNS_NAMESERVERS",
        "DNS_PROXY_SERVER_NAMESERVERS",
        "ENABLE_TCP_CONCURRENT",
        "ENABLE_KEEP_ALIVE",
        "KEEP_ALIVE_IDLE_SEC",
        "UNIFIED_DELAY",
        "WATCH_PRECHECK_LIMIT",
        "WATCH_EXPLORATION_SLOTS",
        "WATCH_CONCURRENCY",
        "SWITCH_MARGIN_MS",
        "SWITCH_STABLE_ROUNDS",
        "EWMA_ALPHA",
        "FAIL_PENALTY_MS",
        "FAIL_COOLDOWN_ROUNDS",
        "EXCLUDE_PROXY_KEYWORDS",
    ]
    with mock.patch.dict(os.environ, env, clear=False):
        for key in keys:
            if key not in env:
                os.environ.pop(key, None)
        sys.modules.pop("chain_proxy", None)
        return importlib.import_module("chain_proxy")


class BuildConfigTest(unittest.TestCase):
    def test_build_config_includes_dns_and_transport_optimizations(self) -> None:
        chain_proxy = load_chain_proxy(
            SOCKS_SERVER="final.example",
            SOCKS_PORT="1080",
            SOCKS_USERNAME="user",
            SOCKS_PASSWORD="pass",
            DNS_NAMESERVERS="https://dns.google/dns-query,1.1.1.1",
            DNS_PROXY_SERVER_NAMESERVERS="https://dns.alidns.com/dns-query,223.5.5.5",
            ENABLE_TCP_CONCURRENT="true",
            ENABLE_KEEP_ALIVE="true",
            KEEP_ALIVE_IDLE_SEC="45",
            UNIFIED_DELAY="true",
            EXCLUDE_PROXY_KEYWORDS="慢节点",
        )
        subscription = yaml.safe_dump(
            {
                "proxies": [
                    {"name": "fast", "type": "ss", "server": "fast.example", "port": 443},
                    {"name": "慢节点-1", "type": "ss", "server": "slow.example", "port": 443},
                ]
            }
        ).encode()

        config, count = chain_proxy.build_config(subscription)

        self.assertEqual(count, 1)
        self.assertEqual(config["tcp-concurrent"], True)
        self.assertEqual(config["keep-alive-idle"], 45)
        self.assertEqual(config["unified-delay"], True)
        self.assertEqual(config["dns"]["nameserver"], ["https://dns.google/dns-query", "1.1.1.1"])
        self.assertEqual(
            config["dns"]["proxy-server-nameserver"],
            ["https://dns.alidns.com/dns-query", "223.5.5.5"],
        )
        self.assertEqual(config["proxy-groups"][0]["proxies"][0].split("::")[-1], "fast")


class ChainSelectorTest(unittest.TestCase):
    def test_selector_uses_ewma_fail_penalty_precheck_and_cooldown(self) -> None:
        chain_proxy = load_chain_proxy(
            WATCH_PRECHECK_LIMIT="2",
            WATCH_CONCURRENCY="4",
            SWITCH_MARGIN_MS="30",
            SWITCH_STABLE_ROUNDS="2",
            EWMA_ALPHA="0.5",
            FAIL_PENALTY_MS="500",
            FAIL_COOLDOWN_ROUNDS="2",
        )
        selector = chain_proxy.ChainSelector()

        self.assertEqual(selector.pick_candidates(["a", "b", "c", "d"]), ["a", "b"])

        decision = selector.record_round({"a": 200, "b": 260})
        self.assertEqual(decision.selected, "a")

        decision = selector.record_round({"a": 220, "b": 160})
        self.assertEqual(decision.selected, "a")

        decision = selector.record_round({"a": 220, "b": 150})
        self.assertEqual(decision.selected, "a")

        decision = selector.record_round({"a": 220, "b": 150})
        self.assertEqual(decision.selected, "b")
        self.assertLess(selector.stats["b"].ewma_ms, selector.stats["a"].ewma_ms)

        selector.record_round({"a": None, "b": 150})
        selector.record_round({"a": None, "b": 150})
        self.assertNotIn("a", selector.pick_candidates(["a", "b", "c"]))


if __name__ == "__main__":
    unittest.main()
