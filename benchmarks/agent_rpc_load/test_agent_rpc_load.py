# SPDX-License-Identifier: MIT OR Apache-2.0

from __future__ import annotations

import hashlib
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from agent_rpc_load import (
    JsonRpcClient,
    Metrics,
    ReplayRecord,
    RpcCallResult,
    load_replay_records,
    run_read_workload,
    run_replay_workload,
    synthetic_address,
)


class MockRpcState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.calls: list[str] = []
        self.receipt_polls: dict[str, int] = {}

    def response(self, request: dict[str, Any]) -> dict[str, Any]:
        method = request["method"]
        with self.lock:
            self.calls.append(method)

        if method == "test_error":
            return {
                "jsonrpc": "2.0",
                "id": request["id"],
                "error": {"code": -32000, "message": "synthetic error"},
            }
        if method == "eth_sendRawTransaction":
            transaction_hash = (
                "0x" + hashlib.sha256(request["params"][0].encode()).hexdigest()
            )
            return {"jsonrpc": "2.0", "id": request["id"], "result": transaction_hash}
        if method == "eth_getTransactionReceipt":
            transaction_hash = request["params"][0]
            with self.lock:
                polls = self.receipt_polls.get(transaction_hash, 0) + 1
                self.receipt_polls[transaction_hash] = polls
            result = (
                None
                if polls == 1
                else {
                    "transactionHash": transaction_hash,
                    "blockNumber": "0x10",
                    "status": "0x1",
                }
            )
            return {"jsonrpc": "2.0", "id": request["id"], "result": result}
        return {"jsonrpc": "2.0", "id": request["id"], "result": "0x0"}


class ReplayClient:
    def __init__(
        self,
        *,
        confirmed_transactions: set[str],
        receipt_delay_seconds: float = 0.0,
    ) -> None:
        self.confirmed_hashes = {
            self.transaction_hash(raw_transaction)
            for raw_transaction in confirmed_transactions
        }
        self.receipt_delay_seconds = receipt_delay_seconds
        self.lock = threading.Lock()
        self.events: list[tuple[str, str, float]] = []

    @staticmethod
    def transaction_hash(raw_transaction: str) -> str:
        return "0x" + hashlib.sha256(raw_transaction.encode()).hexdigest()

    def call(self, method: str, params: list[Any]) -> RpcCallResult:
        value = params[0]
        with self.lock:
            self.events.append((method, value, time.perf_counter()))

        if method == "eth_sendRawTransaction":
            result: Any = self.transaction_hash(value)
        elif method == "eth_getTransactionReceipt":
            if self.receipt_delay_seconds > 0:
                time.sleep(self.receipt_delay_seconds)
            result = (
                {
                    "transactionHash": value,
                    "blockNumber": "0x10",
                    "status": "0x1",
                }
                if value in self.confirmed_hashes
                else None
            )
        else:
            raise AssertionError(f"unexpected method: {method}")

        return RpcCallResult(method=method, latency_ms=0.0, ok=True, result=result)


def make_handler(state: MockRpcState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers["Content-Length"])
            request = json.loads(self.rfile.read(length))
            body = json.dumps(state.response(request)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: Any) -> None:
            return

    return Handler


class AgentRpcLoadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.state = MockRpcState()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.state))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.server.server_address
        cls.client = JsonRpcClient(f"http://{host}:{port}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_synthetic_addresses_are_stable_and_distinct(self) -> None:
        self.assertEqual(synthetic_address("seed", 1), synthetic_address("seed", 1))
        self.assertNotEqual(synthetic_address("seed", 1), synthetic_address("seed", 2))
        self.assertRegex(synthetic_address("seed", 1), r"^0x[0-9a-f]{40}$")

    def test_rpc_errors_are_reported_separately(self) -> None:
        result = self.client.call("test_error", [])
        metrics = Metrics()
        metrics.record(result)
        summary = metrics.summary(1.0)["overall"]

        self.assertFalse(result.ok)
        self.assertEqual(summary["rpc_errors"], 1)
        self.assertEqual(summary["rpc_error_codes"], {"-32000": 1})
        self.assertEqual(summary["transport_errors"], 0)

    def test_read_workload_uses_every_agent_and_method(self) -> None:
        summary = run_read_workload(
            self.client,
            agents=3,
            requests_per_agent=2,
            concurrency=4,
            methods=("nonce", "balance"),
            seed="test",
        )

        self.assertEqual(summary["rpc"]["overall"]["attempts"], 12)
        self.assertEqual(summary["rpc"]["overall"]["succeeded"], 12)
        self.assertEqual(
            set(summary["rpc"]["by_method"]),
            {"eth_getBalance", "eth_getTransactionCount"},
        )

    def test_replay_waits_for_receipts(self) -> None:
        records = [
            ReplayRecord(agent_id="agent-1", raw_transaction="0x01", label="initial"),
            ReplayRecord(agent_id="agent-2", raw_transaction="0x02", label="initial"),
        ]
        summary = run_replay_workload(
            self.client,
            records=records,
            concurrency=2,
            receipt_timeout_seconds=1.0,
            receipt_poll_interval_seconds=0.001,
        )

        self.assertEqual(summary["workload"]["accepted_transactions"], 2)
        self.assertEqual(summary["workload"]["unique_accepted_hashes"], 2)
        self.assertEqual(summary["receipts"]["confirmed"], 2)
        self.assertEqual(summary["receipts"]["timed_out"], 0)
        self.assertTrue(
            all(
                item["state"] == "confirmed" for item in summary["receipts"]["outcomes"]
            )
        )

    def test_replay_polls_all_hashes_without_worker_starvation(self) -> None:
        client = ReplayClient(confirmed_transactions={"0x03", "0x04"})
        records = [
            ReplayRecord("agent-1", "0x01", "unmined-1"),
            ReplayRecord("agent-2", "0x02", "unmined-2"),
            ReplayRecord("agent-1", "0x03", "replacement-1"),
            ReplayRecord("agent-2", "0x04", "replacement-2"),
        ]
        receipt_timeout_seconds = 0.05

        summary = run_replay_workload(
            client,
            records=records,
            concurrency=2,
            receipt_timeout_seconds=receipt_timeout_seconds,
            receipt_poll_interval_seconds=0.001,
        )

        outcomes = {
            outcome["label"]: outcome for outcome in summary["receipts"]["outcomes"]
        }
        self.assertEqual(summary["receipts"]["confirmed"], 2)
        self.assertEqual(summary["receipts"]["timed_out"], 2)
        self.assertEqual(outcomes["unmined-1"]["state"], "timed_out")
        self.assertEqual(outcomes["unmined-2"]["state"], "timed_out")
        for label in ("replacement-1", "replacement-2"):
            self.assertEqual(outcomes[label]["state"], "confirmed")
            self.assertLess(
                outcomes[label]["receipt_latency_ms"],
                receipt_timeout_seconds * 1000,
            )

    def test_replay_polls_before_later_scheduled_submissions(self) -> None:
        client = ReplayClient(confirmed_transactions={"0x01", "0x02"})
        records = [
            ReplayRecord("agent-1", "0x01", "early"),
            ReplayRecord("agent-2", "0x02", "delayed", send_after_ms=100),
        ]

        summary = run_replay_workload(
            client,
            records=records,
            concurrency=1,
            receipt_timeout_seconds=0.04,
            receipt_poll_interval_seconds=0.001,
        )

        early_hash = client.transaction_hash("0x01")
        early_receipt_at = next(
            called_at
            for method, value, called_at in client.events
            if method == "eth_getTransactionReceipt" and value == early_hash
        )
        delayed_submission_at = next(
            called_at
            for method, value, called_at in client.events
            if method == "eth_sendRawTransaction" and value == "0x02"
        )
        outcomes = {
            outcome["label"]: outcome for outcome in summary["receipts"]["outcomes"]
        }

        self.assertLess(early_receipt_at, delayed_submission_at)
        self.assertEqual(outcomes["early"]["state"], "confirmed")
        self.assertLess(outcomes["early"]["receipt_latency_ms"], 40)

    def test_replay_does_not_confirm_receipts_observed_after_deadline(self) -> None:
        client = ReplayClient(
            confirmed_transactions={"0x01"},
            receipt_delay_seconds=0.03,
        )

        summary = run_replay_workload(
            client,
            records=[ReplayRecord("agent-1", "0x01", "slow-receipt")],
            concurrency=1,
            receipt_timeout_seconds=0.01,
            receipt_poll_interval_seconds=0.001,
        )

        self.assertEqual(summary["receipts"]["confirmed"], 0)
        self.assertEqual(summary["receipts"]["timed_out"], 1)
        self.assertEqual(
            summary["receipts"]["outcomes"][0]["state"],
            "timed_out",
        )

    def test_replay_corpus_validation(self) -> None:
        with TemporaryDirectory() as directory:
            corpus = Path(directory) / "corpus.jsonl"
            corpus.write_text(
                "# comments are allowed\n"
                + json.dumps(
                    {
                        "agent_id": "one",
                        "raw_transaction": "0x0102",
                        "send_after_ms": 25,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            records = load_replay_records(corpus)

        self.assertEqual(records, [ReplayRecord("one", "0x0102", "transaction", 25)])


if __name__ == "__main__":
    unittest.main()
