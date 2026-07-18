# SPDX-License-Identifier: MIT OR Apache-2.0

from __future__ import annotations

import hashlib
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from agent_rpc_load import (
    JsonRpcClient,
    Metrics,
    ReplayRecord,
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
