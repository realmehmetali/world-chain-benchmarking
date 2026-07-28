#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Generate synthetic, agent-shaped JSON-RPC load for an EVM node."""

from __future__ import annotations

import argparse
import hashlib
import heapq
import http.client
import json
import math
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

USER_AGENT = "world-chain-agent-load/1.0"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
READ_METHODS = ("nonce", "balance", "call")


@dataclass(frozen=True)
class RpcCallResult:
    """Result and timing information for one JSON-RPC call."""

    method: str
    latency_ms: float
    ok: bool
    result: Any = None
    rpc_error_code: int | str | None = None
    transport_error: str | None = None


@dataclass(frozen=True)
class ReplayRecord:
    """One raw transaction scheduled by the replay workload."""

    agent_id: str
    raw_transaction: str
    label: str
    send_after_ms: int = 0


@dataclass
class _ReceiptTracker:
    """Receipt state for one unique accepted transaction hash."""

    record: ReplayRecord
    submitted_at: float
    deadline: float
    order: int
    outcome: dict[str, Any] | None = None


class JsonRpcClient:
    """Small thread-safe JSON-RPC client using only the Python standard library."""

    def __init__(self, rpc_url: str, timeout_seconds: float = 10.0) -> None:
        if not rpc_url.startswith(("http://", "https://")):
            raise ValueError("RPC URL must use http:// or https://")
        self.rpc_url = rpc_url
        self.timeout_seconds = timeout_seconds
        self._next_id = 0
        self._id_lock = threading.Lock()

    def _request_id(self) -> int:
        with self._id_lock:
            self._next_id += 1
            return self._next_id

    def call(self, method: str, params: list[Any]) -> RpcCallResult:
        """Execute a JSON-RPC call without retrying or hiding an error."""
        started = time.perf_counter()
        request_body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": self._request_id(),
                "method": method,
                "params": params,
            }
        ).encode()
        request = urllib.request.Request(
            self.rpc_url,
            data=request_body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout_seconds
            ) as response:
                response_body = response.read()
        except urllib.error.HTTPError as error:
            try:
                response_body = error.read()
            except (
                http.client.HTTPException,
                OSError,
                TimeoutError,
                urllib.error.URLError,
            ) as read_error:
                return RpcCallResult(
                    method=method,
                    latency_ms=_elapsed_ms(started),
                    ok=False,
                    transport_error=type(read_error).__name__,
                )
            parsed = _parse_json(response_body)
            latency_ms = _elapsed_ms(started)
            if isinstance(parsed, dict) and isinstance(parsed.get("error"), dict):
                return RpcCallResult(
                    method=method,
                    latency_ms=latency_ms,
                    ok=False,
                    rpc_error_code=parsed["error"].get("code", error.code),
                )
            return RpcCallResult(
                method=method,
                latency_ms=latency_ms,
                ok=False,
                transport_error=f"HTTP_{error.code}",
            )
        except (
            http.client.HTTPException,
            OSError,
            TimeoutError,
            urllib.error.URLError,
        ) as error:
            return RpcCallResult(
                method=method,
                latency_ms=_elapsed_ms(started),
                ok=False,
                transport_error=type(error).__name__,
            )

        parsed = _parse_json(response_body)
        latency_ms = _elapsed_ms(started)
        if not isinstance(parsed, dict):
            return RpcCallResult(
                method=method,
                latency_ms=latency_ms,
                ok=False,
                transport_error="INVALID_JSON_RPC_RESPONSE",
            )
        if isinstance(parsed.get("error"), dict):
            return RpcCallResult(
                method=method,
                latency_ms=latency_ms,
                ok=False,
                rpc_error_code=parsed["error"].get("code", "unknown"),
            )
        if "result" not in parsed:
            return RpcCallResult(
                method=method,
                latency_ms=latency_ms,
                ok=False,
                transport_error="MISSING_JSON_RPC_RESULT",
            )
        return RpcCallResult(
            method=method,
            latency_ms=latency_ms,
            ok=True,
            result=parsed["result"],
        )


class Metrics:
    """Thread-safe collection of JSON-RPC results."""

    def __init__(self) -> None:
        self._results: list[RpcCallResult] = []
        self._lock = threading.Lock()

    def record(self, result: RpcCallResult) -> None:
        with self._lock:
            self._results.append(result)

    def summary(self, duration_seconds: float) -> dict[str, Any]:
        with self._lock:
            results = list(self._results)

        methods = sorted({result.method for result in results})
        return {
            "overall": _summarize_results(results, duration_seconds),
            "by_method": {
                method: _summarize_results(
                    [result for result in results if result.method == method],
                    duration_seconds,
                )
                for method in methods
            },
        }


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000


def _parse_json(value: bytes) -> Any:
    try:
        return json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return round(ordered[index], 3)


def _summarize_results(
    results: list[RpcCallResult], duration_seconds: float
) -> dict[str, Any]:
    latencies = [result.latency_ms for result in results]
    rpc_error_codes: dict[str, int] = {}
    transport_error_types: dict[str, int] = {}

    for result in results:
        if result.rpc_error_code is not None:
            code = str(result.rpc_error_code)
            rpc_error_codes[code] = rpc_error_codes.get(code, 0) + 1
        if result.transport_error is not None:
            transport_error_types[result.transport_error] = (
                transport_error_types.get(result.transport_error, 0) + 1
            )

    attempts = len(results)
    return {
        "attempts": attempts,
        "succeeded": sum(result.ok for result in results),
        "rpc_errors": sum(result.rpc_error_code is not None for result in results),
        "transport_errors": sum(
            result.transport_error is not None for result in results
        ),
        "requests_per_second": round(attempts / duration_seconds, 3)
        if duration_seconds > 0
        else None,
        "latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "p99": _percentile(latencies, 0.99),
            "max": round(max(latencies), 3) if latencies else None,
        },
        "rpc_error_codes": dict(sorted(rpc_error_codes.items())),
        "transport_error_types": dict(sorted(transport_error_types.items())),
    }


def synthetic_address(seed: str, agent_number: int) -> str:
    """Derive a stable address-shaped value without creating or storing a key."""
    digest = hashlib.sha256(f"{seed}:{agent_number}".encode()).hexdigest()
    return f"0x{digest[-40:]}"


def _read_params(method: str, address: str) -> tuple[str, list[Any]]:
    if method == "nonce":
        return "eth_getTransactionCount", [address, "pending"]
    if method == "balance":
        return "eth_getBalance", [address, "latest"]
    if method == "call":
        return "eth_call", [
            {"from": address, "to": ZERO_ADDRESS, "data": "0x"},
            "latest",
        ]
    raise ValueError(f"unknown read method: {method}")


def run_read_workload(
    client: JsonRpcClient,
    *,
    agents: int,
    requests_per_agent: int,
    concurrency: int,
    methods: Iterable[str],
    seed: str,
) -> dict[str, Any]:
    """Run concurrent reads using deterministic synthetic agent addresses."""
    selected_methods = tuple(methods)
    if not selected_methods or any(
        method not in READ_METHODS for method in selected_methods
    ):
        raise ValueError(f"methods must be selected from: {', '.join(READ_METHODS)}")

    metrics = Metrics()
    started = time.perf_counter()

    def calls() -> Iterable[tuple[str, list[Any]]]:
        addresses = [
            synthetic_address(seed, agent_number) for agent_number in range(agents)
        ]
        for _ in range(requests_per_agent):
            for address in addresses:
                for method in selected_methods:
                    yield _read_params(method, address)

    def execute(call: tuple[str, list[Any]]) -> None:
        method, params = call
        metrics.record(client.call(method, params))

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        list(executor.map(execute, calls()))

    duration_seconds = time.perf_counter() - started
    return {
        "profile": "read",
        "duration_ms": round(duration_seconds * 1000, 3),
        "workload": {
            "agents": agents,
            "requests_per_agent": requests_per_agent,
            "concurrency": concurrency,
            "methods": list(selected_methods),
            "synthetic_addresses": True,
        },
        "rpc": metrics.summary(duration_seconds),
    }


def load_replay_records(path: Path) -> list[ReplayRecord]:
    """Load and validate an NDJSON raw-transaction replay corpus."""
    records: list[ReplayRecord] = []
    with path.open(encoding="utf-8") as corpus:
        for line_number, line in enumerate(corpus, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid JSON on line {line_number}: {error.msg}"
                ) from error
            if not isinstance(item, dict):
                raise ValueError(f"line {line_number} must contain a JSON object")

            raw_transaction = item.get("raw_transaction")
            if not isinstance(raw_transaction, str) or not raw_transaction.startswith(
                "0x"
            ):
                raise ValueError(
                    f"line {line_number} raw_transaction must be a 0x-prefixed string"
                )
            try:
                bytes.fromhex(raw_transaction[2:])
            except ValueError as error:
                raise ValueError(
                    f"line {line_number} raw_transaction must contain valid hex"
                ) from error
            if len(raw_transaction) <= 2 or len(raw_transaction[2:]) % 2 != 0:
                raise ValueError(
                    f"line {line_number} raw_transaction must contain whole bytes"
                )

            send_after_ms = item.get("send_after_ms", 0)
            if not isinstance(send_after_ms, int) or send_after_ms < 0:
                raise ValueError(
                    f"line {line_number} send_after_ms must be a non-negative integer"
                )

            records.append(
                ReplayRecord(
                    agent_id=str(item.get("agent_id", f"line-{line_number}")),
                    raw_transaction=raw_transaction,
                    label=str(item.get("label", "transaction")),
                    send_after_ms=send_after_ms,
                )
            )

    if not records:
        raise ValueError("replay corpus does not contain any transactions")
    return records


def run_replay_workload(
    client: JsonRpcClient,
    *,
    records: list[ReplayRecord],
    concurrency: int,
    receipt_timeout_seconds: float,
    receipt_poll_interval_seconds: float,
) -> dict[str, Any]:
    """Submit signed synthetic transactions while measuring time to receipt."""
    metrics = Metrics()
    started = time.perf_counter()

    def execute_call(method: str, params: list[Any]) -> tuple[RpcCallResult, float]:
        result = client.call(method, params)
        return result, time.perf_counter()

    scheduled_records = sorted(
        enumerate(records), key=lambda item: (item[1].send_after_ms, item[0])
    )
    future_submission_queue = [
        (started + record.send_after_ms / 1000, order, record)
        for order, (_, record) in enumerate(scheduled_records)
    ]
    ready_submission_queue: list[tuple[float, int, ReplayRecord]] = []
    submission_burst_sizes: dict[float, int] = {}
    for scheduled_at, _, _ in future_submission_queue:
        submission_burst_sizes[scheduled_at] = (
            submission_burst_sizes.get(scheduled_at, 0) + 1
        )
    heapq.heapify(future_submission_queue)
    receipt_queue: list[tuple[float, int, str]] = []
    accepted: dict[str, _ReceiptTracker] = {}
    accepted_submissions = 0
    call_order = 0
    last_scheduled_kind: str | None = None
    priority_burst_at: float | None = None
    priority_burst_launches = 0
    rpc_timeout_seconds = getattr(client, "timeout_seconds", math.inf)
    in_flight: dict[
        Future[tuple[RpcCallResult, float]],
        tuple[int, str, tuple[int, ReplayRecord] | str],
    ] = {}

    def timed_out_outcome(
        transaction_hash: str, tracker: _ReceiptTracker
    ) -> dict[str, Any]:
        return {
            "agent_id": tracker.record.agent_id,
            "label": tracker.record.label,
            "transaction_hash": transaction_hash,
            "state": "timed_out",
            "receipt_latency_ms": None,
        }

    # Workers perform one RPC attempt at a time. The coordinator owns all
    # scheduling waits, preserving the global concurrency limit.
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        while (
            future_submission_queue
            or ready_submission_queue
            or receipt_queue
            or in_flight
        ):
            reserved_task_wake_at = math.inf

            while len(in_flight) < concurrency:
                now = time.perf_counter()
                # Under a strict global cap, overlapping bursts cannot all
                # retain their original timing once earlier calls run long.
                # Newer due bursts therefore take deterministic precedence
                # over older backlog; the backlog resumes after they launch.
                newest_due_at: float | None = None
                while (
                    future_submission_queue
                    and future_submission_queue[0][0] <= now
                ):
                    scheduled_at, submission_order, record = heapq.heappop(
                        future_submission_queue
                    )
                    heapq.heappush(
                        ready_submission_queue,
                        (-scheduled_at, submission_order, record),
                    )
                    if record.send_after_ms > 0 and (
                        newest_due_at is None or scheduled_at > newest_due_at
                    ):
                        newest_due_at = scheduled_at

                if newest_due_at is not None:
                    priority_burst_at = newest_due_at
                    priority_burst_launches = min(
                        submission_burst_sizes[newest_due_at],
                        concurrency - len(in_flight),
                    )

                submission_ready = bool(ready_submission_queue)
                receipt_ready = bool(receipt_queue) and receipt_queue[0][0] <= now
                if not submission_ready and not receipt_ready:
                    break

                if receipt_ready:
                    _, _, transaction_hash = receipt_queue[0]
                    tracker = accepted[transaction_hash]
                    if now >= tracker.deadline:
                        heapq.heappop(receipt_queue)
                        tracker.outcome = timed_out_outcome(transaction_hash, tracker)
                        continue

                # Give a newly due burst the capacity available at that
                # instant. After those launches, resume alternation so a
                # large delayed batch cannot starve receipt polling.
                priority_submission_ready = (
                    submission_ready
                    and priority_burst_launches > 0
                    and -ready_submission_queue[0][0] == priority_burst_at
                )
                if priority_submission_ready:
                    kind = "submission"
                elif submission_ready and receipt_ready:
                    kind = (
                        "receipt"
                        if last_scheduled_kind == "submission"
                        else "submission"
                    )
                elif submission_ready:
                    kind = "submission"
                else:
                    kind = "receipt"

                # Any RPC attempt can run until the RPC timeout. Preserve
                # capacity for the next scheduled burst when the attempt
                # could overlap its deadline. Receipt polls may yield the
                # whole pool, but let at least one earlier ready submission
                # proceed so a full-size future burst cannot pre-idle every
                # worker.
                if (
                    not priority_submission_ready
                    and future_submission_queue
                    and now + rpc_timeout_seconds
                    >= future_submission_queue[0][0]
                ):
                    next_submission_at = future_submission_queue[0][0]
                    reserved_submission_slots = min(
                        submission_burst_sizes[next_submission_at],
                        concurrency,
                    )
                    if (
                        kind == "receipt"
                        and accepted[receipt_queue[0][2]].deadline
                        <= next_submission_at
                    ):
                        # Do not reserve past a receipt's hard deadline.
                        reserved_submission_slots = 0
                    elif kind == "submission":
                        reserved_submission_slots = min(
                            reserved_submission_slots,
                            max(concurrency - 1, 0),
                        )

                    if (
                        kind == "receipt"
                        and submission_ready
                        and len(in_flight)
                        >= concurrency - reserved_submission_slots
                    ):
                        kind = "submission"
                        reserved_submission_slots = min(
                            reserved_submission_slots,
                            max(concurrency - 1, 0),
                        )

                    if len(in_flight) >= concurrency - reserved_submission_slots:
                        reserved_task_wake_at = next_submission_at
                        if receipt_ready:
                            transaction_hash = receipt_queue[0][2]
                            reserved_task_wake_at = min(
                                next_submission_at,
                                accepted[transaction_hash].deadline,
                            )
                        break

                if kind == "submission":
                    neg_scheduled_at, submission_order, record = heapq.heappop(
                        ready_submission_queue
                    )
                    scheduled_at = -neg_scheduled_at
                    submission_burst_sizes[scheduled_at] -= 1
                    if (
                        priority_burst_launches > 0
                        and scheduled_at == priority_burst_at
                    ):
                        priority_burst_launches -= 1
                    future = executor.submit(
                        execute_call,
                        "eth_sendRawTransaction",
                        [record.raw_transaction],
                    )
                    payload: tuple[int, ReplayRecord] | str = (
                        submission_order,
                        record,
                    )
                else:
                    _, _, transaction_hash = heapq.heappop(receipt_queue)
                    tracker = accepted[transaction_hash]
                    future = executor.submit(
                        execute_call,
                        "eth_getTransactionReceipt",
                        [transaction_hash],
                    )
                    payload = transaction_hash

                in_flight[future] = (call_order, kind, payload)
                call_order += 1
                last_scheduled_kind = kind

            next_receipt_due = receipt_queue[0][0] if receipt_queue else math.inf
            if reserved_task_wake_at < math.inf:
                next_receipt_due = reserved_task_wake_at
            next_due = min(
                (
                    future_submission_queue[0][0]
                    if future_submission_queue
                    else math.inf
                ),
                next_receipt_due,
            )

            if not in_flight:
                if next_due < math.inf:
                    time.sleep(max(0.0, next_due - time.perf_counter()))
                continue

            wait_timeout = (
                max(0.0, next_due - time.perf_counter())
                if len(in_flight) < concurrency and next_due < math.inf
                else None
            )
            completed, _ = wait(
                in_flight,
                timeout=wait_timeout,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                continue

            for future in sorted(completed, key=lambda item: in_flight[item][0]):
                _, kind, payload = in_flight.pop(future)
                result, completed_at = future.result()
                metrics.record(result)

                if kind == "submission":
                    assert isinstance(payload, tuple)
                    submission_order, record = payload
                    if result.ok and isinstance(result.result, str):
                        accepted_submissions += 1
                        if result.result not in accepted:
                            tracker = _ReceiptTracker(
                                record=record,
                                submitted_at=completed_at,
                                deadline=completed_at + receipt_timeout_seconds,
                                order=submission_order,
                            )
                            accepted[result.result] = tracker
                            heapq.heappush(
                                receipt_queue,
                                (completed_at, tracker.order, result.result),
                            )
                    continue

                transaction_hash = payload
                assert isinstance(transaction_hash, str)
                tracker = accepted[transaction_hash]
                if completed_at >= tracker.deadline:
                    tracker.outcome = timed_out_outcome(transaction_hash, tracker)
                elif result.ok and isinstance(result.result, dict):
                    tracker.outcome = {
                        "agent_id": tracker.record.agent_id,
                        "label": tracker.record.label,
                        "transaction_hash": transaction_hash,
                        "state": "confirmed",
                        "receipt_latency_ms": round(
                            (completed_at - tracker.submitted_at) * 1000, 3
                        ),
                        "block_number": result.result.get("blockNumber"),
                        "status": result.result.get("status"),
                    }
                else:
                    heapq.heappush(
                        receipt_queue,
                        (
                            min(
                                completed_at + receipt_poll_interval_seconds,
                                tracker.deadline,
                            ),
                            tracker.order,
                            transaction_hash,
                        ),
                    )

    receipt_outcomes = [
        outcome
        for tracker in sorted(accepted.values(), key=lambda tracker: tracker.order)
        if (outcome := tracker.outcome) is not None
    ]
    assert len(receipt_outcomes) == len(accepted)

    duration_seconds = time.perf_counter() - started
    confirmed_latencies = [
        outcome["receipt_latency_ms"]
        for outcome in receipt_outcomes
        if outcome["receipt_latency_ms"] is not None
    ]

    return {
        "profile": "replay",
        "duration_ms": round(duration_seconds * 1000, 3),
        "workload": {
            "scheduled_transactions": len(records),
            "accepted_transactions": accepted_submissions,
            "unique_accepted_hashes": len(accepted),
            "rejected_transactions": len(records) - accepted_submissions,
            "concurrency": concurrency,
        },
        "receipts": {
            "confirmed": sum(
                outcome["state"] == "confirmed" for outcome in receipt_outcomes
            ),
            "timed_out": sum(
                outcome["state"] == "timed_out" for outcome in receipt_outcomes
            ),
            "latency_ms": {
                "p50": _percentile(confirmed_latencies, 0.50),
                "p95": _percentile(confirmed_latencies, 0.95),
                "p99": _percentile(confirmed_latencies, 0.99),
                "max": round(max(confirmed_latencies), 3)
                if confirmed_latencies
                else None,
            },
            "outcomes": receipt_outcomes,
        },
        "rpc": metrics.summary(duration_seconds),
    }


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return parsed


def _add_rpc_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8545")
    parser.add_argument("--rpc-timeout-seconds", type=_positive_float, default=10.0)
    parser.add_argument("--concurrency", type=_positive_int, default=32)
    parser.add_argument(
        "--output", type=Path, help="write the JSON summary to this file"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="profile", required=True)

    read_parser = subparsers.add_parser("read", help="run safe read-only RPC load")
    _add_rpc_arguments(read_parser)
    read_parser.add_argument("--agents", type=_positive_int, default=50)
    read_parser.add_argument("--requests-per-agent", type=_positive_int, default=10)
    read_parser.add_argument(
        "--methods", nargs="+", choices=READ_METHODS, default=["nonce", "balance"]
    )
    read_parser.add_argument("--seed", default="world-chain-benchmark")

    replay_parser = subparsers.add_parser(
        "replay", help="submit a caller-supplied synthetic signed-transaction corpus"
    )
    _add_rpc_arguments(replay_parser)
    replay_parser.add_argument("--input", type=Path, required=True)
    replay_parser.add_argument(
        "--receipt-timeout-seconds", type=_positive_float, default=60.0
    )
    replay_parser.add_argument(
        "--receipt-poll-interval-seconds", type=_positive_float, default=0.25
    )
    replay_parser.add_argument(
        "--allow-writes",
        action="store_true",
        help="required acknowledgement that replay submits transactions",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    client = JsonRpcClient(args.rpc_url, args.rpc_timeout_seconds)

    if args.profile == "read":
        summary = run_read_workload(
            client,
            agents=args.agents,
            requests_per_agent=args.requests_per_agent,
            concurrency=args.concurrency,
            methods=args.methods,
            seed=args.seed,
        )
    else:
        if not args.allow_writes:
            parser.error("replay requires --allow-writes")
        try:
            records = load_replay_records(args.input)
        except (OSError, ValueError) as error:
            parser.error(str(error))
        summary = run_replay_workload(
            client,
            records=records,
            concurrency=args.concurrency,
            receipt_timeout_seconds=args.receipt_timeout_seconds,
            receipt_poll_interval_seconds=args.receipt_poll_interval_seconds,
        )

    rendered = json.dumps(summary, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
