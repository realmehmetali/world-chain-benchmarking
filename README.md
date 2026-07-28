# World Chain Benchmarking

Provisions a single EC2 instance with Rust and Docker pre-installed for Ethereum client benchmarking work.

## Prerequisites

- [Terraform](https://terraform.io) >= 1.0
- [Just](https://github.com/casey/just)
- AWS credentials configured (`AWS_PROFILE`)

### Required AWS permissions

`just up` calls `ec2:RunInstances` from your local session, which `PowerUserAccess` denies — use `AdministratorAccess` (or an equivalent role) on the target account.

## Usage

```bash
just up                              # Provision the instance
just snapshot <url>                  # Download a chain snapshot (s3:// or https://) to /data/snapshot
just upload <remote-folder> [name]   # Compress + upload a remote folder to S3 as .tar.zst (detached)
just upload-status                   # Tail the in-flight upload log
just upload-cancel                   # Kill an in-flight upload
just agent-load-read                 # Run read-only synthetic-agent RPC load
just agent-load-replay <corpus>      # Replay synthetic signed transactions
just test-agent-load                 # Run benchmark regression tests locally
just ssh                             # SSH into the instance
just status                          # Show instance state and cloud-init status
just down                            # Destroy the instance
```

`just upload` streams `tar | zstd -T0 -3 | s5cmd pipe` on the instance using its
IAM role; nothing is staged on disk and no AWS credentials are forwarded. The
second argument is flexible:

- omitted → `s3://world-chain-benchmark-snapshots-<env>/uploads/<basename>-<UTC>.tar.zst`
- bare name (e.g. `block-203`) → `s3://world-chain-benchmark-snapshots-<env>/uploads/block-203.tar.zst`
- full `s3://bucket/key.tar.zst` → used as-is

The work runs detached under `nohup`, so dropping the local SSH session does
not abort the upload — re-attach with `just upload-status`.

## Synthetic agent RPC load

`benchmarks/agent_rpc_load/agent_rpc_load.py` generates agent-shaped JSON-RPC
traffic and reports request throughput, RPC and transport error counts, and
p50/p95/p99 latency. It uses only the Python standard library already present
on the Ubuntu benchmark instance.

Run the safe read-only profile against a node listening on the instance:

```bash
just agent-load-read http://127.0.0.1:8545 100 20 64
```

This profile deterministically derives address-shaped values from a public seed
and issues pending-nonce and balance reads. It does not create keys, require
funds, or submit transactions. Use the script directly to add `eth_call`:

```bash
python3 benchmarks/agent_rpc_load/agent_rpc_load.py read \
  --rpc-url http://127.0.0.1:8545 \
  --agents 100 --requests-per-agent 20 --concurrency 64 \
  --methods nonce balance call
```

The replay profile accepts newline-delimited JSON containing caller-generated,
signed **synthetic** transactions:

```json
{"agent_id":"agent-1","label":"initial","raw_transaction":"0x...","send_after_ms":0}
{"agent_id":"agent-1","label":"replacement","raw_transaction":"0x...","send_after_ms":250}
```

To measure replacement handling, sign the second transaction with the same
sender and nonce but a higher fee. `send_after_ms` controls its delay from the
start of the run. The report separates submission errors by JSON-RPC error code
and records confirmation or timeout plus receipt latency for every accepted
transaction.

Replay enforces one global concurrency limit across submissions and receipt
polls. If slow calls make scheduled bursts overlap, the newest due burst takes
priority over older unsent work so time-sensitive replacements can use the
capacity reserved for them; the older backlog resumes afterward.

```bash
just agent-load-replay ./synthetic-transactions.jsonl \
  http://127.0.0.1:8545 32 60
```

Replay requires the explicit `--allow-writes` acknowledgement when the script
is invoked directly. Only use funded synthetic accounts and an RPC endpoint you
are authorized to load test. Never put production keys in a corpus; raw signed
transactions are sufficient.

This workload measures ordinary EVM traffic from many automated wallets. It
does not synthesize Proof of Human (PBH) transactions because those require a
valid human proof.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
