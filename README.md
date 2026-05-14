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

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
