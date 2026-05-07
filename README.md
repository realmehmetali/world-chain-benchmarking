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
just up                 # Provision the instance
just snapshot <url>     # Download a chain snapshot (s3:// or https://) to /data/snapshot
just ssh                # SSH into the instance
just status             # Show instance state and cloud-init status
just down               # Destroy the instance
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
