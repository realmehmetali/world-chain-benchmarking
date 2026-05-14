#!/bin/bash
set -euo pipefail

apt-get update
apt-get install -y git curl build-essential zstd lz4 nvme-cli jq aria2 pv

# Mount instance store NVMe SSD at /data
NVME_DEVICE=$(nvme list -o json | jq -r '.Devices[] | select(.ModelNumber | contains("Instance Storage")) | .DevicePath' | head -1)
if [ -n "$NVME_DEVICE" ]; then
  mkfs.ext4 -F "$NVME_DEVICE"
  mkdir -p /data
  mount "$NVME_DEVICE" /data
  chown ubuntu:ubuntu /data
  echo "$NVME_DEVICE /data ext4 defaults,nofail 0 2" >>/etc/fstab
  echo "Mounted instance store at /data"
fi

# Install s5cmd (used by download-snapshot.sh for s3:// sources)
curl -sL https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-64bit.tar.gz | tar xz -C /tmp
mv /tmp/s5cmd /usr/local/bin/

# Install Rust toolchain (system-wide via rustup)
export RUSTUP_HOME=/usr/local/rustup
export CARGO_HOME=/usr/local/cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain stable --profile default \
        --component rustfmt --component clippy --no-modify-path
for bin in rustc cargo rustup rustfmt cargo-fmt cargo-clippy clippy-driver; do
    ln -sf "$CARGO_HOME/bin/$bin" "/usr/local/bin/$bin"
done
chmod -R a+rX "$RUSTUP_HOME" "$CARGO_HOME"

echo "Ready"
