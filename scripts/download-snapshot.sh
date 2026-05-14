#!/bin/bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <snapshot-url>" >&2
    exit 2
fi

URL="$1"
SNAPSHOT_DIR="/data/snapshot"
TMP_DIR="/data/tmp"

case "$URL" in
    *.tar.lz4|*.lz4) DECOMPRESS="lz4 -d" ;;
    *.tar.zst|*.tar.zstd|*.zst|*.zstd) DECOMPRESS="zstd -d" ;;
    *)
        echo "error: unsupported snapshot extension in $URL (expected .lz4 / .zst / .zstd)" >&2
        exit 1
        ;;
esac

sudo mkdir -p "$SNAPSHOT_DIR"
sudo chown "$(id -u):$(id -g)" "$SNAPSHOT_DIR"
rm -rf "${SNAPSHOT_DIR:?}/"*
sudo mkdir -p "$TMP_DIR"
sudo chown "$(id -u):$(id -g)" "$TMP_DIR"

echo "==> Disk free space:"
df -h "$SNAPSHOT_DIR" "$TMP_DIR" 2>/dev/null || true

case "$URL" in
    s3://*)
        timeout 7200 s5cmd --no-sign-request cat "$URL" | $DECOMPRESS | tar x -C "$SNAPSHOT_DIR"
        ;;
    https://*|http://*)
        TMPFILE="$TMP_DIR/snapshot-download.tmp"
        rm -f "$TMPFILE"
        aria2c -x 16 -s 16 --file-allocation=none -d "$TMP_DIR" -o snapshot-download.tmp "$URL"
        $DECOMPRESS "$TMPFILE" | tar x -C "$SNAPSHOT_DIR"
        rm -f "$TMPFILE"
        ;;
    *)
        echo "error: unsupported scheme in $URL (expected s3:// or http(s)://)" >&2
        exit 1
        ;;
esac

echo "snapshot extracted to $SNAPSHOT_DIR"
