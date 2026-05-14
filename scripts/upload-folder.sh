#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: $0 <src-folder> [name|s3://uri]" >&2
    exit 2
fi

SRC="${1%/}"
NAME_OR_URI="${2:-}"
DEFAULT_BUCKET="${DEFAULT_BUCKET:-world-chain-benchmark-snapshots-dev}"

if [[ -z "$SRC" || ! -d "$SRC" ]]; then
    echo "error: not a directory: $SRC" >&2
    exit 1
fi

PARENT=$(dirname "$SRC")
BASE=$(basename "$SRC")

# Resolve destination:
#   empty            -> s3://<bucket>/uploads/<basename>-<UTC>.tar.zst
#   s3://...         -> use as-is
#   anything else    -> treat as a custom basename under s3://<bucket>/uploads/
#                       (strip a trailing .tar.zst if the user added it)
if [[ -z "$NAME_OR_URI" ]]; then
    TS=$(date -u +%Y%m%dT%H%M%SZ)
    DEST="s3://${DEFAULT_BUCKET}/uploads/${BASE}-${TS}.tar.zst"
elif [[ "$NAME_OR_URI" == s3://* ]]; then
    DEST="$NAME_OR_URI"
else
    NAME="${NAME_OR_URI%.tar.zst}"
    if [[ "$NAME" == */* ]]; then
        echo "error: custom name '$NAME_OR_URI' must not contain '/'; pass a full s3://... uri instead" >&2
        exit 1
    fi
    DEST="s3://${DEFAULT_BUCKET}/uploads/${NAME}.tar.zst"
fi

if ! command -v pv >/dev/null 2>&1; then
    echo "==> Installing pv..."
    sudo apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq pv
fi

echo "==> Source: $SRC"
echo "==> Dest:   $DEST"
echo "==> Disk free space:"
df -h "$SRC" 2>/dev/null || true

echo "==> Measuring source size (du -sb)..."
TOTAL_BYTES=$(du -sb "$SRC" | awk '{print $1}')
HUMAN=$(numfmt --to=iec --suffix=B "$TOTAL_BYTES" 2>/dev/null || echo "$TOTAL_BYTES bytes")
echo "==> Source size: $HUMAN ($TOTAL_BYTES bytes)"

echo "==> Starting tar | pv | zstd -T0 -3 | s5cmd pipe ..."
# pv flags:
#   -f  force progress even when stderr is a file (we are detached, no tty).
#   -i 10  emit a progress update every 10s (instead of pv's default 1s).
#   -s  total uncompressed bytes for percentage + ETA.
# S3 caps multipart at 10000 parts/object. 256MB parts gives headroom past 2.5TB
# of compressed output. --concurrency 10 saturates the i4i.4xlarge NIC without
# needing meaningful RAM.
tar -C "$PARENT" -cf - "$BASE" \
    | pv -f -i 10 -s "$TOTAL_BYTES" \
    | zstd -T0 -3 \
    | s5cmd pipe --concurrency 10 --part-size 256 "$DEST"

echo "==> Verifying upload..."
s5cmd ls "$DEST"

echo "==> Done: $DEST"
