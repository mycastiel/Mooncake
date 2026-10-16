#!/bin/bash
# Two-pass NDS check on NPU 0-4.
# Pass 1 writes a fresh image so the disk has extent metadata.
# Pass 2 is a new process: NDS scans that metadata and reads it back.
set -euo pipefail

IMG="${IMG:-/home/qyf/nds_xds_bench.img}"
IMG_SIZE="${IMG_SIZE:-8G}"
BYTES="${BYTES:-1G}"
IO="${IO:-1M}"
DEPTH="${DEPTH:-8}"
CARDS="${CARDS:-0,1,2,3,4}"
BENCH="${BENCH:-/home/qyf/Mooncake/install-xds/bin/xds_nds_bench}"
if [[ -f /usr/local/Ascend/ascend-toolkit/set_env.sh ]]; then
    # shellcheck disable=SC1091
    source /usr/local/Ascend/ascend-toolkit/set_env.sh
fi
export LD_LIBRARY_PATH="/home/qyf/Mooncake/install-xds/lib:/home/qyf/XDS/file_p2p:/usr/local/Ascend/cann-9.1.0/aarch64-linux/lib64:/usr/local/Ascend/driver/lib64:${LD_LIBRARY_PATH:-}"

if [[ ! -x "$BENCH" ]]; then
    echo "missing $BENCH" >&2
    exit 1
fi
if [[ ! -e /dev/p2p_device ]]; then
    echo "NDS device /dev/p2p_device is not present" >&2
    exit 1
fi

if [[ ! -f "$IMG" ]]; then
    echo "creating $IMG ($IMG_SIZE)"
    fallocate -l "$IMG_SIZE" "$IMG"
fi

DEV="$(losetup -j "$IMG" | awk -F: 'NR==1 {print $1}')"
if [[ -z "$DEV" ]]; then
    if losetup --help 2>&1 | grep -q direct-io; then
        DEV="$(losetup -f --show --direct-io=on "$IMG")"
    else
        DEV="$(losetup -f --show "$IMG")"
    fi
    echo "attached $IMG at $DEV"
else
    echo "reusing $DEV for $IMG"
fi

echo "===== pass 1: generate, scan disk, write metadata ====="
"$BENCH" --mode generate --device "$DEV" --cards "$CARDS" --bytes "$BYTES" \
    --io "$IO" --depth "$DEPTH"

echo "===== pass 2: restart process, scan existing metadata, read ====="
"$BENCH" --mode load --device "$DEV" --cards "$CARDS" --bytes "$BYTES" \
    --io "$IO" --depth "$DEPTH"
