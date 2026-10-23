#!/usr/bin/env bash
# Privileged Linux acceptance test, isolated from the normal Rust job.
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
test_root="$(mktemp -d -t braid-nfs.XXXXXXXX)"
export BRAID_ROOT="$test_root/data"
mount_root="$test_root/mount"
mkdir -p "$mount_root"
server_pid=""
cleanup() {
  sudo umount "$mount_root" 2>/dev/null || true
  if [[ -n "$server_pid" ]]; then kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; fi
  # Retain the log in the CI artifact directory before deleting this private temp root.
  mkdir -p "$repo_root/target/engineering/nfs"
  cp "$test_root/server.log" "$repo_root/target/engineering/nfs/server.log" 2>/dev/null || true
  rm -rf -- "$test_root"
}
trap cleanup EXIT
RUST_LOG=braidfs_nfs=info,nfsserve=debug "$repo_root/target/debug/braidfs-nfs" --nfs-port 20491 >"$test_root/server.log" 2>&1 &
server_pid=$!
for attempt in $(seq 1 100); do
  if ! kill -0 "$server_pid" 2>/dev/null; then cat "$test_root/server.log"; exit 1; fi
  if (echo >/dev/tcp/127.0.0.1/20491) 2>/dev/null; then break; fi
  sleep 0.1
done
sudo mount -t nfs -o vers=3,proto=tcp,port=20491,mountport=20491,nolock,soft,timeo=10,retrans=2 127.0.0.1:/ "$mount_root"
python3 - "$mount_root" <<'PY'
import os, pathlib, sys
root = pathlib.Path(sys.argv[1])
assert '.braidfs' not in os.listdir(root)
directory = root / 'operations'
directory.mkdir()
source = directory / 'source.bin'
with source.open('w+b', buffering=0) as file:
    file.write(b'\xff\x00abcdef')
    file.seek(4); file.write(b'XY')
    file.truncate(6); os.fsync(file.fileno())
    file.seek(0); assert file.read() == b'\xff\x00abXY'
    destination = directory / 'renamed.bin'
    source.rename(destination)
    # An already open handle must follow the renamed inode.
    file.seek(0); assert file.read() == b'\xff\x00abXY'
with destination.open('r+b', buffering=0) as file:
    file.seek(5 * 1024 * 1024); file.write(b'end'); os.fsync(file.fileno())
assert destination.stat().st_size == 5 * 1024 * 1024 + 3
for i in range(180):
    (directory / f'entry-{i:03}').touch()
names = {entry.name for entry in directory.iterdir()}
expected = {'renamed.bin'} | {f'entry-{i:03}' for i in range(180)}
assert names == expected, f'READDIR lost entries: missing={sorted(expected-names)}, unexpected={sorted(names-expected)}'
destination.unlink()
for entry in directory.iterdir(): entry.unlink()
directory.rmdir()
print('Mounted NFS create/read/write/truncate/rename/open-handle/readdir/remove checks passed')
PY
