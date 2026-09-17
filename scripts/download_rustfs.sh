#!/usr/bin/env bash
set -euo pipefail

install_dir=${1:?usage: $0 install_directory}
# Apache-2.0 release: https://github.com/rustfs/rustfs/releases/tag/1.0.0
version=1.0.0
case "$(uname -m)" in
  x86_64)
    arch=x86_64
    checksum=2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42
    ;;
  aarch64|arm64)
    arch=aarch64
    checksum=780e832d68e0148dc042f05647796056fe014e7cf1a8f195e3e83b22a3bb988f
    ;;
  *) echo "Unsupported RustFS architecture: $(uname -m)" >&2; exit 1 ;;
esac

download_dir=$(mktemp -d)
trap 'rm -rf "$download_dir"' EXIT
archive="rustfs-linux-${arch}-gnu-v${version}.zip"
curl --fail --show-error --location --retry 3 --connect-timeout 15 --max-time 300 \
  "https://github.com/rustfs/rustfs/releases/download/${version}/${archive}" \
  --output "$download_dir/$archive"
echo "$checksum  $download_dir/$archive" | sha256sum --check --strict
unzip -q "$download_dir/$archive" rustfs -d "$download_dir"
install -d -- "$install_dir"
install -m 0755 -- "$download_dir/rustfs" "$install_dir/rustfs"
