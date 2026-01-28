#!/bin/bash

# Return 0 if clang-format exists and its version is exactly 18.1.8
clang_format_is_18_1_8() {
  command -v clang-format-18.1.8 >/dev/null 2>&1 || return 1
  clang-format-18.1.8 --version 2>/dev/null | grep -Eq 'clang-format version[[:space:]]+18\.1\.8\b'
}

install_clang_format_18_1_8() {
  cd $HOME
  local SUDO=""
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    SUDO="sudo"
  fi

  local ARCH DPKG TINFO_PKG_URL LLVM_PKG_URL
  ARCH="$(uname -m)"
  DPKG="$(dpkg --print-architecture)"

  # 1) Install libtinfo5 via the exact commands you requested (amd64 deb)
  echo "[INFO] Installing libtinfo5"
  case "$ARCH" in
    x86_64)
      TINFO_PKG_URL="http://security.ubuntu.com/ubuntu/pool/universe/n/ncurses"
      ;;
    aarch64|arm64)
      TINFO_PKG_URL="https://cn.ports.ubuntu.com/pool/universe/n/ncurses"
      ;;
    *)
      echo "[ERROR] Unsupported arch: $ARCH"
      return 1
      ;;
  esac

  $SUDO apt update
  local TINFO_DEB="libtinfo5_6.3-2ubuntu0.1_${DPKG}.deb"
  local TINFO_URL="${TINFO_PKG_URL}/${TINFO_DEB}"

  echo "[INFO] Downloading ${TINFO_DEB} from ${TINFO_URL}"
  
  wget -q -O "./${TINFO_DEB}" "$TINFO_URL"
  $SUDO apt install -y "./${TINFO_DEB}"

  # 2) Download LLVM clang-format 18.1.8
  echo "[INFO] Downloading LLVM clang-format 18.1.8"
  case "$ARCH" in
    x86_64)
      PKG_URL="https://github.com/llvm/llvm-project/releases/download/llvmorg-18.1.8/clang+llvm-18.1.8-x86_64-linux-gnu-ubuntu-18.04.tar.xz"
      ;;
    aarch64|arm64)
      PKG_URL="https://github.com/llvm/llvm-project/releases/download/llvmorg-18.1.8/clang+llvm-18.1.8-aarch64-linux-gnu.tar.xz"
      ;;
    *)
      echo "Unsupported arch: $ARCH" >&2
      return 1
      ;;
  esac

  mkdir -p ./llvm-18.1.8

  echo "[INFO] Downloading LLVM clang-format 18.1.8 from ${PKG_URL}"

  curl -L "$PKG_URL" -o /tmp/llvm-18.1.8.tar.xz
  tar -xf /tmp/llvm-18.1.8.tar.xz -C ./llvm-18.1.8 --strip-components=1
  rm -f /tmp/llvm-18.1.8.tar.xz

  # 3) Link as versioned binary
  $SUDO ln -sf "$(pwd)/llvm-18.1.8/bin/clang-format" /usr/local/bin/clang-format-18.1.8

  cd -
  echo "[INFO] Verifying clang-format-18.1.8"
  
  # 4) Verify
  if /usr/local/bin/clang-format-18.1.8 --version 2>/dev/null | grep -Eq 'clang-format version[[:space:]]+18\.1\.8\b'; then
    echo "[OK] Installed: $(/usr/local/bin/clang-format-18.1.8 --version)"
  else
    echo "[ERR] clang-format-18.1.8 install/link failed." >&2
    /usr/local/bin/clang-format-18.1.8 --version >&2 || true
    return 1
  fi
}

# One-shot entry: if check fails, install
ensure_clang_format_18_1_8() {
  if clang_format_is_18_1_8; then
    echo "[OK] clang-format already 18.1.8: $(clang-format-18.1.8 --version)"
    return 0
  fi
  echo "[ERR] clang-format not found, installing 18.1.8"
  install_clang_format_18_1_8
}

# Format all files in the project
ensure_clang_format_18_1_8

git ls-files -z '*.c' '*.cc' '*.cpp' '*.h' '*.hpp' \
| awk -v RS='\0' -v ORS='\0' '!/^(third_party|vendor|build|external)\//' \
| xargs -0 -r clang-format-18.1.8 -i --style="file"

echo "[OK] Format completed"