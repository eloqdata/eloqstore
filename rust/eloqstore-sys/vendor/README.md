# Vendor 目录说明

本目录用于 Rust FFI 构建，通过**软链接**复用仓库根目录的源码，避免重复维护。

## 目录结构

```
vendor/
├── CMakeLists.txt          # Rust FFI 专用构建配置
├── external -> ../../../external  # 软链接：外部依赖（submodules + 工具文件）
├── src -> ../../../src            # 软链接：C++ 核心源码
├── include -> ../../../include    # 软链接：C++ 头文件
├── ffi/                           # FFI 专用文件（仅 2 个文件）
│   ├── src/
│   │   └── eloqstore_capi.cpp    # Rust FFI C API 实现
│   └── include/
│       └── eloqstore_capi.h      # Rust FFI C API 头文件
└── tools/                         # 工具（可选）
    └── page_checksum_tool.cpp
```

## 设计原则

1. **最小化重复**：`vendor/` 实际只存储 **4 个文件**（CMakeLists.txt + 2 个 FFI 文件 + 1 个工具）
2. **软链接复用**：`src/`、`include/`、`external/` 全部通过软链接指向仓库根目录
3. **FFI 隔离**：Rust 专用的 C API 文件（`eloqstore_capi.*`）独立存放在 `ffi/` 目录

## 维护指南

### 修改核心业务代码
直接在**仓库根目录**修改即可，无需同步到 vendor：
- 修改 `/src/*.cpp` → vendor 自动生效（软链接）
- 修改 `/include/*.h` → vendor 自动生效（软链接）
- 修改 `/external/*` → vendor 自动生效（软链接）

### 修改 FFI 专用代码
在 `vendor/ffi/` 目录下修改：
- `vendor/ffi/src/eloqstore_capi.cpp`
- `vendor/ffi/include/eloqstore_capi.h`

### 更新 submodule
在仓库根目录执行：
```bash
git submodule update --init --recursive
```
或直接运行 `cargo build`（build.rs 会自动执行）

## 构建说明

`build.rs` 会：
1. 自动执行 `git submodule update --init --recursive`
2. 使用 CMake 构建 `vendor/` 目录
3. 通过软链接自动使用仓库根目录的最新源码

无需手动同步文件！
