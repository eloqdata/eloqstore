from __future__ import annotations

from pathlib import Path
import os
import shutil
import subprocess
import sysconfig

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        if self.target_name != "wheel":
            return

        root = Path(self.root)
        repo_root = root.parent
        native_dir = root / "build" / "native"
        cmake_dir = root / "build" / "cmake-wheel"
        native_dir.mkdir(parents=True, exist_ok=True)
        cmake_dir.mkdir(parents=True, exist_ok=True)

        cmake_args = [
            "cmake",
            "-S",
            str(repo_root),
            "-B",
            str(cmake_dir),
            "-DCMAKE_BUILD_TYPE=Release",
            "-DWITH_UNIT_TESTS=OFF",
            "-DWITH_EXAMPLE=OFF",
            "-DWITH_DB_STRESS=OFF",
            "-DWITH_BENCHMARK=OFF",
        ]
        if os.environ.get("CCACHE_DIR") and shutil.which("ccache"):
            cmake_args.extend(
                [
                    "-DCMAKE_C_COMPILER_LAUNCHER=ccache",
                    "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
                ]
            )
        subprocess.run(cmake_args, check=True)
        subprocess.run(
            [
                "cmake",
                "--build",
                str(cmake_dir),
                "--target",
                "eloqstore_capi",
                "-j4",
            ],
            check=True,
        )

        built_lib = cmake_dir / "libeloqstore_capi.so"
        if not built_lib.exists():
            raise RuntimeError(f"Expected built library at {built_lib}")

        output_lib = native_dir / "libeloqstore_capi.so"
        shutil.copy2(built_lib, output_lib)

        build_data["pure_python"] = False
        platform_tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
        build_data["tag"] = f"py3-none-{platform_tag}"
