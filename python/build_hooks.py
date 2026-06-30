from __future__ import annotations

from pathlib import Path
import os
import shutil
import subprocess
import sysconfig

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        if self.target_name not in {"wheel", "editable"}:
            return

        root = Path(self.root)
        repo_root = root.parent
        native_dir = root / "build" / "native"
        native_libs_dir = native_dir / ".libs"
        cmake_dir = root / "build" / "cmake-wheel"
        package_libs_dir = root / "src" / "eloqstore" / ".libs"
        native_dir.mkdir(parents=True, exist_ok=True)
        native_libs_dir.mkdir(parents=True, exist_ok=True)
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
        bundled_lib = native_libs_dir / "libeloqstore_capi.so"
        shutil.copy2(built_lib, bundled_lib)

        self._copy_private_shared_libs(built_lib, native_libs_dir)

        if package_libs_dir.exists():
            shutil.rmtree(package_libs_dir)
        shutil.copytree(native_libs_dir, package_libs_dir)

        build_data["pure_python"] = False
        if self.target_name == "wheel":
            platform_tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
            build_data["tag"] = f"py3-none-{platform_tag}"

    def _copy_private_shared_libs(
        self,
        built_lib: Path,
        output_dir: Path,
    ) -> None:
        ldd = subprocess.run(
            ["ldd", str(built_lib)],
            check=True,
            capture_output=True,
            text=True,
        )
        for line in ldd.stdout.splitlines():
            if "=>" not in line:
                continue
            _, resolved = line.split("=>", 1)
            resolved = resolved.strip().split(" ", 1)[0]
            if not resolved or resolved == "not":
                continue
            resolved_path = Path(resolved).resolve()
            resolved_str = str(resolved_path)
            # Treat distro-managed library directories as system dependencies.
            # Bundling /usr/local copies such as glog can cause duplicate loads
            # when transitive dependencies also resolve the same SONAME via the
            # dynamic linker.
            if (
                resolved_str.startswith("/lib")
                or resolved_str.startswith("/usr/lib")
                or resolved_str.startswith("/usr/local/lib")
            ):
                continue
            copied_path = output_dir / resolved_path.name
            shutil.copy2(resolved_path, copied_path)

            soname = self._read_soname(resolved_path)
            if soname and soname != copied_path.name:
                soname_path = output_dir / soname
                if soname_path.exists() or soname_path.is_symlink():
                    soname_path.unlink()
                soname_path.symlink_to(copied_path.name)

    def _read_soname(self, lib_path: Path) -> str | None:
        readelf = subprocess.run(
            ["readelf", "-d", str(lib_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        for line in readelf.stdout.splitlines():
            if "(SONAME)" not in line:
                continue
            start = line.find("[")
            end = line.find("]", start + 1)
            if start == -1 or end == -1:
                continue
            return line[start + 1 : end]
        return None
