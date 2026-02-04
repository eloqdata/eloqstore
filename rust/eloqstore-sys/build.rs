use cmake::Config;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn add_usr_local_link_search_paths_if_needed() {
    // Some CI/container images install libraries (e.g. glog) under /usr/local/lib.
    // The linker does not always search /usr/local/lib by default, so we add it
    // when we detect commonly required libraries there.
    for dir in ["/usr/local/lib", "/usr/local/lib64"] {
        let d = PathBuf::from(dir);
        if !d.is_dir() {
            continue;
        }
        let has_glog = d.join("libglog.so").exists() || d.join("libglog.so.1").exists();
        let has_gflags =
            d.join("libgflags.so").exists() || d.join("libgflags.so.2").exists();
        if has_glog || has_gflags {
            println!("cargo:rustc-link-search=native={}", d.display());
        }
    }
}

fn main() {
    // Initialize and update git submodule before build
    // vendor/{src,include,external} are soft-linked to repository root
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Ok(repo_root) = manifest_dir.join("../..").canonicalize() {
        if repo_root.join(".git").exists() {
            let status = Command::new("git")
                .args(&["submodule", "update", "--init", "--recursive"])
                .current_dir(&repo_root)
                .status();
            if let Ok(s) = status {
                if !s.success() {
                    panic!("git submodule update --init --recursive failed, please manually execute in repository root");
                }
            }
        }
    }

    // Parallel CMake build: use NUM_JOBS (set by Cargo) or CPU count
    let num_jobs = std::env::var("NUM_JOBS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .or_else(|| std::thread::available_parallelism().ok().map(|p| p.get() as u32))
        .unwrap_or(1)
        .max(1);

    // Enable static linking of all dependencies
    let dst = Config::new("vendor")
        .define("CMAKE_CXX_STANDARD", "20")
        .define("BUILD_FOR_RUST", "ON")
        .define("STATIC_ALL_DEPS", "ON")
        .define("WITH_UNIT_TESTS", "OFF")
        .define("WITH_EXAMPLE", "OFF")
        .define("WITH_DB_STRESS", "OFF")
        .define("WITH_BENCHMARK", "OFF")
        .define("ELOQ_MODULE_ENABLED", "OFF")
        .build_arg(format!("-j{}", num_jobs))
        .build();

    let build_dir = dst.join("build");
    let lib_dir = dst.join("lib");
    if !lib_dir.exists() {
        fs::create_dir_all(&lib_dir).expect("Failed to create lib dir");
    }

    // Look for the combined shared library first
    let combined_lib = build_dir.join("libeloqstore_combine.so");
    let combined_lib_dest = lib_dir.join("libeloqstore_combine.so");

    if combined_lib.exists() {
        // Copy the combined shared library to the lib directory
        fs::copy(&combined_lib, &combined_lib_dest)
            .expect("Failed to copy libeloqstore_combine.so");

        // Copy to OUT_DIR so it can be embedded via include_bytes! in embedded_lib.rs
        if let Ok(out_dir) = std::env::var("OUT_DIR") {
            let embedded_lib_path = PathBuf::from(&out_dir).join("libeloqstore_combine.so");
            fs::copy(&combined_lib, &embedded_lib_path)
                .expect("Failed to copy libeloqstore_combine.so to OUT_DIR for embedding");
        }

        println!("cargo:rustc-link-search=native={}", lib_dir.display());

        // Strategy to help runtime loader find libeloqstore_combine.so:
        // 1. Copy .so to target/debug/ and target/release/ for easy access
        // 2. Set rpath with $ORIGIN to find .so relative to executable location
        // 3. Also set absolute path as fallback

        let lib_dir_str = lib_dir.display().to_string();

        // Strategy: Copy .so to target/debug/ and target/release/ directories
        // so executables can find it via $ORIGIN/.. rpath
        // This avoids the need for complex path calculations or environment variables

        let mut copied_to_target = false;

        // Try to detect target directory from OUT_DIR
        // OUT_DIR format: target/debug/build/eloqstore-sys-<hash>/out
        // Go up 3 levels: out -> build -> eloqstore-sys-<hash> -> build -> target/debug
        if let Ok(out_dir) = std::env::var("OUT_DIR") {
            let out_path = PathBuf::from(&out_dir);
            if let Some(target_profile_dir) = out_path.ancestors().nth(3) {
                // target_profile_dir is target/debug or target/release
                let target_lib = target_profile_dir.join("libeloqstore_combine.so");
                if let Some(parent) = target_lib.parent() {
                    if fs::create_dir_all(parent).is_ok() {
                        if fs::copy(&combined_lib_dest, &target_lib).is_ok() {
                            copied_to_target = true;
                        }
                    }
                }

                // Also copy to the other profile (debug <-> release)
                if let Some(target_base) = target_profile_dir.parent() {
                    let other_profile = if target_profile_dir.ends_with("debug") {
                        target_base.join("release")
                    } else {
                        target_base.join("debug")
                    };
                    let other_lib = other_profile.join("libeloqstore_combine.so");
                    if let Some(parent) = other_lib.parent() {
                        let _ = fs::create_dir_all(parent);
                        let _ = fs::copy(&combined_lib_dest, &other_lib);
                    }
                }
            }
        }

        // Fallback: try CARGO_TARGET_DIR if set
        if !copied_to_target {
            if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
                let target_debug_lib = PathBuf::from(&target_dir)
                    .join("debug")
                    .join("libeloqstore_combine.so");
                let target_release_lib = PathBuf::from(&target_dir)
                    .join("release")
                    .join("libeloqstore_combine.so");

                if let Some(parent) = target_debug_lib.parent() {
                    let _ = fs::create_dir_all(parent);
                    let _ = fs::copy(&combined_lib_dest, &target_debug_lib);
                }
                if let Some(parent) = target_release_lib.parent() {
                    let _ = fs::create_dir_all(parent);
                    let _ = fs::copy(&combined_lib_dest, &target_release_lib);
                }
            }
        }

        // Set rpath to find .so relative to executable
        // $ORIGIN refers to the directory containing the executable
        // For examples: target/debug/examples/basic_usage -> $ORIGIN/../.. points to target/debug/
        // For binaries: target/debug/my_app -> $ORIGIN/.. points to target/debug/
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/..");
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/../..");
        // Also add absolute path as fallback (for cargo run/test scenarios)
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir_str);

        // Choose linking strategy:
        // 1. Static linking (ELOQSTORE_STATIC_EXE=1): Link static library directly - no external .so needed
        // 2. Dynamic linking (default): Use .so file (requires .so to be available at runtime)
        //
        // For fully self-contained executables, use static linking:
        //   ELOQSTORE_STATIC_EXE=1 cargo build --example basic_usage
        let use_static = std::env::var("ELOQSTORE_STATIC_EXE").is_ok();

        if use_static {
            // Fully static: link the static library directly (no external .so needed)
            let static_lib = build_dir.join("libeloqstore.a");
            if static_lib.exists() {
                fs::copy(&static_lib, lib_dir.join("libeloqstore.a"))
                    .expect("Failed to copy libeloqstore.a");
                println!("cargo:rustc-link-lib=static=eloqstore");

                // Also need to link all abseil libraries statically
                let absl_dir = build_dir.join("external/abseil/absl");
                if absl_dir.exists() {
                    for entry in fs::read_dir(&absl_dir).expect("Failed to read abseil dir") {
                        let entry = entry.expect("Failed to get entry");
                        let path = entry.path();
                        if path.is_dir() {
                            println!("cargo:rustc-link-search=native={}", path.display());
                            if let Ok(entries) = fs::read_dir(&path) {
                                for file_entry in entries {
                                    let file_path = file_entry.expect("Failed to get file").path();
                                    if file_path.extension().map(|e| e == "a").unwrap_or(false) {
                                        let name = file_path.file_stem().unwrap().to_str().unwrap();
                                        if name.starts_with("libabsl_") {
                                            let lib_name = &name[3..];
                                            println!("cargo:rustc-link-lib={}", lib_name);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Fallback to dynamic if static not available
                println!("cargo:warning=Static library not found, falling back to dynamic linking");
                println!("cargo:rustc-link-lib=dylib=eloqstore_combine");
            }
        } else {
            // Dynamic linking: use the combined shared library (requires .so at runtime)
            println!("cargo:rustc-link-lib=dylib=eloqstore_combine");
        }

        // Link system libraries
        // In static mode, we need to link all dependencies that libeloqstore.a depends on
        if use_static {
            add_usr_local_link_search_paths_if_needed();

            // Static mode: link all system dependencies that libeloqstore.a needs
            println!("cargo:rustc-link-lib=zstd");
            println!("cargo:rustc-link-lib=glog");
            println!("cargo:rustc-link-lib=gflags");
            println!("cargo:rustc-link-lib=curl");
            println!("cargo:rustc-link-lib=crypto");
            println!("cargo:rustc-link-lib=ssl");
            println!("cargo:rustc-link-lib=uring");
            println!("cargo:rustc-link-lib=jsoncpp");
            println!("cargo:rustc-link-lib=boost_context");
            println!("cargo:rustc-link-lib=aws-cpp-sdk-s3");
            println!("cargo:rustc-link-lib=aws-cpp-sdk-core");
            println!("cargo:rustc-link-lib=aws-c-event-stream");
            println!("cargo:rustc-link-lib=aws-checksums");
            println!("cargo:rustc-link-lib=aws-c-auth");
            println!("cargo:rustc-link-lib=aws-c-cal");
            println!("cargo:rustc-link-lib=aws-c-common");
        }

        // System libraries that are always needed (pthread, dl, stdc++)
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=stdc++");

        // zstd in dynamic mode (if not using static)
        if !use_static {
            println!("cargo:rustc-link-lib=zstd");
        }
    } else {
        // Fallback to static library approach if combined library is not built
        let src_lib = build_dir.join("libeloqstore.a");
        if src_lib.exists() {
            fs::copy(&src_lib, lib_dir.join("libeloqstore.a"))
                .expect("Failed to copy libeloqstore.a");
        }

        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        println!("cargo:rustc-link-lib=static=eloqstore");

        // Find and link all abseil libraries
        let absl_dir = build_dir.join("external/abseil/absl");
        if absl_dir.exists() {
            // Add search paths for all subdirectories
            for entry in fs::read_dir(&absl_dir).expect("Failed to read abseil dir") {
                let entry = entry.expect("Failed to get entry");
                let path = entry.path();
                if path.is_dir() {
                    println!("cargo:rustc-link-search=native={}", path.display());
                }
            }

            // Find all .a files and link them
            for entry in fs::read_dir(&absl_dir).expect("Failed to read abseil dir") {
                let entry = entry.expect("Failed to get entry");
                let path = entry.path();
                if path.is_dir() {
                    if let Ok(entries) = fs::read_dir(&path) {
                        for file_entry in entries {
                            let file_path = file_entry.expect("Failed to get file").path();
                            if file_path.extension().map(|e| e == "a").unwrap_or(false) {
                                let name = file_path.file_stem().unwrap().to_str().unwrap();
                                if name.starts_with("libabsl_") {
                                    let lib_name = &name[3..];
                                    println!("cargo:rustc-link-lib={}", lib_name);
                                }
                            }
                        }
                    }
                }
            }
        }

        // System libraries that need to be linked (fallback mode)
        add_usr_local_link_search_paths_if_needed();
        println!("cargo:rustc-link-lib=zstd");
        println!("cargo:rustc-link-lib=glog");
        println!("cargo:rustc-link-lib=gflags");
        println!("cargo:rustc-link-lib=curl");
        println!("cargo:rustc-link-lib=crypto");
        println!("cargo:rustc-link-lib=ssl");
        println!("cargo:rustc-link-lib=uring");
        println!("cargo:rustc-link-lib=jsoncpp");
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=boost_context");
        println!("cargo:rustc-link-lib=aws-cpp-sdk-s3");
        println!("cargo:rustc-link-lib=aws-cpp-sdk-core");
        println!("cargo:rustc-link-lib=aws-c-event-stream");
        println!("cargo:rustc-link-lib=aws-checksums");
        println!("cargo:rustc-link-lib=aws-c-auth");
        println!("cargo:rustc-link-lib=aws-c-cal");
        println!("cargo:rustc-link-lib=aws-c-common");
        println!("cargo:rustc-link-lib=stdc++");
    }

    let include_path = PathBuf::from("vendor/include");
    println!("cargo:include={}", include_path.display());

    println!("cargo:rerun-if-changed=vendor/CMakeLists.txt");
    println!("cargo:rerun-if-changed=vendor/src/");
    println!("cargo:rerun-if-changed=vendor/include/");
    // Rebuild when repository root's submodule or external changes
    println!("cargo:rerun-if-changed=../../.gitmodules");
    println!("cargo:rerun-if-changed=../../external/");
}
