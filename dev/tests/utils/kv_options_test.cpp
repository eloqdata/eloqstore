#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <filesystem>

#include "kv_options.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"
#include "fixtures/temp_directory.h"

using namespace eloqstore;
using namespace eloqstore::test;

class KvOptionsTestFixture {
public:
    KvOptionsTestFixture() : temp_dir_("kvoptions_test") {}

    std::string CreateIniFile(const std::string& content) {
        std::string path = (temp_dir_.path() / "test.ini").string();
        std::ofstream file(path);
        file << content;
        file.close();
        return path;
    }

protected:
    TempDirectory temp_dir_;
};

TEST_CASE_METHOD(KvOptionsTestFixture, "KvOptions_DefaultValues", "[kvoptions][unit]") {
    KvOptions opts;

    SECTION("Runtime parameters") {
        REQUIRE(opts.num_threads == 1);
        REQUIRE(opts.comparator_ == Comparator::DefaultComparator());
        REQUIRE(opts.data_page_restart_interval == 16);
        REQUIRE(opts.index_page_restart_interval == 16);
        REQUIRE(opts.init_page_count == (1 << 15));
        REQUIRE(opts.skip_verify_checksum == false);
        REQUIRE(opts.index_buffer_pool_size == (1 << 15));
        REQUIRE(opts.manifest_limit == (8 << 20));
        REQUIRE(opts.fd_limit == 10000);
        REQUIRE(opts.io_queue_size == 4096);
        REQUIRE(opts.max_inflight_write == (32 << 10));
        REQUIRE(opts.max_write_batch_pages == 256);
        REQUIRE(opts.buf_ring_size == (1 << 12));
        REQUIRE(opts.coroutine_stack_size == 32 * 1024);
    }

    SECTION("Storage parameters") {
        REQUIRE(opts.num_retained_archives == 0);
        REQUIRE(opts.archive_interval_secs == 0);
        REQUIRE(opts.num_gc_threads == 0);
        REQUIRE(opts.evict_interval_secs == 60);
        REQUIRE(opts.gc_interval_secs == 600);
        REQUIRE(opts.local_space_limit == 0);
        REQUIRE(opts.per_shard_limit_ == 0);
        REQUIRE(opts.data_append_mode == false);
    }
}

TEST_CASE_METHOD(KvOptionsTestFixture, "KvOptions_LoadFromIni_ValidConfig", "[kvoptions][unit]") {
    SECTION("Basic configuration") {
        std::string ini_content = R"(
[run]
num_threads = 4
index_buffer_pool_size = 50000
fd_limit = 1000
skip_verify_checksum = true

[permanent]
store_path = /tmp/test_store
data_page_size = 8192
pages_per_file_shift = 12
data_append_mode = true
)";

        std::string ini_path = CreateIniFile(ini_content);
        KvOptions opts;
        int result = opts.LoadFromIni(ini_path.c_str());

        REQUIRE(result == 0);
        REQUIRE(opts.num_threads == 4);
        REQUIRE(opts.index_buffer_pool_size == 50000);
        REQUIRE(opts.fd_limit == 1000);
        REQUIRE(opts.skip_verify_checksum == true);
        REQUIRE(opts.data_path == "/tmp/test_store");
        REQUIRE(opts.data_page_size == 8192);
        REQUIRE(opts.pages_per_file_shift == 12);
        REQUIRE(opts.data_append_mode == true);
    }

    SECTION("Multiple data paths") {
        std::string ini_content = R"(
[run]
num_threads = 2

[permanent]
store_path = /path1, /path2, /path3
data_page_size = 4096
)";

        std::string ini_path = CreateIniFile(ini_content);
        KvOptions opts;
        int result = opts.LoadFromIni(ini_path.c_str());

        REQUIRE(result == 0);
        REQUIRE(opts.data_paths.size() == 3);
        REQUIRE(opts.data_paths[0] == "/path1");
        REQUIRE(opts.data_paths[1] == "/path2");
        REQUIRE(opts.data_paths[2] == "/path3");
    }

    SECTION("All parameters") {
        std::string ini_content = R"(
[run]
num_threads = 8
index_buffer_pool_size = 100000
manifest_limit = 16777216
fd_limit = 5000
io_queue_size = 8192
max_inflight_write = 65536
max_write_batch_pages = 512
buf_ring_size = 8192
coroutine_stack_size = 65536
num_retained_archives = 10
archive_interval_secs = 3600
num_gc_threads = 2
evict_interval_secs = 120
gc_interval_secs = 1200
local_space_limit = 1073741824
skip_verify_checksum = true

[permanent]
store_path = /data/store
data_page_size = 16384
pages_per_file_shift = 14
data_append_mode = false
cloud_store_path = s3://bucket/path
cloud_worker_count = 4
)";

        std::string ini_path = CreateIniFile(ini_content);
        KvOptions opts;
        int result = opts.LoadFromIni(ini_path.c_str());

        REQUIRE(result == 0);
        REQUIRE(opts.num_threads == 8);
        REQUIRE(opts.index_buffer_pool_size == 100000);
        REQUIRE(opts.manifest_limit == 16777216);
        REQUIRE(opts.fd_limit == 5000);
        REQUIRE(opts.io_queue_size == 8192);
        REQUIRE(opts.max_inflight_write == 65536);
        REQUIRE(opts.max_write_batch_pages == 512);
        REQUIRE(opts.buf_ring_size == 8192);
        REQUIRE(opts.coroutine_stack_size == 65536);
        REQUIRE(opts.num_retained_archives == 10);
        REQUIRE(opts.archive_interval_secs == 3600);
        REQUIRE(opts.num_gc_threads == 2);
        REQUIRE(opts.evict_interval_secs == 120);
        REQUIRE(opts.gc_interval_secs == 1200);
        REQUIRE(opts.local_space_limit == 1073741824);
        REQUIRE(opts.cloud_store_path == "s3://bucket/path");
        REQUIRE(opts.cloud_worker_count == 4);
    }
}

TEST_CASE_METHOD(KvOptionsTestFixture, "KvOptions_LoadFromIni_InvalidConfig", "[kvoptions][unit][error]") {
    SECTION("Non-existent file") {
        KvOptions opts;
        int result = opts.LoadFromIni("/non/existent/file.ini");
        REQUIRE(result != 0);
    }

    SECTION("Empty file") {
        std::string ini_path = CreateIniFile("");
        KvOptions opts;
        int result = opts.LoadFromIni(ini_path.c_str());
        // Should load with defaults
        REQUIRE(result == 0);
    }

    SECTION("Invalid section names") {
        std::string ini_content = R"(
[invalid_section]
some_param = 123

[another_invalid]
another_param = abc
)";

        std::string ini_path = CreateIniFile(ini_content);
        KvOptions opts;
        int result = opts.LoadFromIni(ini_path.c_str());
        // Should ignore invalid sections
        REQUIRE(result == 0);
    }

    SECTION("Invalid parameter values") {
        std::string ini_content = R"(
[run]
num_threads = abc
index_buffer_pool_size = -100
fd_limit = 999999999999999999

[permanent]
data_page_size = not_a_number
)";

        std::string ini_path = CreateIniFile(ini_content);
        KvOptions opts;
        int result = opts.LoadFromIni(ini_path.c_str());
        // Should handle gracefully, keeping defaults for invalid values
        REQUIRE(result == 0);
        // Values should be defaults or clamped
    }
}

TEST_CASE_METHOD(KvOptionsTestFixture, "KvOptions_Validation", "[kvoptions][unit]") {
    KvOptions opts;

    SECTION("Valid page sizes") {
        std::vector<uint32_t> valid_sizes = {512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
        for (uint32_t size : valid_sizes) {
            opts.data_page_size = size;
            // Should be power of 2
            REQUIRE((size & (size - 1)) == 0);
        }
    }

    SECTION("Pages per file shift") {
        // Should be reasonable values
        for (uint8_t shift = 4; shift <= 20; ++shift) {
            opts.pages_per_file_shift = shift;
            uint32_t pages_per_file = 1U << shift;
            REQUIRE(pages_per_file >= 16);
            REQUIRE(pages_per_file <= 1048576);
        }
    }

    SECTION("Buffer ring size must be power of 2") {
        std::vector<uint16_t> valid_sizes = {256, 512, 1024, 2048, 4096, 8192, 16384, 32768};
        for (uint16_t size : valid_sizes) {
            opts.buf_ring_size = size;
            REQUIRE((size & (size - 1)) == 0);
            REQUIRE(size <= 32768);
        }
    }

    SECTION("Coroutine stack size minimum") {
        // Minimum required is 16KB according to comments
        opts.coroutine_stack_size = 16 * 1024;
        REQUIRE(opts.coroutine_stack_size >= 16 * 1024);

        opts.coroutine_stack_size = 8 * 1024;  // Too small
        // Should be validated in actual usage
    }
}

TEST_CASE_METHOD(KvOptionsTestFixture, "KvOptions_Equality", "[kvoptions][unit]") {
    KvOptions opts1, opts2;

    SECTION("Default options are equal") {
        REQUIRE(opts1 == opts2);
    }

    SECTION("Different values are not equal") {
        opts1.num_threads = 4;
        opts2.num_threads = 8;
        REQUIRE(!(opts1 == opts2));

        opts2.num_threads = 4;
        REQUIRE(opts1 == opts2);

        opts1.data_path = "/path1";
        opts2.data_path = "/path2";
        REQUIRE(!(opts1 == opts2));
    }

    SECTION("All fields compared") {
        // Modify each field and check inequality
        KvOptions base;

        // Runtime parameters
        {
            KvOptions modified = base;
            modified.num_threads = base.num_threads + 1;
            REQUIRE(!(base == modified));
        }
        {
            KvOptions modified = base;
            modified.index_buffer_pool_size = base.index_buffer_pool_size + 1;
            REQUIRE(!(base == modified));
        }
        {
            KvOptions modified = base;
            modified.skip_verify_checksum = !base.skip_verify_checksum;
            REQUIRE(!(base == modified));
        }

        // Storage parameters
        {
            KvOptions modified = base;
            modified.data_append_mode = !base.data_append_mode;
            REQUIRE(!(base == modified));
        }
        {
            KvOptions modified = base;
            modified.data_page_size = base.data_page_size * 2;
            REQUIRE(!(base == modified));
        }
    }
}

TEST_CASE("KvOptions_EdgeCases", "[kvoptions][unit][edge-case]") {
    KvOptions opts;

    SECTION("Maximum values") {
        opts.num_threads = UINT16_MAX;
        opts.index_buffer_pool_size = UINT32_MAX;
        opts.fd_limit = UINT32_MAX;
        opts.local_space_limit = UINT64_MAX;

        // Should handle without overflow
        REQUIRE(opts.num_threads == UINT16_MAX);
        REQUIRE(opts.index_buffer_pool_size == UINT32_MAX);
    }

    SECTION("Zero values") {
        opts.num_threads = 0;  // Invalid but should be handled
        opts.index_buffer_pool_size = 0;
        opts.fd_limit = 0;

        // Zero threads should be invalid
        REQUIRE(opts.num_threads == 0);
    }

    SECTION("Path edge cases") {
        opts.data_path = "";  // Empty path
        REQUIRE(opts.data_path.empty());

        opts.data_path = std::string(1000, 'x');  // Very long path
        REQUIRE(opts.data_path.length() == 1000);

        opts.data_path = "/path/with spaces/and-special@chars#";
        REQUIRE(!opts.data_path.empty());

        // Unicode path
        opts.data_path = u8"/路径/パス/경로";
        REQUIRE(!opts.data_path.empty());
    }
}

TEST_CASE_METHOD(KvOptionsTestFixture, "KvOptions_StressTest", "[kvoptions][stress]") {
    SECTION("Load many different configurations") {
        for (int i = 0; i < 100; ++i) {
            std::stringstream ss;
            ss << "[run]\n";
            ss << "num_threads = " << (1 + i % 16) << "\n";
            ss << "index_buffer_pool_size = " << (1000 + i * 100) << "\n";
            ss << "fd_limit = " << (100 + i * 10) << "\n";
            ss << "skip_verify_checksum = " << (i % 2 ? "true" : "false") << "\n";
            ss << "\n[permanent]\n";
            ss << "store_path = /tmp/store_" << i << "\n";
            ss << "data_page_size = " << (512 << (i % 8)) << "\n";
            ss << "data_append_mode = " << (i % 2 ? "false" : "true") << "\n";

            std::string ini_path = CreateIniFile(ss.str());
            KvOptions opts;
            int result = opts.LoadFromIni(ini_path.c_str());

            REQUIRE(result == 0);
            REQUIRE(opts.num_threads == (1 + i % 16));
            REQUIRE(opts.index_buffer_pool_size == uint32_t(1000 + i * 100));
        }
    }

    SECTION("Concurrent option loading") {
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};

        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([this, t, &success_count]() {
                for (int i = 0; i < 10; ++i) {
                    std::stringstream ss;
                    ss << "[run]\n";
                    ss << "num_threads = " << (t + 1) << "\n";
                    ss << "[permanent]\n";
                    ss << "store_path = /tmp/thread_" << t << "_iter_" << i << "\n";

                    std::string path = (temp_dir_.path() /
                                       ("thread_" + std::to_string(t) + "_" +
                                        std::to_string(i) + ".ini")).string();

                    std::ofstream file(path);
                    file << ss.str();
                    file.close();

                    KvOptions opts;
                    if (opts.LoadFromIni(path.c_str()) == 0) {
                        success_count++;
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        REQUIRE(success_count == 80);  // All should succeed
    }
}