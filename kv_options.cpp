#include "kv_options.h"

#include <glog/logging.h>

#include <bit>
#include <boost/algorithm/string.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cctype>
#include <charconv>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "inih/cpp/INIReader.h"

namespace eloqstore
{
// Helper function to parse size with units (KB, MB, GB, TB)
static uint64_t ParseSizeWithUnit(std::string_view s)
{
    auto is_space = [](unsigned char c) { return std::isspace(c); };

    while (!s.empty() && is_space(static_cast<unsigned char>(s.front())))
    {
        s.remove_prefix(1);
    }
    while (!s.empty() && is_space(static_cast<unsigned char>(s.back())))
    {
        s.remove_suffix(1);
    }
    if (s.empty())
    {
        return 0;
    }

    uint64_t mul = 1;

    if (s.size() >= 2)
    {
        const char c1 =
            std::toupper(static_cast<unsigned char>(s[s.size() - 2]));
        const char c2 =
            std::toupper(static_cast<unsigned char>(s[s.size() - 1]));
        if (c1 == 'K' && c2 == 'B')
        {
            mul = 1ULL << 10;
            s.remove_suffix(2);
        }
        else if (c1 == 'M' && c2 == 'B')
        {
            mul = 1ULL << 20;
            s.remove_suffix(2);
        }
        else if (c1 == 'G' && c2 == 'B')
        {
            mul = 1ULL << 30;
            s.remove_suffix(2);
        }
        else if (c1 == 'T' && c2 == 'B')
        {
            mul = 1ULL << 40;
            s.remove_suffix(2);
        }
    }

    while (!s.empty() && is_space(static_cast<unsigned char>(s.back())))
    {
        s.remove_suffix(1);
    }
    if (s.empty())
    {
        return 0;
    }

    uint64_t v = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || ptr != s.data() + s.size())
    {
        return 0;
    }

    return v * mul;
}

static std::vector<std::string> SplitDaemonList(std::string_view raw)
{
    auto is_delim = [](char ch)
    {
        return ch == ',' || ch == ';' ||
               std::isspace(static_cast<unsigned char>(ch));
    };

    std::vector<std::string> urls;
    std::string current;
    auto flush = [&]()
    {
        size_t begin = 0;
        while (begin < current.size() &&
               std::isspace(static_cast<unsigned char>(current[begin])))
        {
            begin++;
        }
        size_t end = current.size();
        while (end > begin &&
               std::isspace(static_cast<unsigned char>(current[end - 1])))
        {
            end--;
        }
        if (end > begin)
        {
            urls.emplace_back(current.substr(begin, end - begin));
        }
        current.clear();
    };

    for (char ch : raw)
    {
        if (is_delim(ch))
        {
            flush();
        }
        else
        {
            current.push_back(ch);
        }
    }
    flush();
    return urls;
}
int KvOptions::LoadFromIni(const char *path)
{
    INIReader reader(path);
    if (int res = reader.ParseError(); res != 0)
    {
        return res;
    }
    constexpr char sec_run[] = "run";
    if (!reader.HasSection(sec_run))
    {
        return -2;
    }

    if (reader.HasValue(sec_run, "num_threads"))
    {
        num_threads = reader.GetUnsigned(sec_run, "num_threads", 1);
    }
    if (reader.HasValue(sec_run, "data_page_restart_interval"))
    {
        data_page_restart_interval =
            reader.GetUnsigned(sec_run, "data_page_restart_interval", 16);
    }
    if (reader.HasValue(sec_run, "index_page_restart_interval"))
    {
        index_page_restart_interval =
            reader.GetUnsigned(sec_run, "index_page_restart_interval", 16);
    }
    if (reader.HasValue(sec_run, "init_page_count"))
    {
        init_page_count =
            reader.GetUnsigned(sec_run, "init_page_count", 1 << 15);
    }
    if (reader.HasValue(sec_run, "skip_verify_checksum"))
    {
        skip_verify_checksum =
            reader.GetBoolean(sec_run, "skip_verify_checksum", false);
    }
    if (reader.HasValue(sec_run, "index_buffer_pool_size"))
    {
        std::string index_buffer_pool_size_str =
            reader.Get(sec_run, "index_buffer_pool_size", "");
        index_buffer_pool_size = ParseSizeWithUnit(index_buffer_pool_size_str);
    }
    if (reader.HasValue(sec_run, "manifest_limit"))
    {
        manifest_limit = reader.GetUnsigned(sec_run, "manifest_limit", 8 * MB);
    }
    if (reader.HasValue(sec_run, "fd_limit"))
    {
        fd_limit = reader.GetUnsigned(sec_run, "fd_limit", 1024);
    }
    if (reader.HasValue(sec_run, "io_queue_size"))
    {
        io_queue_size = reader.GetUnsigned(sec_run, "io_queue_size", 4096);
    }
    if (reader.HasValue(sec_run, "max_inflight_write"))
    {
        max_inflight_write =
            reader.GetUnsigned(sec_run, "max_inflight_write", 4096);
    }
    if (reader.HasValue(sec_run, "max_write_batch_pages"))
    {
        max_write_batch_pages =
            reader.GetUnsigned(sec_run, "max_write_batch_pages", 64);
    }
    if (reader.HasValue(sec_run, "buf_ring_size"))
    {
        buf_ring_size = reader.GetUnsigned(sec_run, "buf_ring_size", 1 << 10);
    }
    if (reader.HasValue(sec_run, "coroutine_stack_size"))
    {
        coroutine_stack_size =
            reader.GetUnsigned(sec_run, "coroutine_stack_size", 16 * KB);
    }

    if (reader.HasValue(sec_run, "num_retained_archives"))
    {
        num_retained_archives =
            reader.GetUnsigned(sec_run, "num_retained_archives", 0);
    }
    if (reader.HasValue(sec_run, "archive_interval_secs"))
    {
        archive_interval_secs =
            reader.GetUnsigned(sec_run, "archive_interval_secs", 86400);
    }
    if (reader.HasValue(sec_run, "max_archive_tasks"))
    {
        max_archive_tasks =
            reader.GetUnsigned(sec_run, "max_archive_tasks", 256);
    }
    if (reader.HasValue(sec_run, "file_amplify_factor"))
    {
        file_amplify_factor =
            reader.GetUnsigned(sec_run, "file_amplify_factor", 2);
    }
    if (reader.HasValue(sec_run, "local_space_limit"))
    {
        local_space_limit = reader.GetUnsigned(sec_run, "local_space_limit", 0);
    }
    if (reader.HasValue(sec_run, "reserve_space_ratio"))
    {
        reserve_space_ratio =
            reader.GetUnsigned(sec_run, "reserve_space_ratio", 100);
    }
    if (reader.HasValue(sec_run, "prewarm_cloud_cache"))
    {
        prewarm_cloud_cache =
            reader.GetBoolean(sec_run, "prewarm_cloud_cache", false);
    }
    if (reader.HasValue(sec_run, "cloud_store_daemon_ports") ||
        reader.HasValue(sec_run, "cloud_store_daemon_url"))
    {
        std::string raw =
            reader.Get(sec_run, "cloud_store_daemon_ports", "5572");
        // Backward compatibility: old key name
        if (raw == "5572")
        {
            raw = reader.Get(sec_run, "cloud_store_daemon_url", raw);
        }

        auto parsed = SplitDaemonList(raw);
        if (!parsed.empty())
        {
            cloud_store_daemon_ports = std::move(parsed);
        }
    }

    constexpr char sec_permanent[] = "permanent";
    if (!reader.HasSection(sec_permanent))
    {
        return -2;
    }

    if (reader.HasValue(sec_permanent, "store_path"))
    {
        std::string input = reader.Get(sec_permanent, "store_path", "");
        boost::split(store_path, input, boost::is_any_of(": "));
    }
    if (reader.HasValue(sec_permanent, "cloud_store_path"))
    {
        cloud_store_path = reader.Get(sec_permanent, "cloud_store_path", "");
    }
    if (reader.HasValue(sec_permanent, "data_page_size"))
    {
        std::string value_str =
            reader.Get(sec_permanent, "data_page_size", "4KB");
        uint64_t parsed_size = ParseSizeWithUnit(value_str);
        data_page_size = (parsed_size > 0) ? parsed_size : (1 << 12);
    }
    if (reader.HasValue(sec_permanent, "data_file_size"))
    {
        std::string value_str =
            reader.Get(sec_permanent, "data_file_size", "8MB");
        uint64_t parsed_size = ParseSizeWithUnit(value_str);
        uint32_t data_file_size = (parsed_size > 0) ? parsed_size : (8 * MB);
        // Calculate pages_per_file_shift from data_file_size
        // data_file_size = data_page_size * (1 << pages_per_file_shift)
        // So pages_per_file_shift = floor(log2(data_file_size /
        // data_page_size))
        uint32_t pages_per_file = data_file_size / data_page_size;
        if (pages_per_file == 0)
        {
            LOG(WARNING) << "data_file_size " << data_file_size
                         << " is smaller than data_page_size " << data_page_size
                         << ", falling back to one page.";
            pages_per_file_shift = 0;
        }
        else
        {
            pages_per_file_shift = std::numeric_limits<uint32_t>::digits -
                                   std::countl_zero(pages_per_file) - 1;
            if ((pages_per_file & (pages_per_file - 1)) != 0)
            {
                uint32_t adjusted_pages = 1U << pages_per_file_shift;
                uint64_t adjusted_size =
                    static_cast<uint64_t>(adjusted_pages) * data_page_size;
                LOG(WARNING) << "data_file_size " << data_file_size
                             << " is not a power-of-two multiple of page size "
                             << data_page_size << ", rounded down to "
                             << adjusted_size << " bytes.";
            }
        }
    }
    if (reader.HasValue(sec_permanent, "overflow_pointers"))
    {
        overflow_pointers =
            reader.GetUnsigned(sec_permanent, "overflow_pointers", 16);
    }
    if (reader.HasValue(sec_permanent, "data_append_mode"))
    {
        data_append_mode =
            reader.GetBoolean(sec_permanent, "data_append_mode", false);
    }
    return 0;
}

bool KvOptions::operator==(const KvOptions &other) const
{
    return num_threads == other.num_threads &&
           data_page_restart_interval == other.data_page_restart_interval &&
           index_page_restart_interval == other.index_page_restart_interval &&
           init_page_count == other.init_page_count &&
           skip_verify_checksum == other.skip_verify_checksum &&
           index_buffer_pool_size == other.index_buffer_pool_size &&
           manifest_limit == other.manifest_limit &&
           fd_limit == other.fd_limit && io_queue_size == other.io_queue_size &&
           max_inflight_write == other.max_inflight_write &&
           max_write_batch_pages == other.max_write_batch_pages &&
           buf_ring_size == other.buf_ring_size &&
           coroutine_stack_size == other.coroutine_stack_size &&
           num_retained_archives == other.num_retained_archives &&
           archive_interval_secs == other.archive_interval_secs &&
           max_archive_tasks == other.max_archive_tasks &&
           file_amplify_factor == other.file_amplify_factor &&
           local_space_limit == other.local_space_limit &&
           reserve_space_ratio == other.reserve_space_ratio &&
           prewarm_cloud_cache == other.prewarm_cloud_cache &&
           store_path == other.store_path &&
           cloud_store_path == other.cloud_store_path &&
           cloud_store_daemon_ports == other.cloud_store_daemon_ports &&
           data_page_size == other.data_page_size &&
           pages_per_file_shift == other.pages_per_file_shift &&
           overflow_pointers == other.overflow_pointers &&
           data_append_mode == other.data_append_mode;
}

size_t KvOptions::FilePageOffsetMask() const
{
    return (1 << pages_per_file_shift) - 1;
}

size_t KvOptions::DataFileSize() const
{
    return static_cast<size_t>(data_page_size) << pages_per_file_shift;
}
}  // namespace eloqstore
