#include "kv_options.h"

#include <boost/algorithm/string.hpp>

#include "inih/cpp/INIReader.h"

namespace eloqstore
{
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
        index_buffer_pool_size =
            reader.GetUnsigned(sec_run, "index_buffer_pool_size", UINT32_MAX);
    }
    if (reader.HasValue(sec_run, "manifest_limit"))
    {
        manifest_limit =
            reader.GetUnsigned(sec_run, "manifest_limit", 16 << 20);
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
            reader.GetUnsigned(sec_run, "coroutine_stack_size", 16 * 1024);
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
    if (reader.HasValue(sec_run, "num_gc_threads"))
    {
        num_gc_threads = reader.GetUnsigned(sec_run, "num_gc_threads", 1);
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
    if (reader.HasValue(sec_run, "cloud_store_daemon_url"))
    {
        cloud_store_daemon_url = reader.Get(
            sec_run, "cloud_store_daemon_url", "http://127.0.0.1:5572");
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
        data_page_size =
            reader.GetUnsigned(sec_permanent, "data_page_size", 1 << 12);
    }
    if (reader.HasValue(sec_permanent, "pages_per_file_shift"))
    {
        pages_per_file_shift =
            reader.GetUnsigned(sec_permanent, "pages_per_file_shift", 11);
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
           num_gc_threads == other.num_gc_threads &&
           local_space_limit == other.local_space_limit &&
           reserve_space_ratio == other.reserve_space_ratio &&
           store_path == other.store_path &&
           cloud_store_path == other.cloud_store_path &&
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
    return data_page_size << pages_per_file_shift;
}
}  // namespace eloqstore