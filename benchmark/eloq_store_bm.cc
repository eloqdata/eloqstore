#include "eloq_store_bm.h"

#include <cassert>
#include <iomanip>
#include <numeric>

#include "../kv_options.h"

namespace EloqStoreBM
{
static const std::string table_name_str = "eloq_store_bm";

thread_local bool worker_started = false;
thread_local size_t finished_write_req_cnt = 0;
thread_local size_t total_read_req_cnt = 0;
thread_local size_t finished_read_req_cnt = 0;
thread_local size_t key_not_found_cnt = 0;
thread_local bool process_thr = false;
thread_local bool process_six = false;
thread_local bool process_nin = false;
thread_local std::list<uint64_t> req_latency;
thread_local bool worker_terminated = false;
thread_local object_generator read_obj_gen;

inline void start_op(const std::string &command, uint64_t &start_ts)
{
    size_t ops = 0;
    if (command == "GET")
    {
        ops = finished_read_req_cnt + 1;
    }

    if ((ops / Benchmark::sample_granular) !=
        ((ops - 1) / Benchmark::sample_granular))
    {
        start_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
    }
    else
    {
        start_ts = 0;
    }
}

inline void finish_op(const std::string &command, uint64_t start_ts)
{
    if (start_ts > 0)
    {
        uint64_t now =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        uint64_t d = now - start_ts;
        assert(
            [&]()
            {
                if (d >= 100000000)
                {
                    DLOG(INFO) << "latency: " << d << " now: " << now
                               << " start: " << start_ts;
                }
                return d < 100000000;
            }());
        req_latency.push_back(d);
    }

    if (command == "GET")
    {
        ++finished_read_req_cnt;
    }
}

LoadPartitionsOperation::LoadPartitionsOperation(size_t id,
                                                 uint64_t part_data_cnt,
                                                 const Benchmark *bm)
    : self_id_(id), part_idx_(0), target_data_cnt_(part_data_cnt), bm_(bm)
{
    req_ = std::make_unique<::eloqstore::BatchWriteRequest>();
    size_t part_per_req = bm_->partition_count_ / bm_->concurr_req_count_;
    part_ids_.resize(part_per_req);
    for (size_t i = 0; i < part_per_req; ++i)
    {
        part_ids_[i] = i * bm->concurr_req_count_ + self_id_;
    }
}

ReadOperation::ReadOperation(const Benchmark *bm) : start_ts_(0), bm_(bm)
{
    req_ = std::make_unique<::eloqstore::ReadRequest>();
    size_t prefix_len = bm_->key_prefix_.size();
    key_.reserve(bm_->key_byte_size_ + prefix_len);
}

void BMResult::Wait(uint64_t start_ts)
{
    if (bm_->command_ == "LOAD")
    {
        // wait the workers
        std::unique_lock<std::mutex> lk(bm_mux_);
        // Wait all the request finished.
        LOG(INFO) << "Waitting all " << worker_count_
                  << " workers load finish.";
        bm_cv_.wait(
            lk,
            [this]()
            { return bm_finished_load_part_cnt_ == bm_->partition_count_; });
    }
    else if (bm_->command_ == "GET")
    {
        uint64_t end_ts = start_ts + bm_->total_test_time_sec_ * 1000 * 1000;
        LOG(INFO) << "All task start and will stop after "
                  << bm_->total_test_time_sec_
                  << "s. Start timestamp: " << start_ts
                  << ", End timestamp: " << end_ts;

        uint32_t wait_time_ms = bm_->total_test_time_sec_ * 1000;

        std::unique_lock<std::mutex> lk(bm_mux_);
        bm_total_read_req_cnt_ += bm_->concurr_req_count_;

        while (!bm_cv_.wait_for(
            lk,
            std::chrono::milliseconds(wait_time_ms),
            [&]()
            {
                uint64_t now =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now()
                            .time_since_epoch())
                        .count();
                return now >= end_ts || bm_failed_;
            }))
        {
            // Continue to sleep.
            uint64_t mid_ts =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now()
                        .time_since_epoch())
                    .count();
            wait_time_ms = (end_ts - mid_ts) / 1000;
            DLOG(INFO) << "mid timestamp: " << mid_ts;
        }
        lk.unlock();

        auto stop_tp = std::chrono::high_resolution_clock::now();
        LOG(INFO) << "Stop timestamp: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         stop_tp.time_since_epoch())
                         .count();

        // Terminate the test.
        bm_terminated_.store(true, std::memory_order_acq_rel);
        // Wait all the request finished.
        LOG(INFO) << "Waitting all the worker finished.";
        lk.lock();
        bm_cv_.wait(lk,
                    [this]()
                    {
                        return (bm_total_finished_read_req_cnt_ ==
                                bm_total_read_req_cnt_) ||
                               bm_failed_;
                    });
    }
}

void BMResult::Nofity(bool succ,
                      size_t finished_part,
                      size_t finished_req_cnt,
                      size_t sent_req_cnt,
                      std::list<uint64_t> *latency)
{
    std::unique_lock<std::mutex> lk(bm_mux_);
    bm_failed_ = !succ;

    if (bm_->command_ == "LOAD")
    {
        bm_finished_load_part_cnt_ += finished_part;
        bm_finished_write_req_cnt_ += finished_req_cnt;
        if (bm_finished_load_part_cnt_ == bm_->partition_count_)
        {
            bm_cv_.notify_one();
        }
    }
    else if (bm_->command_ == "GET")
    {
        bm_total_read_req_cnt_ += sent_req_cnt;
        bm_total_finished_read_req_cnt_ += finished_req_cnt;

        if (latency != nullptr)
        {
            req_latency_all_.merge(*latency);
        }

        if ((bm_total_finished_read_req_cnt_ == bm_total_read_req_cnt_) ||
            bm_failed_)
        {
            bm_cv_.notify_one();
        }
    }
}

void BMResult::ReportResult(uint64_t end_ts)
{
    if (bm_failed_)
    {
        LOG(INFO) << "Benchmark test failed.";
        return;
    }

    // QPS
    auto duration = end_ts - bm_->start_ts_;
    double duration_sec = static_cast<double>(duration) / 1000000.0;
    if (bm_->command_ == "LOAD")
    {
        LOG(INFO) << "All " << bm_finished_write_req_cnt_
                  << " command finished with duration: " << duration
                  << "(us). The QPS:" << std::fixed << std::setprecision(2)
                  << static_cast<double>(bm_finished_write_req_cnt_ /
                                         duration_sec);
    }
    else if (bm_->command_ == "GET")
    {
        LOG(INFO) << "All " << bm_total_read_req_cnt_
                  << " command finished with duration: " << duration
                  << "(us). The QPS:" << std::fixed << std::setprecision(2)
                  << static_cast<double>(bm_total_read_req_cnt_ / duration_sec);
    }

    // Latency
    ReportLatency();
}

void BMResult::ReportLatency()
{
    size_t metr_size = req_latency_all_.size();
    if (!metr_size)
    {
        return;
    }

    size_t p50 = metr_size * 0.5;
    size_t p90 = metr_size * 0.9;
    size_t p95 = metr_size * 0.95;
    size_t p99 = metr_size * 0.99;
    size_t p99_9 = metr_size * 0.999;
    size_t p99_99 = metr_size * 0.9999;
    int64_t l_p50 = 0, l_p90 = 0, l_p95 = 0, l_p99 = 0, l_p99_9 = 0,
            l_p99_99 = 0;

    size_t n = 0;
    for (auto l : req_latency_all_)
    {
        ++n;
        if (n == p50)
        {
            l_p50 = l;
        }
        else if (n == p90)
        {
            l_p90 = l;
        }
        else if (n == p95)
        {
            l_p95 = l;
        }
        else if (n == p99)
        {
            l_p99 = l;
        }
        else if (n == p99_9)
        {
            l_p99_9 = l;
        }
        else if (n == p99_99)
        {
            l_p99_99 = l;
        }
    }

    uint64_t mean = 0, sum = 0;
    sum = std::accumulate(req_latency_all_.begin(), req_latency_all_.end(), 0);
    mean = sum / metr_size;
    DLOG(INFO) << "latency count: " << metr_size << " sum: " << sum;

    LOG(INFO) << "Latency: Min->" << req_latency_all_.front() << ", Max->"
              << req_latency_all_.back() << ", Mean->" << mean << ", p50->"
              << l_p50 << ", p90->" << l_p90 << ", p95->" << l_p95 << ", p99->"
              << l_p99 << ", p99.9->" << l_p99_9 << ", p99.99->" << l_p99_99;
}

void Benchmark::OnBatchWrite(::eloqstore::KvRequest *req)
{
    ::eloqstore::BatchWriteRequest *write_req =
        static_cast<::eloqstore::BatchWriteRequest *>(req);

    LoadPartitionsOperation *load_op = static_cast<LoadPartitionsOperation *>(
        reinterpret_cast<void *>(req->UserData()));
    BMResult &bm_res = load_op->bm_->result_;

    if (!worker_started)
    {
        worker_started = true;
        finished_write_req_cnt = 0;
    }
    ++finished_write_req_cnt;

    if (write_req->Error() != ::eloqstore::KvError::NoError)
    {
        LOG(ERROR) << "Write to EloqStore failed with error code: "
                   << static_cast<uint32_t>(write_req->Error())
                   << ", error message: " << write_req->ErrMessage();

        size_t remain_part_cnt =
            load_op->part_ids_.size() - 1 - load_op->part_idx_;

        bm_res.Nofity(false, remain_part_cnt);
        return;
    }

    uint64_t batch_cnt_max =
        (load_op->bm_->batch_byte_size_ << 20) / load_op->bm_->value_byte_size_;

    load_op->loaded_data_cnt_ += load_op->batch_cnt_;

    // Send next request.
    const ::eloqstore::TableIdent &eloq_store_table_id = req->TableId();

    // Check the benckmark test status
    if (load_op->loaded_data_cnt_ >= load_op->target_data_cnt_)
    {
        // The current partition has completed.
        DLOG(INFO) << "Load data into table: " << eloq_store_table_id
                   << " finished after " << load_op->loaded_data_cnt_
                   << " keys for partition#"
                   << load_op->part_ids_[load_op->part_idx_];
        // Check the process
        {
            bool output_log = false;
            unsigned int loaded =
                (load_op->part_idx_ * 100) / load_op->part_ids_.size();
            if (!process_nin && loaded >= 90)
            {
                process_nin = true;
                output_log = true;
            }
            else if (!process_six && loaded >= 60)
            {
                process_six = true;
                output_log = true;
            }
            else if (!process_thr && loaded >= 30)
            {
                process_thr = true;
                output_log = true;
            }

            if (output_log)
            {
                LOG(INFO) << "**********The process of the worker is " << loaded
                          << "**************************************";
            }
        }

        if (++load_op->part_idx_ < load_op->part_ids_.size())
        {
            // reset
            load_op->loaded_data_cnt_ = 0;
            load_op->batch_cnt_ = 0;

            // load data into next partition
            uint32_t part_id = load_op->part_ids_[load_op->part_idx_];
            ::eloqstore::TableIdent table_id(table_name_str, part_id);
            // Construct a batch value

            bool last_batch = (load_op->target_data_cnt_ -
                               load_op->loaded_data_cnt_) < batch_cnt_max;

            size_t batch_cnt =
                last_batch
                    ? (load_op->target_data_cnt_ - load_op->loaded_data_cnt_)
                    : batch_cnt_max;
            batch_records entries;
            entries.reserve(batch_cnt);

            // object generator.
            object_generator &obj_gen = load_op->bm_->load_obj_gens_[part_id];

            GenBatchRecord(*load_op->bm_, obj_gen, entries, batch_cnt);
            load_op->batch_cnt_ = batch_cnt;

            write_req->SetArgs(table_id, std::move(entries));
            // Send to eloq store.
            if (!load_op->bm_->eloq_store_->ExecAsyn(
                    write_req, write_req->UserData(), OnBatchWrite))
            {
                LOG(ERROR) << "Send write request to EloqStore failed "
                              "for table:"
                           << table_id;

                size_t remain_part_cnt =
                    load_op->part_ids_.size() - 1 - load_op->part_idx_;

                bm_res.Nofity(false, remain_part_cnt);
            }

            return;
        }
        // else: all partition finished.

        LOG(INFO) << "EloqStore worker finished after hanle "
                  << finished_write_req_cnt << " requests.";
        // All request finished on this worker thread.
        bm_res.Nofity(true, load_op->part_ids_.size(), finished_write_req_cnt);

        // reset.
        finished_write_req_cnt = 0;
        return;
    }
    // else: continue to load into the current partition.

    // Construct a batch value
    bool last_batch =
        (load_op->target_data_cnt_ - load_op->loaded_data_cnt_) < batch_cnt_max;
    size_t batch_cnt =
        last_batch ? (load_op->target_data_cnt_ - load_op->loaded_data_cnt_)
                   : batch_cnt_max;

    batch_records entries;
    entries.reserve(batch_cnt);

    uint32_t part_id = eloq_store_table_id.partition_id_;
    // object generator.
    object_generator &obj_gen = load_op->bm_->load_obj_gens_[part_id];
    GenBatchRecord(*load_op->bm_, obj_gen, entries, batch_cnt);
    load_op->batch_cnt_ = batch_cnt;

    write_req->SetArgs(eloq_store_table_id, std::move(entries));
    // Send to eloq store.
    if (!load_op->bm_->eloq_store_->ExecAsyn(
            write_req, write_req->UserData(), OnBatchWrite))
    {
        LOG(ERROR) << "Send write request to EloqStore failed for table:"
                   << eloq_store_table_id;

        size_t remain_part_cnt =
            load_op->part_ids_.size() - 1 - load_op->part_idx_;
        bm_res.Nofity(false, remain_part_cnt);
    }
}

void Benchmark::GenBatchRecord(const Benchmark &bm,
                               object_generator &obj_gen,
                               batch_records &out_buff,
                               size_t batch_cnt)
{
    auto timestamp_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();

    int8_t iter = obj_iter_type(bm.key_pattern_, SET_CMD_IDX);
    uint64_t key_index = 0;
    for (size_t key_i = 0; key_i < batch_cnt && key_index < bm.key_maximum_;
         ++key_i)
    {
        ::eloqstore::WriteDataEntry entry;

        key_index = obj_gen.get_key_index(iter);
        obj_gen.generate_key(key_index, entry.key_);

        uint32_t value_len = 0;
        const char *val_ptr = obj_gen.get_value(key_index, &value_len);

        entry.val_.append(val_ptr, value_len);
        entry.timestamp_ = timestamp_ts;
        entry.op_ = ::eloqstore::WriteOp::Upsert;

        // move the key value to entry;
        out_buff.emplace_back(std::move(entry));
    }

#ifndef NDEBUG
    auto end_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();

    assert(std::is_sorted(out_buff.begin(),
                          out_buff.end(),
                          [](const ::eloqstore::WriteDataEntry &lhs,
                             const ::eloqstore::WriteDataEntry &rhs)
                          { return lhs.key_ < rhs.key_; }));

    std::ostringstream k_oss;
    for (size_t i = 0; i < 2 && i < out_buff.size(); ++i)
    {
        auto &kvalue = out_buff[i];
        k_oss << " -> 0x";
        k_oss << std::hex << std::setfill('0');
        size_t key_len = kvalue.key_.size();
        for (size_t i = 0; i < key_len; ++i)
        {
            k_oss << std::setw(2)
                  << static_cast<unsigned>(
                         static_cast<uint8_t>(kvalue.key_[i]));
        }
        k_oss << ";";
    }

    std::ostringstream oss;
    for (size_t i = 0; i < 1 && i < out_buff.size(); ++i)
    {
        auto &kvalue = out_buff[i];
        oss << "-> 0x";
        oss << std::hex << std::setfill('0');
        size_t value_len = kvalue.val_.size();
        for (size_t j = 0; j < value_len; ++j)
        {
            oss << std::setw(2)
                << static_cast<unsigned>(static_cast<uint8_t>(kvalue.val_[j]));
        }
        oss << ";";
    }
    DLOG(INFO) << "Batch values count: " << out_buff.size()
               << ". The first five keys: " << k_oss.str()
               << ", values: " << oss.str()
               << ". Cost: " << (end_ts - timestamp_ts) << "(us).";
#endif
}

void Benchmark::OnRead(::eloqstore::KvRequest *req)
{
    ::eloqstore::ReadRequest *read_req =
        static_cast<::eloqstore::ReadRequest *>(req);
    ReadOperation *read_op =
        static_cast<ReadOperation *>(reinterpret_cast<void *>(req->UserData()));
    BMResult &bm_res = read_op->bm_->result_;

    if (!worker_started)
    {
        worker_started = true;

        uint32_t seed_num =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count() &
            0xFFFFFFFF;
        uint32_t curr_part_id = req->TableId().partition_id_;

        read_obj_gen.set_random_data(true);
        read_obj_gen.set_random_seed(seed_num + curr_part_id);
        read_obj_gen.set_key_size(read_op->bm_->key_byte_size_);
        read_obj_gen.set_data_size_fixed(read_op->bm_->value_byte_size_);
        read_obj_gen.set_key_prefix(read_op->bm_->key_prefix_.data());
        read_obj_gen.set_key_range(read_op->bm_->key_minimum_,
                                   read_op->bm_->key_maximum_);
    }

    // finish this operation
    finish_op(read_op->bm_->command_, read_op->start_ts_);

    if (read_req->Error() == ::eloqstore::KvError::NotFound)
    {
        ++key_not_found_cnt;
    }
    else if (read_req->Error() != ::eloqstore::KvError::NoError)
    {
        LOG(ERROR) << "Read from EloqStore failed with error code: "
                   << static_cast<uint32_t>(read_req->Error())
                   << ", error message: " << read_req->ErrMessage();
        if (!worker_terminated)
        {
            worker_terminated = true;
            bm_res.Nofity(false);
        }
        return;
    }

    // Check the benckmark test status
    if (bm_res.bm_terminated_.load(std::memory_order_relaxed))
    {
        if (!worker_terminated)
        {
            worker_terminated = true;

            req_latency.sort();

            bm_res.Nofity(true,
                          0,
                          finished_read_req_cnt,
                          total_read_req_cnt,
                          &req_latency);

            assert(req_latency.size() == 0);
        }
        else
        {
            // discard the latency metrics
            // Just update the finished request count.
            bm_res.Nofity(true, 0, 1);
        }

        total_read_req_cnt = 0;
        finished_read_req_cnt = 0;
        return;
    }

    // Send next request.
    // get next key randomly.
    int8_t iter = obj_iter_type(read_op->bm_->key_pattern_, GET_CMD_IDX);
    uint64_t key_index = read_obj_gen.get_key_index(iter);
    read_op->key_.clear();
    read_obj_gen.generate_key(key_index, read_op->key_);

    // get next partition id randomly
    uint32_t next_part_id = key_index % read_op->bm_->partition_count_;
    ::eloqstore::TableIdent table_id(table_name_str, next_part_id);
#ifndef NDEBUG
    {
        std::ostringstream oss;
        oss << "0x";
        oss << std::hex << std::setfill('0');
        size_t key_len = read_obj_gen.get_key_len();
        std::string &key = read_op->key_;
        for (size_t i = 0; i < key_len; ++i)
        {
            oss << std::setw(2)
                << static_cast<unsigned>(static_cast<uint8_t>(key[i]));
        }
        DLOG(INFO) << "To read key: " << oss.str()
                   << " with key length: " << key.size() << " of partition#"
                   << next_part_id << " from EloqStore.";
    }
#endif

    read_req->SetArgs(table_id, read_op->key_);

    // start this operation.
    start_op(read_op->bm_->command_, read_op->start_ts_);

    // Send to eloq store.
    if (!read_op->bm_->eloq_store_->ExecAsyn(read_req, req->UserData(), OnRead))
    {
        LOG(ERROR) << "Send read request to EloqStore failed for table:"
                   << table_id;
        if (!worker_terminated)
        {
            // Terminate this worker.
            worker_terminated = true;
            bm_res.Nofity(false);
        }
        return;
    }

    // Update the request count that this worker sent
    ++total_read_req_cnt;
}
// workers to start load requests.
void Benchmark::LoadWorker(size_t id, const Benchmark &bm)
{
    // @@id is used to find the load operation.
    size_t part_per_req = bm.partition_count_ / bm.concurr_req_count_;

    assert(id <= bm.load_ops_.size());
    auto &load_op = bm.load_ops_[id];

    uint32_t part_id = load_op.part_ids_[load_op.part_idx_];
    // object generator.
    object_generator &obj_gen = bm.load_obj_gens_[part_id];
    // table name
    ::eloqstore::TableIdent eloq_store_table_id(table_name_str, part_id);
    // construct a batch value
    batch_records entries;
    // The first batch
    const uint64_t data_cnt_per_part = load_op.target_data_cnt_;

    uint64_t batch_cnt_max = (bm.batch_byte_size_ << 20) / bm.value_byte_size_;
    size_t batch_cnt =
        data_cnt_per_part <= batch_cnt_max ? data_cnt_per_part : batch_cnt_max;
    GenBatchRecord(bm, obj_gen, entries, batch_cnt);

    // construct eloq store write request
    load_op.batch_cnt_ = batch_cnt;
    auto &kv_write_req = load_op.req_;
    assert(kv_write_req != nullptr);
    uint64_t user_data =
        reinterpret_cast<uint64_t>(static_cast<const void *>(&load_op));
    kv_write_req->SetArgs(eloq_store_table_id, std::move(entries));
    // send to eloq store.
    if (!bm.eloq_store_->ExecAsyn(kv_write_req.get(), user_data, OnBatchWrite))
    {
        LOG(ERROR) << "Send write request to EloqStore failed for "
                      "partition#"
                   << part_id;
        return;
    }
}

Benchmark::Benchmark(std::string &command,
                     uint64_t batch_byte_size,
                     size_t total_data_size,
                     uint32_t partition_count,
                     uint32_t key_byte_size,
                     uint32_t value_byte_size,
                     std::string &key_prefix,
                     std::string &key_pattern,
                     uint32_t key_minimum,
                     uint32_t key_maximum,
                     uint32_t concurr_req_count,
                     uint32_t total_test_time_sec,
                     uint32_t worker_cnt)
    : batch_byte_size_(batch_byte_size),
      command_(command),
      total_data_size_(total_data_size),
      partition_count_(partition_count),
      key_byte_size_(key_byte_size),
      value_byte_size_(value_byte_size),
      key_prefix_(key_prefix),
      key_minimum_(key_minimum),
      key_maximum_(key_maximum),
      concurr_req_count_(concurr_req_count),
      total_test_time_sec_(total_test_time_sec),
      key_pattern_(key_pattern),
      result_(worker_cnt, this)
{
}

bool Benchmark::OpenEloqStore(const eloqstore::KvOptions &kv_options)
{
    eloq_store_ = std::make_unique<::eloqstore::EloqStore>(kv_options);

    ::eloqstore::KvError res = eloq_store_->Start();
    if (res != ::eloqstore::KvError::NoError)
    {
        LOG(ERROR) << "EloqStore start failed with error code: "
                   << static_cast<uint32_t>(res);
        return false;
    }

    return true;
}

void Benchmark::CloseEloqStore()
{
    if (eloq_store_ != nullptr)
    {
        eloq_store_->Stop();
    }
}

void Benchmark::RunBenchmark()
{
    LOG(INFO) << "Begin run EloqStore benchmark.";
    std::chrono::system_clock::time_point start_tp =
        std::chrono::high_resolution_clock::now();

    start_ts_ = std::chrono::duration_cast<std::chrono::microseconds>(
                    start_tp.time_since_epoch())
                    .count();

    uint32_t seed_num = start_ts_ & 0xFFFFFFFF;

    if (command_ == "LOAD")
    {
        // load the amount data into all the partition mean
        uint64_t data_cnt_per_part = total_data_size_ / partition_count_;
        DLOG(INFO) << "Load data count per partition: " << data_cnt_per_part;
        // request count -> partition count;
        // 8 -> 1024
        // 128 partion / per request
        // req id 0 : 0, 8, 16, 24,...
        // req id 1 : 1, 9, 17, 25,...
        // ...

        load_obj_gens_.resize(partition_count_);
        for (size_t part_id = 0; part_id < partition_count_; ++part_id)
        {
            object_generator &obj_gen = load_obj_gens_[part_id];
            obj_gen.set_random_data(true);
            obj_gen.set_random_seed(seed_num + part_id);
            obj_gen.set_key_size(key_byte_size_);
            obj_gen.set_data_size_fixed(value_byte_size_);
            obj_gen.set_key_prefix(key_prefix_.data());
            obj_gen.set_key_range(key_minimum_, key_maximum_);
        }

        std::vector<std::thread> client_thds;
        client_thds.reserve(concurr_req_count_);
        load_ops_.reserve(concurr_req_count_);

        // update the start ts
        start_tp = std::chrono::high_resolution_clock::now();
        start_ts_ = std::chrono::duration_cast<std::chrono::microseconds>(
                        start_tp.time_since_epoch())
                        .count();
        for (size_t req_id = 0; req_id < concurr_req_count_; ++req_id)
        {
            load_ops_.emplace_back(req_id, data_cnt_per_part, this);
            client_thds.push_back(
                std::thread([req_id, this]() { LoadWorker(req_id, *this); }));
        }

        for (size_t i = 0; i < client_thds.size(); ++i)
        {
            client_thds[i].join();
        }
    }
    else if (command_ == "GET")
    {
        read_obj_gen.set_random_data(true);
        read_obj_gen.set_random_seed(seed_num + partition_count_);
        read_obj_gen.set_key_size(key_byte_size_);
        read_obj_gen.set_data_size_fixed(value_byte_size_);
        read_obj_gen.set_key_prefix(key_prefix_.data());
        read_obj_gen.set_key_range(key_minimum_, key_maximum_);

        read_ops_.reserve(concurr_req_count_);
        int8_t iter = obj_iter_type(key_pattern_, GET_CMD_IDX);
        for (size_t req_id = 0; req_id < concurr_req_count_; ++req_id)
        {
            // For init, use the partition id from the 1st partition.
            uint32_t part_id = req_id >= partition_count_
                                   ? (req_id % partition_count_)
                                   : req_id;
            // table name
            ::eloqstore::TableIdent eloq_store_table_id(table_name_str,
                                                        part_id);

            auto &read_op = read_ops_.emplace_back(this);

            uint64_t key_index = read_obj_gen.get_key_index(iter);
            read_obj_gen.generate_key(key_index, read_op.key_);

            // The eloq store read request
            auto &kv_read_req = read_op.req_;
            assert(kv_read_req != nullptr);
            kv_read_req->SetArgs(eloq_store_table_id, read_op.key_);
            // Send to eloq store.
            uint64_t data = reinterpret_cast<uint64_t>(&read_op);
            if (!eloq_store_->ExecAsyn(kv_read_req.get(), data, OnRead))
            {
                LOG(ERROR) << "Send read request to EloqStore failed for "
                              "partition#"
                           << part_id << " key: " << read_op.key_;
                return;
            }
        }
    }
    else
    {
        LOG(ERROR) << "Unsupport command: " << command_;
        return;
    }

    // Wait the result.
    result_.Wait(start_ts_);

    // Report
    auto end_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    result_.ReportResult(end_ts);

    LOG(INFO) << "EloqStore benchmark " << command_ << " finished.";
}

}  // namespace EloqStoreBM
