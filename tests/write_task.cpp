#include "tasks/write_task.h"

#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <string>
#include <vector>

namespace
{
class TestKvTask : public eloqstore::KvTask
{
public:
    eloqstore::TaskType Type() const override
    {
        return eloqstore::TaskType::BatchWrite;
    }
};

class TestWriteTask : public eloqstore::WriteTask
{
public:
    eloqstore::TaskType Type() const override
    {
        return eloqstore::TaskType::BatchWrite;
    }

    void SetTable(eloqstore::TableIdent tbl_id)
    {
        tbl_ident_ = std::move(tbl_id);
    }

    eloqstore::ObjectStore::UploadTask *PendingUpload(size_t idx)
    {
        return pending_upload_tasks_.at(idx).get();
    }

    size_t PendingUploadCount() const
    {
        return pending_upload_tasks_.size();
    }

    uint32_t InflightUploads() const
    {
        return inflight_upload_tasks_;
    }

    size_t UploadStateBufferCapacity() const
    {
        return upload_state_.buffer.capacity();
    }

    size_t PoolSize() const
    {
        return pool_.size();
    }

    size_t AcquireCount() const
    {
        return acquire_count_;
    }

    size_t ReleaseCount() const
    {
        return release_count_;
    }

    bool AllocatedNewBuffer() const
    {
        return allocated_new_buffer_;
    }

    void SeedPoolBuffer(size_t bytes)
    {
        eloqstore::DirectIoBuffer buffer;
        buffer.resize(bytes);
        buffer.clear();
        pool_.push_back(std::move(buffer));
    }

    void BorrowUploadStateBuffer()
    {
        EnsureUploadStateBuffer();
    }

    eloqstore::KvError DrainPendingUploads()
    {
        return WaitPendingUploads();
    }

protected:
    eloqstore::DirectIoBuffer AcquireUploadStateBuffer() override
    {
        acquire_count_++;
        if (pool_.empty())
        {
            allocated_new_buffer_ = true;
            return {};
        }
        eloqstore::DirectIoBuffer buffer = std::move(pool_.back());
        pool_.pop_back();
        return buffer;
    }

    void ReleaseUploadStateBuffer(eloqstore::DirectIoBuffer buffer) override
    {
        release_count_++;
        if (buffer.capacity() != 0)
        {
            buffer.clear();
            pool_.push_back(std::move(buffer));
        }
    }

private:
    std::vector<eloqstore::DirectIoBuffer> pool_;
    size_t acquire_count_{0};
    size_t release_count_{0};
    bool allocated_new_buffer_{false};
};
}  // namespace

TEST_CASE("write task reset returns upload buffer to pool",
          "[write_task][cloud]")
{
    TestWriteTask task;
    task.SetTable({"upload-buffer-reset", 0});
    task.SeedPoolBuffer(64 * 1024);

    task.BorrowUploadStateBuffer();

    REQUIRE(task.UploadStateBufferCapacity() >= 64 * 1024);
    REQUIRE(task.PoolSize() == 0);

    task.ResetUploadState();

    REQUIRE(task.UploadStateBufferCapacity() == 0);
    REQUIRE(task.PoolSize() == 1);
    REQUIRE(task.AcquireCount() == 1);
    REQUIRE(task.ReleaseCount() == 1);
    REQUIRE_FALSE(task.AllocatedNewBuffer());
}

TEST_CASE("write task releases completed upload buffers before commit",
          "[write_task][cloud]")
{
    TestWriteTask task;
    task.SetTable({"upload-buffer-release", 0});
    task.SeedPoolBuffer(64 * 1024);
    task.BorrowUploadStateBuffer();

    auto upload_task = std::make_unique<eloqstore::ObjectStore::UploadTask>(
        &task.TableId(), "data_1_1");
    upload_task->owner_write_task_ = &task;
    upload_task->error_ = eloqstore::KvError::CloudErr;
    upload_task->data_buffer_ = std::move(task.MutableUploadState().buffer);

    REQUIRE(upload_task->data_buffer_.capacity() >= 64 * 1024);
    REQUIRE(task.PoolSize() == 0);

    task.AddInflightUploadTask();
    auto *raw_task = upload_task.get();
    task.AddPendingUploadTask(std::move(upload_task));

    task.CompletePendingUploadTask(raw_task);

    REQUIRE(task.InflightUploads() == 0);
    REQUIRE(raw_task->data_buffer_.size() == 0);
    REQUIRE(raw_task->data_buffer_.capacity() == 0);
    REQUIRE(task.PoolSize() == 1);
    REQUIRE(task.DrainPendingUploads() == eloqstore::KvError::CloudErr);
    REQUIRE(task.PendingUploadCount() == 0);
    REQUIRE_FALSE(task.AllocatedNewBuffer());
}

TEST_CASE("object store task completion finishes kv task io",
          "[write_task][cloud]")
{
    TestKvTask kv_task;
    kv_task.inflight_io_ = 1;

    eloqstore::ObjectStore::DeleteTask task("remote/path");
    task.SetKvTask(&kv_task);

    task.CompleteCloudTask();

    REQUIRE(kv_task.inflight_io_ == 0);
}

TEST_CASE("object store upload completion delegates to owner write task",
          "[write_task][cloud]")
{
    TestWriteTask owner;
    owner.SetTable({"upload-complete-hook", 0});
    owner.AddInflightUploadTask();

    eloqstore::ObjectStore::UploadTask upload_task(&owner.TableId(),
                                                   "data_1_1");
    upload_task.owner_write_task_ = &owner;
    upload_task.SetKvTask(&owner);

    upload_task.CompleteCloudTask();

    REQUIRE(owner.InflightUploads() == 0);
    REQUIRE(owner.inflight_io_ == 0);
    REQUIRE(owner.ReleaseCount() == 1);
}
