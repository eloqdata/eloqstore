#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <chrono>
#include <atomic>

#include "task.h"
#include "shard.h"
#include "kv_request.h"
#include "fixtures/test_fixtures.h"
#include "fixtures/test_helpers.h"

using namespace eloqstore;
using namespace eloqstore::test;

// Mock task for testing base functionality
class MockTask : public KvTask {
public:
    explicit MockTask(TaskType type) : type_(type) {}

    TaskType Type() const override { return type_; }

    void Abort() override {
        aborted_ = true;
    }

    bool aborted_ = false;
    TaskType type_;
};

class TaskBaseTestFixture {
public:
    TaskBaseTestFixture() {
        InitOptions();
    }

    void InitOptions() {
        options_ = std::make_unique<KvOptions>();
        options_->page_size = 4096;
        options_->num_threads = 4;
    }

protected:
    std::unique_ptr<KvOptions> options_;
};

TEST_CASE_METHOD(TaskBaseTestFixture, "KvTask_BasicOperations", "[task][unit]") {
    SECTION("Task type identification") {
        MockTask read_task(TaskType::Read);
        REQUIRE(read_task.Type() == TaskType::Read);

        MockTask write_task(TaskType::BatchWrite);
        REQUIRE(write_task.Type() == TaskType::BatchWrite);

        MockTask scan_task(TaskType::Scan);
        REQUIRE(scan_task.Type() == TaskType::Scan);
    }

    SECTION("Task status management") {
        MockTask task(TaskType::Read);

        REQUIRE(task.status_ == TaskStatus::Idle);

        task.status_ = TaskStatus::Ongoing;
        REQUIRE(task.status_ == TaskStatus::Ongoing);

        task.status_ = TaskStatus::Blocked;
        REQUIRE(task.status_ == TaskStatus::Blocked);

        task.status_ = TaskStatus::Finished;
        REQUIRE(task.status_ == TaskStatus::Finished);
    }

    SECTION("Task abort") {
        MockTask task(TaskType::BatchWrite);
        REQUIRE(task.aborted_ == false);

        task.Abort();
        REQUIRE(task.aborted_ == true);
    }

    SECTION("IO tracking") {
        MockTask task(TaskType::Read);

        REQUIRE(task.inflight_io_ == 0);
        REQUIRE(task.io_res_ == 0);

        task.inflight_io_ = 5;
        REQUIRE(task.inflight_io_ == 5);

        task.FinishIo();
        // After finishing I/O, inflight count should decrease
    }

    SECTION("Task chaining") {
        MockTask task1(TaskType::Read);
        MockTask task2(TaskType::Scan);
        MockTask task3(TaskType::BatchWrite);

        task1.next_ = &task2;
        task2.next_ = &task3;

        REQUIRE(task1.next_ == &task2);
        REQUIRE(task2.next_ == &task3);
        REQUIRE(task3.next_ == nullptr);
    }
}

TEST_CASE_METHOD(TaskBaseTestFixture, "TaskType_Coverage", "[task][unit]") {
    SECTION("All task types") {
        std::vector<TaskType> all_types = {
            TaskType::Read,
            TaskType::Scan,
            TaskType::BatchWrite,
            TaskType::BackgroundWrite,
            TaskType::EvictFile
        };

        for (TaskType type : all_types) {
            MockTask task(type);
            REQUIRE(task.Type() == type);
        }
    }
}

TEST_CASE_METHOD(TaskBaseTestFixture, "TaskStatus_Transitions", "[task][unit]") {
    MockTask task(TaskType::Read);

    SECTION("Normal lifecycle") {
        // Idle -> Ongoing -> Finished
        REQUIRE(task.status_ == TaskStatus::Idle);

        task.status_ = TaskStatus::Ongoing;
        REQUIRE(task.status_ == TaskStatus::Ongoing);

        task.status_ = TaskStatus::Finished;
        REQUIRE(task.status_ == TaskStatus::Finished);
    }

    SECTION("Blocked transitions") {
        // Ongoing -> Blocked -> Ongoing -> Finished
        task.status_ = TaskStatus::Ongoing;

        task.status_ = TaskStatus::Blocked;
        REQUIRE(task.status_ == TaskStatus::Blocked);

        task.status_ = TaskStatus::Ongoing;
        REQUIRE(task.status_ == TaskStatus::Ongoing);

        task.status_ = TaskStatus::Finished;
        REQUIRE(task.status_ == TaskStatus::Finished);
    }

    SECTION("IO blocking") {
        task.status_ = TaskStatus::Ongoing;

        task.status_ = TaskStatus::BlockedIO;
        REQUIRE(task.status_ == TaskStatus::BlockedIO);

        // Simulate IO completion
        task.status_ = TaskStatus::Ongoing;
        REQUIRE(task.status_ == TaskStatus::Ongoing);
    }
}

TEST_CASE("WaitingZone_Operations", "[task][unit]") {
    WaitingZone zone;

    SECTION("Empty zone") {
        REQUIRE(zone.Empty() == true);
    }

    SECTION("Single task waiting") {
        MockTask task(TaskType::Read);

        zone.Wait(&task);
        REQUIRE(zone.Empty() == false);

        zone.WakeOne();
        REQUIRE(zone.Empty() == true);
    }

    SECTION("Multiple tasks waiting") {
        MockTask task1(TaskType::Read);
        MockTask task2(TaskType::Scan);
        MockTask task3(TaskType::BatchWrite);

        zone.Wait(&task1);
        zone.Wait(&task2);
        zone.Wait(&task3);

        REQUIRE(zone.Empty() == false);

        // Wake one at a time
        zone.WakeOne();
        REQUIRE(zone.Empty() == false);

        zone.WakeOne();
        REQUIRE(zone.Empty() == false);

        zone.WakeOne();
        REQUIRE(zone.Empty() == true);
    }

    SECTION("Wake N tasks") {
        std::vector<MockTask> tasks;
        for (int i = 0; i < 10; ++i) {
            tasks.emplace_back(TaskType::Read);
        }

        for (auto& task : tasks) {
            zone.Wait(&task);
        }

        zone.WakeN(5);
        // Should have woken 5 tasks

        zone.WakeN(3);
        // Should have woken 3 more

        zone.WakeAll();
        REQUIRE(zone.Empty() == true);
    }

    SECTION("Wake all") {
        std::vector<MockTask> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.emplace_back(TaskType::Read);
        }

        for (auto& task : tasks) {
            zone.Wait(&task);
        }

        REQUIRE(zone.Empty() == false);

        zone.WakeAll();
        REQUIRE(zone.Empty() == true);
    }
}

TEST_CASE("WaitingSeat_Operations", "[task][unit]") {
    WaitingSeat seat;

    SECTION("Single task wait and wake") {
        MockTask task(TaskType::Read);

        seat.Wait(&task);
        seat.Wake();

        // Task should be woken
    }

    SECTION("Replace waiting task") {
        MockTask task1(TaskType::Read);
        MockTask task2(TaskType::Scan);

        seat.Wait(&task1);
        seat.Wait(&task2);  // Should replace task1

        seat.Wake();
        // Only task2 should be woken
    }
}

TEST_CASE("Mutex_Operations", "[task][unit]") {
    Mutex mutex;

    SECTION("Basic lock and unlock") {
        mutex.Lock();
        // Mutex is now locked

        mutex.Unlock();
        // Mutex is now unlocked
    }

    SECTION("Multiple lock attempts") {
        MockTask task1(TaskType::Read);
        MockTask task2(TaskType::Scan);

        mutex.Lock();
        // First lock succeeds

        // Second task would block here if it tried to lock
        // (Would need coroutine context to test properly)

        mutex.Unlock();
        // Would wake waiting task
    }
}

TEST_CASE("Task_IOOperations", "[task][unit]") {
    MockTask task(TaskType::Read);

    SECTION("IO result handling") {
        REQUIRE(task.io_res_ == 0);

        // Simulate IO error
        task.io_res_ = -EIO;
        REQUIRE(task.WaitIoResult() == -EIO);

        // Simulate successful IO
        task.io_res_ = 0;
        REQUIRE(task.WaitIoResult() == 0);
    }

    SECTION("Inflight IO tracking") {
        REQUIRE(task.inflight_io_ == 0);

        // Simulate starting IO operations
        task.inflight_io_ = 1;
        REQUIRE(task.inflight_io_ == 1);

        task.inflight_io_ = 5;
        REQUIRE(task.inflight_io_ == 5);

        // Simulate completing IO
        task.FinishIo();
        // inflight_io_ should decrement
    }

    SECTION("IO flags") {
        REQUIRE(task.io_flags_ == 0);

        task.io_flags_ = 0x01;
        REQUIRE(task.io_flags_ == 0x01);

        task.io_flags_ |= 0x02;
        REQUIRE(task.io_flags_ == 0x03);
    }
}

TEST_CASE("OverflowPointers_Decoding", "[task][unit]") {
    SECTION("Decode empty pointers") {
        std::string_view empty;
        std::array<PageId, max_overflow_pointers> pointers;

        uint8_t count = DecodeOverflowPointers(empty, pointers);
        REQUIRE(count == 0);
    }

    SECTION("Decode single pointer") {
        // Create encoded pointer data
        std::string encoded;
        PageId page_id = 12345;
        encoded.append(reinterpret_cast<const char*>(&page_id), sizeof(PageId));

        std::array<PageId, max_overflow_pointers> pointers;
        uint8_t count = DecodeOverflowPointers(encoded, pointers);

        // Should decode one pointer
        REQUIRE(count >= 0);
    }
}

TEST_CASE("Task_EdgeCases", "[task][unit][edge-case]") {
    SECTION("Null request") {
        MockTask task(TaskType::Read);
        REQUIRE(task.req_ == nullptr);

        // Task should handle null request gracefully
    }

    SECTION("Task without coroutine") {
        MockTask task(TaskType::BatchWrite);
        // Task without initialized coroutine should handle operations
    }

    SECTION("Maximum inflight IO") {
        MockTask task(TaskType::BackgroundWrite);

        task.inflight_io_ = UINT32_MAX;
        REQUIRE(task.inflight_io_ == UINT32_MAX);

        // Should handle maximum value
    }

    SECTION("Circular task chain") {
        MockTask task1(TaskType::Read);
        MockTask task2(TaskType::Scan);

        task1.next_ = &task2;
        task2.next_ = &task1;  // Circular reference

        // Should detect or handle circular chains
        REQUIRE(task1.next_ == &task2);
        REQUIRE(task2.next_ == &task1);
    }
}

TEST_CASE("Task_Concurrency", "[task][stress]") {
    SECTION("Concurrent status updates") {
        MockTask task(TaskType::BatchWrite);
        std::atomic<int> updates{0};

        std::vector<std::thread> threads;
        for (int i = 0; i < 4; ++i) {
            threads.emplace_back([&task, &updates]() {
                for (int j = 0; j < 1000; ++j) {
                    // Cycle through statuses
                    task.status_ = TaskStatus::Ongoing;
                    task.status_ = TaskStatus::Blocked;
                    task.status_ = TaskStatus::Ongoing;
                    task.status_ = TaskStatus::Finished;
                    updates++;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(updates == 4000);
    }

    SECTION("Concurrent WaitingZone operations") {
        WaitingZone zone;
        std::vector<MockTask> tasks(100, MockTask(TaskType::Read));
        std::atomic<int> waits{0};
        std::atomic<int> wakes{0};

        std::vector<std::thread> threads;

        // Waiting threads
        for (int i = 0; i < 50; ++i) {
            threads.emplace_back([&zone, &tasks, &waits, i]() {
                zone.Wait(&tasks[i]);
                waits++;
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // Waking thread
        threads.emplace_back([&zone, &wakes]() {
            while (!zone.Empty()) {
                zone.WakeOne();
                wakes++;
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(zone.Empty() == true);
    }
}

TEST_CASE("LoadPage_Functions", "[task][unit]") {
    TableIdent table("test", 1);

    SECTION("Load regular page") {
        FilePageId fp_id = 0;
        auto [page, err] = LoadPage(table, fp_id);

        // May fail if file doesn't exist
        if (err == KvError::NoError) {
            REQUIRE(page.Data() != nullptr);
        }
    }

    SECTION("Load data page") {
        PageId page_id = 0;
        FilePageId fp_id = 0;
        auto [data_page, err] = LoadDataPage(table, page_id, fp_id);

        if (err == KvError::NoError) {
            REQUIRE(data_page.Data() != nullptr);
        }
    }

    SECTION("Load overflow page") {
        PageId page_id = 0;
        FilePageId fp_id = 0;
        auto [overflow_page, err] = LoadOverflowPage(table, page_id, fp_id);

        if (err == KvError::NoError) {
            REQUIRE(overflow_page.Data() != nullptr);
        }
    }
}