/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#ifdef ELOQSTORE_WITH_TXSERVICE
#include <cstddef>
#include <memory>
#include <string>

#include "meter.h"
#include "metrics.h"

namespace metrics
{
inline const Name NAME_ELOQSTORE_WORK_ONE_ROUND_DURATION{
    "eloqstore_work_one_round_duration"};
inline const Name NAME_ELOQSTORE_TASK_MANAGER_ACTIVE_TASKS{
    "eloqstore_task_manager_active_tasks"};
inline const Name NAME_ELOQSTORE_REQUEST_LATENCY{"eloqstore_request_latency"};
inline const Name NAME_ELOQSTORE_REQUESTS_COMPLETED{
    "eloqstore_requests_completed"};
inline const Name NAME_ELOQSTORE_INDEX_BUFFER_POOL_USED{
    "eloqstore_index_buffer_pool_used"};
inline const Name NAME_ELOQSTORE_INDEX_BUFFER_POOL_LIMIT{
    "eloqstore_index_buffer_pool_limit"};
inline const Name NAME_ELOQSTORE_OPEN_FILE_COUNT{"eloqstore_open_file_count"};
inline const Name NAME_ELOQSTORE_OPEN_FILE_LIMIT{"eloqstore_open_file_limit"};
inline const Name NAME_ELOQSTORE_LOCAL_SPACE_USED{"eloqstore_local_space_used"};
inline const Name NAME_ELOQSTORE_LOCAL_SPACE_LIMIT{
    "eloqstore_local_space_limit"};

// Collection interval for Phase 9-11 gauge metrics (index buffer pool, page
// pool, open file, local space) These metrics are collected every N
// WorkOneRound() calls to reduce overhead
inline constexpr size_t ELOQSTORE_GAUGE_COLLECTION_INTERVAL = 1000;
}  // namespace metrics
#endif  // ELOQSTORE_WITH_TXSERVICE
