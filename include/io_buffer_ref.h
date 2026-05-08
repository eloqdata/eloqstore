#pragma once

#include <cstdint>

namespace eloqstore
{

/**
 * @brief A reference to a chunk within an IO uring registered buffer.
 */
struct IoBufferRef
{
    char *data_;
    uint16_t buf_index_;
};

}  // namespace eloqstore