#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct NdsReadStats {
    uint64_t bytes = 0;
    uint64_t elapsed_us = 0;
    int files = 0;
    int iocbs = 0;
};

// dlopen libnds, nds_init, and nds_io_new_ctx. One context per process.
bool InitNdsDiskIo();

// Read bucket bytes straight into device memory. Does not use TransferEngine.
tl::expected<NdsReadStats, ErrorCode> ReadDiskToDeviceWithNds(
    const std::vector<std::string>& paths,
    const std::vector<uint64_t>& offsets,
    const std::vector<std::vector<Slice>>& slices);

}  // namespace mooncake
