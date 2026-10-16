// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef XDS_TRANSPORT_H_
#define XDS_TRANSPORT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"

struct nds_io_ctx;

namespace mooncake {
namespace tent {

class XdsFileContext;

struct XdsTask {
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    size_t transferred_bytes = 0;
    // Number of iocbs this task belongs to. One contiguous request is one.
    size_t iocb_count = 0;
    size_t iocbs_done = 0;
    int32_t error = 0;
};

// One NDS iocb. Its iov[] is the buffer scatter for a single fd and one
// contiguous file range. Completion is one event for every task in parts.
struct XdsIocb {
    struct Part {
        size_t task_index = 0;
        size_t bytes = 0;
    };
    std::vector<Part> parts;
    bool done = false;
};

struct XdsSubBatch : public Transport::SubBatch {
    size_t max_size = 0;
    size_t submitted_iocbs = 0;
    struct nds_io_ctx* ctx = nullptr;
    std::vector<XdsTask> tasks;
    std::vector<XdsIocb> iocbs;
    std::mutex status_mutex;
    virtual size_t size() const { return tasks.size(); }
};

// Local file backend that submits NPU HBM I/O through libnds. Topology,
// RAID striping, and unwritten zero-fill stay inside XDS.
class XdsTransport : public Transport {
   public:
    XdsTransport();

    ~XdsTransport();

    virtual Status install(std::string& local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef& batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef& batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus& status);

    virtual Status addMemoryBuffer(BufferDesc& desc,
                                   const MemoryOptions& options);

    virtual Status removeMemoryBuffer(BufferDesc& desc);

    virtual const char* getName() const { return "xds"; }

   private:
    std::string getXdsFilePath(SegmentID handle);

    XdsFileContext* findFileContext(SegmentID target_id);

    bool coversRegistered(uint64_t addr, size_t length) const;

    bool lookupRegistered(uint64_t addr, size_t length, uint64_t* base,
                          uint64_t* range_length) const;

    void reap(XdsSubBatch* batch);

    bool installed_ = false;
    bool nds_ready_ = false;
    std::string local_segment_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Config> conf_;

    std::mutex file_context_lock_;
    using FileContextMap =
        std::unordered_map<SegmentID, std::shared_ptr<XdsFileContext>>;
    FileContextMap file_context_map_;
    size_t io_batch_depth_ = 32;

    struct RegisteredRange {
        uint64_t addr = 0;
        uint64_t length = 0;
    };
    mutable std::mutex mem_lock_;
    std::vector<RegisteredRange> registered_;

    std::mutex allocated_batches_lock_;
    std::vector<XdsSubBatch*> allocated_batches_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // XDS_TRANSPORT_H_
