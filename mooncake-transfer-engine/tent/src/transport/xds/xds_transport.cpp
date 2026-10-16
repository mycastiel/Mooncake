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

#include "tent/transport/xds/xds_transport.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include <cstdio>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <utility>

extern "C" {
#include "nds_api.h"
}

#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kSectorSize = 512;
// nds_io_vec::buf_len rejects lengths above 2GiB. Larger ranges stay on the
// same iocb and continue in the next iov.
constexpr uint32_t kMaxIovBytes = 2u << 30;

bool sectorAligned(uint64_t value) { return (value & (kSectorSize - 1)) == 0; }

struct IoFrag {
    int fd = -1;
    uint32_t opcode = 0;
    bool registered = false;
    uint64_t reg_base = 0;
    uint64_t reg_length = 0;
    uint64_t file_off = 0;
    uint64_t buf = 0;
    uint32_t len = 0;
    size_t task_index = 0;
};

struct IoGroup {
    int fd = -1;
    uint32_t opcode = 0;
    bool registered = false;
    uint64_t reg_base = 0;
    uint64_t reg_length = 0;
    uint64_t file_off = 0;
    uint64_t file_end = 0;
    std::vector<nds_io_vec> iovs;
    std::vector<XdsIocb::Part> parts;
};

}  // namespace

class XdsFileContext {
   public:
    explicit XdsFileContext(const std::string& path) : ready_(false), fd_(-1) {
        fd_ = open(path.c_str(), O_RDWR | O_DIRECT);
        if (fd_ < 0) {
            PLOG(ERROR) << "Failed to open file " << path;
            return;
        }
        int32_t fds[1] = {fd_};
        nds_fs_desc desc{};
        desc.fs_fd = fds;
        desc.fs_fd_cnt = 1;
        int rc = nds_register_fs(&desc);
        if (rc < 0) {
            LOG(ERROR) << "nds_register_fs failed for " << path
                       << ", rc=" << rc;
            fprintf(stderr, "NDS scan failed: %s rc=%d\n", path.c_str(), rc);
            close(fd_);
            fd_ = -1;
            return;
        }
        ready_ = true;
        LOG(INFO) << "NDS loaded: scanned disk " << path
                  << " and registered topology, fd=" << fd_;
        printf("NDS loaded: scanned disk %s and registered topology, fd=%d\n",
               path.c_str(), fd_);
        fflush(stdout);
    }

    XdsFileContext(const XdsFileContext&) = delete;
    XdsFileContext& operator=(const XdsFileContext&) = delete;

    ~XdsFileContext() {
        if (fd_ >= 0) {
            int32_t fds[1] = {fd_};
            nds_fs_desc desc{};
            desc.fs_fd = fds;
            desc.fs_fd_cnt = 1;
            nds_unregister_fs(&desc);
            close(fd_);
        }
    }

    int fd() const { return fd_; }

    bool ready() const { return ready_; }

   private:
    bool ready_;
    int fd_;
};

XdsTransport::XdsTransport() = default;

XdsTransport::~XdsTransport() { uninstall(); }

Status XdsTransport::install(std::string& local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "XDS transport has been installed" LOC_MARK);
    }

    nds_init_param param{};
    int rc = nds_init(&param);
    if (rc < 0) {
        return Status::InternalError(
            std::string("nds_init failed: ") + std::to_string(rc) + LOC_MARK);
    }
    nds_ready_ = true;
    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    conf_ = conf;
    io_batch_depth_ = conf_ ? conf_->get("transports/xds/io_batch_depth", 32)
                            : 32;
    if (io_batch_depth_ == 0 || io_batch_depth_ > NDS_IO_MAX_IO_CNT)
        io_batch_depth_ = 32;
    caps.npu_to_file = true;
    installed_ = true;
    LOG(INFO) << "NDS loaded: XDS transport installed, nds_init ok, "
                 "io_batch_depth="
              << io_batch_depth_;
    printf("NDS loaded: XDS transport installed, nds_init ok, io_batch_depth=%zu\n",
           io_batch_depth_);
    fflush(stdout);
    return Status::OK();
}

Status XdsTransport::uninstall() {
    if (!installed_) return Status::OK();

    {
        std::lock_guard<std::mutex> lock(allocated_batches_lock_);
        for (auto* batch : allocated_batches_) {
            if (batch->ctx) nds_io_destroy_ctx(batch->ctx);
            delete batch;
        }
        allocated_batches_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(file_context_lock_);
        file_context_map_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(mem_lock_);
        for (const auto& range : registered_) {
            nds_unregister_mem(reinterpret_cast<void*>(range.addr),
                               range.length, 0);
        }
        registered_.clear();
    }
    if (nds_ready_) {
        nds_exit();
        nds_ready_ = false;
    }
    metadata_.reset();
    installed_ = false;
    return Status::OK();
}

Status XdsTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    if (!installed_)
        return Status::InvalidArgument("XDS transport is not installed" LOC_MARK);

    auto* xds_batch = new XdsSubBatch();
    nds_io_ctx_param param{};
    param.max_io_cnt = static_cast<uint32_t>(
        std::min(io_batch_depth_, static_cast<size_t>(NDS_IO_MAX_IO_CNT)));
    int rc = nds_io_new_ctx(&param, &xds_batch->ctx);
    if (rc < 0) {
        delete xds_batch;
        return Status::InternalError(
            std::string("nds_io_new_ctx failed: ") + std::to_string(rc) +
            LOC_MARK);
    }
    xds_batch->max_size = max_size;
    {
        std::lock_guard<std::mutex> lock(allocated_batches_lock_);
        allocated_batches_.push_back(xds_batch);
    }
    batch = xds_batch;
    return Status::OK();
}

Status XdsTransport::freeSubBatch(SubBatchRef& batch) {
    auto* xds_batch = dynamic_cast<XdsSubBatch*>(batch);
    if (!xds_batch)
        return Status::InvalidArgument("Invalid XDS sub-batch" LOC_MARK);

    {
        std::lock_guard<std::mutex> lock(allocated_batches_lock_);
        auto it = std::find(allocated_batches_.begin(),
                            allocated_batches_.end(), xds_batch);
        if (it != allocated_batches_.end()) allocated_batches_.erase(it);
    }
    {
        std::lock_guard<std::mutex> lock(xds_batch->status_mutex);
        if (xds_batch->ctx) {
            nds_io_destroy_ctx(xds_batch->ctx);
            xds_batch->ctx = nullptr;
        }
    }
    delete xds_batch;
    batch = nullptr;
    return Status::OK();
}

std::string XdsTransport::getXdsFilePath(SegmentID target_id) {
    std::string ret;
    auto status = metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) {
            if (segment->type != SegmentType::File)
                return Status::NeedsRefreshCache(
                    "Segment type is not File" LOC_MARK);
            auto& detail = std::get<FileSegmentDesc>(segment->detail);
            if (detail.buffers.empty())
                return Status::NeedsRefreshCache("No buffers found" LOC_MARK);
            ret = detail.buffers[0].path;
            return Status::OK();
        });
    if (!status.ok()) return "";
    return ret;
}

XdsFileContext* XdsTransport::findFileContext(SegmentID target_id) {
    std::lock_guard<std::mutex> guard(file_context_lock_);
    auto it = file_context_map_.find(target_id);
    if (it != file_context_map_.end()) return it->second.get();

    std::string path = getXdsFilePath(target_id);
    if (path.empty()) return nullptr;
    auto context = std::make_shared<XdsFileContext>(path);
    if (!context->ready()) return nullptr;
    auto* raw = context.get();
    file_context_map_.emplace(target_id, std::move(context));
    return raw;
}

bool XdsTransport::coversRegistered(uint64_t addr, size_t length) const {
    uint64_t base = 0;
    uint64_t range_length = 0;
    return lookupRegistered(addr, length, &base, &range_length);
}

bool XdsTransport::lookupRegistered(uint64_t addr, size_t length,
                                    uint64_t* base,
                                    uint64_t* range_length) const {
    std::lock_guard<std::mutex> lock(mem_lock_);
    uint64_t end = addr + length;
    for (const auto& range : registered_) {
        if (addr >= range.addr && end <= range.addr + range.length) {
            *base = range.addr;
            *range_length = range.length;
            return true;
        }
    }
    return false;
}

Status XdsTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* xds_batch = dynamic_cast<XdsSubBatch*>(batch);
    if (!xds_batch)
        return Status::InvalidArgument("Invalid XDS sub-batch" LOC_MARK);

    std::lock_guard<std::mutex> lock(xds_batch->status_mutex);
    std::vector<IoFrag> frags;
    frags.reserve(request_list.size());
    for (size_t req_index = 0; req_index < request_list.size(); ++req_index) {
        const auto& request = request_list[req_index];
        if (request.length == 0 || !sectorAligned(request.target_offset) ||
            !sectorAligned(request.length) ||
            !sectorAligned(reinterpret_cast<uint64_t>(request.source))) {
            return Status::InvalidArgument(
                "XDS requires 512-byte aligned address, offset, and length" LOC_MARK);
        }
        XdsFileContext* context = findFileContext(request.target_id);
        if (!context || !context->ready())
            return Status::InvalidArgument("Invalid remote segment" LOC_MARK);

        uint64_t reg_base = 0;
        uint64_t reg_length = 0;
        bool registered = lookupRegistered(
            reinterpret_cast<uint64_t>(request.source), request.length,
            &reg_base, &reg_length);
        uint32_t opcode = request.opcode == Request::READ ? NDS_IO_OP_PREAD
                                                          : NDS_IO_OP_PWRITE;
        for (size_t offset = 0; offset < request.length;) {
            size_t length = std::min(static_cast<size_t>(kMaxIovBytes),
                                     request.length - offset);
            IoFrag frag;
            frag.fd = context->fd();
            frag.opcode = opcode;
            frag.registered = registered;
            frag.reg_base = reg_base;
            frag.reg_length = reg_length;
            frag.file_off = request.target_offset + offset;
            frag.buf = reinterpret_cast<uint64_t>(request.source) + offset;
            frag.len = static_cast<uint32_t>(length);
            frag.task_index = req_index;
            frags.push_back(frag);
            offset += length;
        }
    }

    // Same fd, opcode, and registered HBM window, with a contiguous file
    // range, share one iocb. Each piece is an iov. A different window cannot
    // share the iocb: NDS pins every iov against the registration of iov[0].
    std::vector<IoGroup> groups;
    for (const auto& frag : frags) {
        IoGroup* match = nullptr;
        for (auto& group : groups) {
            if (group.fd == frag.fd && group.opcode == frag.opcode &&
                group.registered == frag.registered &&
                group.reg_base == frag.reg_base &&
                group.reg_length == frag.reg_length &&
                group.file_end == frag.file_off &&
                group.iovs.size() < NDS_IO_MAX_IOV) {
                match = &group;
                break;
            }
        }
        if (!match) {
            groups.emplace_back();
            match = &groups.back();
            match->fd = frag.fd;
            match->opcode = frag.opcode;
            match->registered = frag.registered;
            match->reg_base = frag.reg_base;
            match->reg_length = frag.reg_length;
            match->file_off = frag.file_off;
            match->file_end = frag.file_off;
        }
        nds_io_vec iov{};
        iov.buf_addr = frag.buf;
        iov.buf_len = frag.len;
        match->iovs.push_back(iov);
        match->file_end += frag.len;
        if (match->parts.empty() ||
            match->parts.back().task_index != frag.task_index) {
            match->parts.push_back(
                XdsIocb::Part{frag.task_index, frag.len});
        } else {
            match->parts.back().bytes += frag.len;
        }
    }
    if (groups.empty()) return Status::OK();
    if (xds_batch->submitted_iocbs + groups.size() > io_batch_depth_)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    const size_t task_base = xds_batch->tasks.size();
    xds_batch->tasks.resize(task_base + request_list.size());
    const size_t iocb_base = xds_batch->iocbs.size();
    xds_batch->iocbs.resize(iocb_base + groups.size());
    for (size_t i = 0; i < groups.size(); ++i) {
        auto& stored = xds_batch->iocbs[iocb_base + i];
        stored.parts = std::move(groups[i].parts);
        for (auto& part : stored.parts) {
            part.task_index += task_base;
            xds_batch->tasks[part.task_index].iocb_count++;
        }
    }

    std::vector<nds_io_vec> iovs;
    size_t iov_total = 0;
    for (const auto& group : groups) iov_total += group.iovs.size();
    iovs.reserve(iov_total);
    std::vector<size_t> iov_at(groups.size());
    for (size_t i = 0; i < groups.size(); ++i) {
        iov_at[i] = iovs.size();
        iovs.insert(iovs.end(), groups[i].iovs.begin(), groups[i].iovs.end());
    }
    std::vector<nds_io_cb> cbs(groups.size());
    uint64_t submit_bytes = 0;
    for (size_t i = 0; i < groups.size(); ++i) {
        auto& cb = cbs[i];
        cb.opcode = groups[i].opcode;
        cb.rw_flags = groups[i].registered ? NDS_IO_F_REGISTERED_MEM : 0;
        cb.obj.fd = groups[i].fd;
        cb.offset = groups[i].file_off;
        cb.user_data = iocb_base + i + 1;
        cb.iov = iovs.data() + iov_at[i];
        cb.iov_cnt = static_cast<uint32_t>(groups[i].iovs.size());
        cb.host_pid = 0;
        submit_bytes += groups[i].file_end - groups[i].file_off;
    }

    int accepted = nds_io_submit(xds_batch->ctx, static_cast<int>(cbs.size()),
                                 cbs.data());
    const char* op_name =
        cbs.empty() ? "none"
                    : (cbs[0].opcode == NDS_IO_OP_PREAD ? "pread" : "pwrite");
    printf("NDS data path: nds_io_submit fd=%d opcode=%s iocb=%zu iov=%zu "
           "accepted=%d bytes=%llu registered_mem=%d\n",
           cbs.empty() ? -1 : cbs[0].obj.fd, op_name, cbs.size(), iov_total,
           accepted, (unsigned long long)submit_bytes,
           cbs.empty() ? 0
                       : ((cbs[0].rw_flags & NDS_IO_F_REGISTERED_MEM) != 0));
    fflush(stdout);
    if (accepted < 0) {
        xds_batch->tasks.resize(task_base);
        xds_batch->iocbs.resize(iocb_base);
        return Status::InternalError(
            std::string("nds_io_submit failed: ") + std::to_string(accepted) +
            LOC_MARK);
    }
    xds_batch->submitted_iocbs += static_cast<size_t>(accepted);
    if (static_cast<size_t>(accepted) == cbs.size()) return Status::OK();

    for (size_t i = static_cast<size_t>(accepted); i < groups.size(); ++i) {
        auto& iocb = xds_batch->iocbs[iocb_base + i];
        iocb.done = true;
        for (const auto& part : iocb.parts) {
            auto& task = xds_batch->tasks[part.task_index];
            task.iocbs_done++;
            if (task.error == 0) task.error = -EIO;
            if (task.iocb_count > 0 && task.iocbs_done == task.iocb_count)
                task.status = TransferStatusEnum::FAILED;
        }
    }
    return Status::InternalError(
        "nds_io_submit accepted a short batch" LOC_MARK);
}

void XdsTransport::reap(XdsSubBatch* batch) {
    if (!batch->ctx) return;
    nds_io_event events[32];
    timespec timeout{};
    for (;;) {
        int n = nds_io_getevents(batch->ctx, 0, 32, events, &timeout);
        if (n <= 0) return;
        for (int i = 0; i < n; i++) {
            uint64_t cookie = events[i].user_data;
            if (cookie == 0 || cookie > batch->iocbs.size()) continue;
            auto& iocb = batch->iocbs[static_cast<size_t>(cookie - 1)];
            if (iocb.done) continue;
            iocb.done = true;
            if (batch->submitted_iocbs > 0) batch->submitted_iocbs--;
            for (const auto& part : iocb.parts) {
                if (part.task_index >= batch->tasks.size()) continue;
                auto& task = batch->tasks[part.task_index];
                if (task.iocbs_done >= task.iocb_count) continue;
                task.iocbs_done++;
                if (events[i].res == 0) {
                    task.transferred_bytes += part.bytes;
                } else if (task.error == 0) {
                    task.error = static_cast<int32_t>(events[i].res);
                }
                if (task.iocbs_done == task.iocb_count) {
                    task.status = task.error == 0
                                      ? TransferStatusEnum::COMPLETED
                                      : TransferStatusEnum::FAILED;
                }
            }
        }
    }
}

Status XdsTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus& status) {
    auto* xds_batch = dynamic_cast<XdsSubBatch*>(batch);
    if (!xds_batch)
        return Status::InvalidArgument("Invalid XDS sub-batch" LOC_MARK);
    if (task_id < 0 || task_id >= static_cast<int>(xds_batch->tasks.size()))
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);

    std::lock_guard<std::mutex> lock(xds_batch->status_mutex);
    auto& task = xds_batch->tasks[task_id];
    if (task.status == TransferStatusEnum::PENDING) reap(xds_batch);
    status = TransferStatus{task.status, task.transferred_bytes};
    return Status::OK();
}

Status XdsTransport::addMemoryBuffer(BufferDesc& desc,
                                     const MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() != "npu" && !options.location.starts_with("npu"))
        return Status::OK();
    int rc = nds_register_mem(reinterpret_cast<void*>(desc.addr), desc.length,
                              0);
    if (rc < 0) {
        fprintf(stderr,
                "NDS register NPU memory failed: addr=%p size=%zu rc=%d\n",
                reinterpret_cast<void*>(desc.addr), desc.length, rc);
        return Status::InternalError(
            std::string("nds_register_mem failed: ") + std::to_string(rc) +
            LOC_MARK);
    }
    printf("NDS loaded: registered NPU memory location=%s addr=%p size=%zu\n",
           options.location.c_str(), reinterpret_cast<void*>(desc.addr),
           desc.length);
    fflush(stdout);
    {
        std::lock_guard<std::mutex> lock(mem_lock_);
        registered_.push_back(
            RegisteredRange{static_cast<uint64_t>(desc.addr), desc.length});
    }
    desc.transports.push_back(XDS);
    return Status::OK();
}

Status XdsTransport::removeMemoryBuffer(BufferDesc& desc) {
    LocationParser location(desc.location);
    if (location.type() != "npu" && !desc.location.starts_with("npu"))
        return Status::OK();
    int rc = nds_unregister_mem(reinterpret_cast<void*>(desc.addr), desc.length,
                                0);
    if (rc < 0) {
        return Status::InternalError(
            std::string("nds_unregister_mem failed: ") + std::to_string(rc) +
            LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mem_lock_);
    uint64_t addr = static_cast<uint64_t>(desc.addr);
    registered_.erase(std::remove_if(registered_.begin(), registered_.end(),
                                     [&](const RegisteredRange& range) {
                                         return range.addr == addr &&
                                                range.length == desc.length;
                                     }),
                      registered_.end());
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
