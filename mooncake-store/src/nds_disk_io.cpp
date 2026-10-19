#include "nds_disk_io.h"

#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "/home/qyf/XDS/file_p2p/nds_api.h"

namespace mooncake {
namespace {

struct NdsApi {
    int (*init)(nds_init_param*) = nullptr;
    int (*register_fs)(const nds_fs_desc*) = nullptr;
    int (*io_new_ctx)(const nds_io_ctx_param*, nds_io_ctx**) = nullptr;
    int (*io_destroy_ctx)(nds_io_ctx*) = nullptr;
    int (*io_submit)(nds_io_ctx*, int, const nds_io_cb*) = nullptr;
    int (*io_getevents)(nds_io_ctx*, int, int, nds_io_event*, timespec*) =
        nullptr;
    void* handle = nullptr;
    nds_io_ctx* ctx = nullptr;
    bool ready = false;
};

NdsApi& Api() {
    static NdsApi api;
    return api;
}

std::mutex& NdsMu() {
    static std::mutex mu;
    return mu;
}

bool EnsureNds() {
    auto& api = Api();
    if (api.ready) return true;
    api.handle = dlopen("/home/qyf/XDS/file_p2p/libnds.so", RTLD_NOW | RTLD_LOCAL);
    if (!api.handle) {
        LOG(ERROR) << "NDS dlopen failed: " << dlerror();
        return false;
    }
#define LOAD(field, name)                              \
    api.field = reinterpret_cast<decltype(api.field)>( \
        dlsym(api.handle, name));                      \
    if (!api.field) {                                  \
        LOG(ERROR) << "NDS missing symbol " << name;   \
        return false;                                  \
    }
    LOAD(init, "nds_init");
    LOAD(register_fs, "nds_register_fs");
    LOAD(io_new_ctx, "nds_io_new_ctx");
    LOAD(io_destroy_ctx, "nds_io_destroy_ctx");
    LOAD(io_submit, "nds_io_submit");
    LOAD(io_getevents, "nds_io_getevents");
#undef LOAD
    nds_init_param init_param{};
    int rc = api.init(&init_param);
    if (rc < 0) {
        LOG(ERROR) << "nds_init failed rc=" << rc;
        return false;
    }
    nds_io_ctx_param param{};
    param.max_io_cnt = 256;
    rc = api.io_new_ctx(&param, &api.ctx);
    if (rc < 0 || api.ctx == nullptr) {
        LOG(ERROR) << "nds_io_new_ctx failed rc=" << rc;
        return false;
    }
    api.ready = true;
    LOG(INFO) << "NDS disk read enabled inside store, transfer engine unchanged";
    return true;
}

int OpenFile(const std::string& path, std::unordered_map<std::string, int>& fds) {
    auto it = fds.find(path);
    if (it != fds.end()) return it->second;
    int fd = open(path.c_str(), O_RDWR | O_DIRECT);
    if (fd < 0) {
        PLOG(ERROR) << "NDS open failed " << path;
        return -1;
    }
    int32_t raw = fd;
    nds_fs_desc desc{};
    desc.fs_fd = &raw;
    desc.fs_fd_cnt = 1;
    int rc = Api().register_fs(&desc);
    if (rc < 0) {
        LOG(ERROR) << "nds_register_fs failed path=" << path << " rc=" << rc;
        close(fd);
        return -1;
    }
    LOG(INFO) << "NDS loaded: scanned disk " << path << " fd=" << fd;
    fds.emplace(path, fd);
    return fd;
}

}  // namespace

bool InitNdsDiskIo() {
    std::lock_guard<std::mutex> lock(NdsMu());
    return EnsureNds();
}

tl::expected<NdsReadStats, ErrorCode> ReadDiskToDeviceWithNds(
    const std::vector<std::string>& paths,
    const std::vector<uint64_t>& offsets,
    const std::vector<std::vector<Slice>>& slices) {
    std::lock_guard<std::mutex> lock(NdsMu());
    if (!EnsureNds()) return tl::make_unexpected(ErrorCode::INVALID_READ);
    static std::unordered_map<std::string, int> fds;

    struct Group {
        int fd = -1;
        uint64_t offset = 0;
        std::vector<nds_io_vec> iovs;
    };
    std::vector<Group> groups;
    groups.reserve(paths.size());
    uint64_t nbytes = 0;
    for (size_t i = 0; i < paths.size(); ++i) {
        int fd = OpenFile(paths[i], fds);
        if (fd < 0) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        Group group;
        group.fd = fd;
        group.offset = offsets[i];
        for (const auto& slice : slices[i]) {
            if (slice.size == 0 || slice.ptr == nullptr) continue;
            nds_io_vec iov{};
            iov.buf_addr = reinterpret_cast<uint64_t>(slice.ptr);
            iov.buf_len = static_cast<uint32_t>(slice.size);
            group.iovs.push_back(iov);
            nbytes += slice.size;
        }
        if (group.iovs.empty()) continue;
        groups.push_back(std::move(group));
    }
    if (groups.empty()) return NdsReadStats{};

    std::vector<nds_io_cb> cbs(groups.size());
    for (size_t i = 0; i < groups.size(); ++i) {
        nds_io_cb cb{};
        cb.opcode = NDS_IO_OP_PREAD;
        cb.rw_flags = 0;
        cb.obj.fd = groups[i].fd;
        cb.offset = groups[i].offset;
        cb.user_data = i + 1;
        cb.iov = groups[i].iovs.data();
        cb.iov_cnt = static_cast<uint32_t>(groups[i].iovs.size());
        cb.host_pid = 0;
        cbs[i] = cb;
    }

    auto start = std::chrono::steady_clock::now();
    int accepted =
        Api().io_submit(Api().ctx, static_cast<int>(cbs.size()), cbs.data());
    if (accepted < 0) {
        LOG(ERROR) << "nds_io_submit failed rc=" << accepted;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    if (static_cast<size_t>(accepted) != cbs.size()) {
        LOG(ERROR) << "nds_io_submit short accept " << accepted << "/"
                   << cbs.size();
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    std::vector<char> done(groups.size(), 0);
    size_t finished = 0;
    auto deadline = start + std::chrono::seconds(60);
    while (finished < groups.size()) {
        if (std::chrono::steady_clock::now() > deadline) {
            LOG(ERROR) << "NDS disk read timed out, done=" << finished << "/"
                       << groups.size();
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        nds_io_event events[32];
        timespec timeout{};
        timeout.tv_sec = 1;
        int n = Api().io_getevents(Api().ctx, 1, 32, events, &timeout);
        if (n < 0) {
            LOG(ERROR) << "nds_io_getevents failed rc=" << n;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        for (int i = 0; i < n; ++i) {
            uint64_t cookie = events[i].user_data;
            if (cookie == 0 || cookie > groups.size()) continue;
            size_t idx = static_cast<size_t>(cookie - 1);
            if (done[idx]) continue;
            done[idx] = 1;
            finished++;
            if (events[i].res != 0) {
                LOG(ERROR) << "NDS disk read failed cookie=" << cookie
                           << " res=" << events[i].res;
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
        }
    }
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
    NdsReadStats stats;
    stats.bytes = nbytes;
    stats.elapsed_us = static_cast<uint64_t>(us);
    stats.files = static_cast<int>(groups.size());
    stats.iocbs = static_cast<int>(groups.size());
    double mbps = us > 0 ? (static_cast<double>(nbytes) / 1048576.0) /
                               (static_cast<double>(us) / 1e6)
                         : 0;
    LOG(INFO) << "NDS store read done, files=" << stats.files
              << " iocbs=" << stats.iocbs << " bytes=" << stats.bytes
              << " elapsed_us=" << stats.elapsed_us
              << " bandwidth=" << mbps << "MB/s";
    return stats;
}

}  // namespace mooncake
