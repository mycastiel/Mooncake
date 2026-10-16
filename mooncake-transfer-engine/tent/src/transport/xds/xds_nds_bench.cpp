// NPU 0-4 to a local block device through the XDS/NDS transport.
// generate: write the range so the disk has extent metadata.
// load: new process, NDS scans that metadata again, then read it back.

#include <dlfcn.h>
#include <getopt.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"

namespace {

constexpr int kHostToDevice = 1;
constexpr int kDeviceToHost = 2;
constexpr int kHugeFirst = 0;

using acl_init_fn = int (*)(const char*);
using acl_finalize_fn = int (*)();
using aclrt_set_device_fn = int (*)(int);
using aclrt_malloc_fn = int (*)(void**, size_t, int);
using aclrt_free_fn = int (*)(void*);
using aclrt_memcpy_fn = int (*)(void*, size_t, const void*, size_t, int);

struct Acl {
    void* handle = nullptr;
    acl_init_fn init = nullptr;
    acl_finalize_fn finalize = nullptr;
    aclrt_set_device_fn set_device = nullptr;
    aclrt_malloc_fn malloc = nullptr;
    aclrt_free_fn free = nullptr;
    aclrt_memcpy_fn memcpy = nullptr;
};

struct DeviceBuf {
    int id = 0;
    void* addr = nullptr;
    size_t bytes = 0;
    uint64_t offset = 0;
};

void die(const std::string& msg) {
    fprintf(stderr, "xds_nds_bench: %s\n", msg.c_str());
    exit(1);
}

uint64_t parse_size(const char* text) {
    char* end = nullptr;
    unsigned long long v = strtoull(text, &end, 0);
    if (end == text) die(std::string("bad size: ") + text);
    if (*end == 'k' || *end == 'K') v <<= 10;
    else if (*end == 'm' || *end == 'M') v <<= 20;
    else if (*end == 'g' || *end == 'G') v <<= 30;
    return v;
}

Acl load_acl() {
    Acl acl;
    acl.handle = dlopen("libascendcl.so", RTLD_NOW);
    if (!acl.handle) die(std::string("dlopen libascendcl.so: ") + dlerror());
    acl.init = (acl_init_fn)dlsym(acl.handle, "aclInit");
    acl.finalize = (acl_finalize_fn)dlsym(acl.handle, "aclFinalize");
    acl.set_device = (aclrt_set_device_fn)dlsym(acl.handle, "aclrtSetDevice");
    acl.malloc = (aclrt_malloc_fn)dlsym(acl.handle, "aclrtMalloc");
    acl.free = (aclrt_free_fn)dlsym(acl.handle, "aclrtFree");
    acl.memcpy = (aclrt_memcpy_fn)dlsym(acl.handle, "aclrtMemcpy");
    if (!acl.init || !acl.finalize || !acl.set_device || !acl.malloc ||
        !acl.free || !acl.memcpy)
        die("libascendcl.so is missing ACL symbols");
    return acl;
}

void usage(const char* argv0) {
    fprintf(stderr,
            "Usage: %s --mode generate|load|read --device /dev/md0 "
            "[--cards 0,1,2,3,4] [--bytes 1G] [--io 1M] [--depth 16]\n",
            argv0);
}

}  // namespace

int main(int argc, char** argv) {
    std::string mode;
    std::string device;
    std::string cards_text = "0,1,2,3,4";
    uint64_t bytes = 1ull << 30;
    uint64_t io_size = 1ull << 20;
    int depth = 16;

    static option opts[] = {
        {"mode", required_argument, nullptr, 'm'},
        {"device", required_argument, nullptr, 'd'},
        {"cards", required_argument, nullptr, 'c'},
        {"bytes", required_argument, nullptr, 'b'},
        {"io", required_argument, nullptr, 'i'},
        {"depth", required_argument, nullptr, 'q'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };
    int c;
    while ((c = getopt_long(argc, argv, "m:d:c:b:i:q:h", opts, nullptr)) !=
           -1) {
        switch (c) {
            case 'm':
                mode = optarg;
                break;
            case 'd':
                device = optarg;
                break;
            case 'c':
                cards_text = optarg;
                break;
            case 'b':
                bytes = parse_size(optarg);
                break;
            case 'i':
                io_size = parse_size(optarg);
                break;
            case 'q':
                depth = atoi(optarg);
                break;
            default:
                usage(argv[0]);
                return c == 'h' ? 0 : 1;
        }
    }
    if ((mode != "generate" && mode != "load" && mode != "read") ||
        device.empty() ||
        bytes == 0 || io_size == 0 || depth <= 0)
        die("need --mode generate|load and --device");
    if ((bytes | io_size) & 511) die("bytes and io must be 512-byte aligned");

    std::vector<int> cards;
    size_t pos = 0;
    while (pos < cards_text.size()) {
        size_t comma = cards_text.find(',', pos);
        if (comma == std::string::npos) comma = cards_text.size();
        cards.push_back(atoi(cards_text.substr(pos, comma - pos).c_str()));
        pos = comma + 1;
    }
    if (cards.empty()) die("no cards");

    unsetenv("MC_TENT_CONF");
    printf("xds_nds_bench mode=%s device=%s cards=%s bytes_per_card=%llu "
           "io=%llu depth=%d\n",
           mode.c_str(), device.c_str(), cards_text.c_str(),
           (unsigned long long)bytes, (unsigned long long)io_size, depth);
    fflush(stdout);

    Acl acl = load_acl();
    int rc = acl.init(nullptr);
    if (rc != 0) die("aclInit failed: " + std::to_string(rc));

    std::vector<DeviceBuf> bufs;
    std::vector<uint8_t> pattern(io_size);
    for (size_t i = 0; i < pattern.size(); i++)
        pattern[i] = static_cast<uint8_t>(i * 17 + 3);

    for (size_t i = 0; i < cards.size(); i++) {
        rc = acl.set_device(cards[i]);
        if (rc != 0) die("aclrtSetDevice failed: " + std::to_string(rc));
        void* addr = nullptr;
        rc = acl.malloc(&addr, bytes, kHugeFirst);
        if (rc != 0 || !addr)
            die("aclrtMalloc failed on card " + std::to_string(cards[i]) +
                ": " + std::to_string(rc));
        if (mode == "generate") {
            for (uint64_t off = 0; off < bytes; off += io_size) {
                rc = acl.memcpy(static_cast<uint8_t*>(addr) + off, io_size,
                                pattern.data(), io_size, kHostToDevice);
                if (rc != 0) die("aclrtMemcpy H2D failed");
            }
        }
        DeviceBuf buf;
        buf.id = cards[i];
        buf.addr = addr;
        buf.bytes = bytes;
        buf.offset = i * bytes;
        bufs.push_back(buf);
        printf("HBM card=%d addr=%p size=%zu disk_offset=%llu\n", buf.id,
               buf.addr, buf.bytes, (unsigned long long)buf.offset);
    }

    auto conf = std::make_shared<mooncake::tent::Config>();
    conf->set("metadata_type", std::string("p2p"));
    conf->set("local_segment_name", std::string("xds-nds-bench"));
    conf->set("log_level", std::string("info"));
    conf->set("merge_requests", false);
    conf->set("transports/xds/enable", true);
    conf->set("transports/xds/io_batch_depth", 256);
    conf->set("transports/io_uring/enable", false);
    conf->set("transports/rdma/enable", false);
    conf->set("transports/tcp/enable", true);
    conf->set("transports/ascend_direct/enable", false);

    mooncake::tent::TransferEngine engine(conf);
    if (!engine.available()) die("transfer engine is not available");
    printf("transfer engine up, segment=%s\n", engine.getSegmentName().c_str());

    mooncake::tent::SegmentID file = 0;
    auto status = engine.openSegment(file, "file://" + device);
    if (!status.ok()) die("openSegment: " + status.ToString());

    for (auto& buf : bufs) {
        rc = acl.set_device(buf.id);
        if (rc != 0) die("aclrtSetDevice before register failed");
        mooncake::tent::MemoryOptions opt;
        opt.location = "npu:" + std::to_string(buf.id);
        opt.type = mooncake::tent::XDS;
        status = engine.registerLocalMemory(buf.addr, buf.bytes, opt);
        if (!status.ok())
            die("registerLocalMemory card " + std::to_string(buf.id) + ": " +
                status.ToString());
    }

    const int batch_slots = std::max(1, depth) * static_cast<int>(bufs.size());
    std::vector<uint64_t> cursor(bufs.size(), 0);
    size_t submit_cursor = 0;
    uint64_t done_bytes = 0;
    const uint64_t total_bytes = bytes * bufs.size();

    auto next_request = [&](mooncake::tent::Request& req) -> bool {
        for (size_t n = 0; n < bufs.size(); n++) {
            size_t i = (submit_cursor + n) % bufs.size();
            if (cursor[i] >= bufs[i].bytes) continue;
            uint64_t off = cursor[i];
            cursor[i] += io_size;
            submit_cursor = i + 1;
            req.opcode = mode == "generate" ? mooncake::tent::Request::WRITE
                                            : mooncake::tent::Request::READ;
            req.source = static_cast<uint8_t*>(bufs[i].addr) + off;
            req.target_id = file;
            req.target_offset = bufs[i].offset + off;
            req.length = io_size;
            req.transport_hint = mooncake::tent::XDS;
            return true;
        }
        return false;
    };

    auto t0 = std::chrono::steady_clock::now();
    while (done_bytes < total_bytes) {
        std::vector<mooncake::tent::Request> reqs;
        reqs.reserve(batch_slots);
        for (int n = 0; n < batch_slots; n++) {
            mooncake::tent::Request req;
            if (!next_request(req)) break;
            reqs.push_back(req);
        }
        if (reqs.empty()) die("no requests left before the byte count finished");
        auto batch = engine.allocateBatch(reqs.size());
        status = engine.submitTransfer(batch, reqs);
        if (!status.ok()) die("submitTransfer: " + status.ToString());
        std::vector<int> finished(reqs.size(), 0);
        size_t left = reqs.size();
        while (left > 0) {
            for (size_t i = 0; i < reqs.size(); i++) {
                if (finished[i]) continue;
                mooncake::tent::TransferStatus st;
                status = engine.getTransferStatus(batch, i, st);
                if (!status.ok())
                    die("getTransferStatus: " + status.ToString());
                if (st.s == mooncake::tent::PENDING ||
                    st.s == mooncake::tent::INITIAL)
                    continue;
                if (st.s != mooncake::tent::COMPLETED)
                    die("transfer failed status=" + std::to_string(st.s));
                finished[i] = 1;
                left--;
                done_bytes += io_size;
            }
        }
        engine.freeBatch(batch);
    }
    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    double gib = static_cast<double>(done_bytes) / (1ull << 30);
    printf("NDS %s done: bytes=%llu time=%.3fs bandwidth=%.2f GiB/s "
           "(%.2f GB/s) cards=%zu\n",
           mode.c_str(), (unsigned long long)done_bytes, sec, gib / sec,
           (done_bytes / 1e9) / sec, bufs.size());
    fflush(stdout);

    if (mode == "load") {
        std::vector<uint8_t> back(io_size);
        for (auto& buf : bufs) {
            rc = acl.set_device(buf.id);
            if (rc != 0) die("aclrtSetDevice before verify failed");
            rc = acl.memcpy(back.data(), io_size, buf.addr, io_size,
                            kDeviceToHost);
            if (rc != 0) die("aclrtMemcpy D2H failed");
            if (memcmp(back.data(), pattern.data(), io_size) != 0)
                die("payload mismatch on card " + std::to_string(buf.id) +
                    " (NDS read did not return the generated data)");
            printf("NDS verify ok: card=%d first_io=%llu bytes match\n",
                   buf.id, (unsigned long long)io_size);
        }
    }

    for (auto& buf : bufs) {
        acl.set_device(buf.id);
        engine.unregisterLocalMemory(buf.addr, buf.bytes);
        acl.free(buf.addr);
    }
    acl.finalize();
    printf("xds_nds_bench %s finished\n", mode.c_str());
    return 0;
}
