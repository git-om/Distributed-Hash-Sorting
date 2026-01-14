#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <cmath>

#include "BLAKE3/c/blake3.h"

#ifndef NONCE_SIZE
#define NONCE_SIZE 6
#endif
#ifndef HASH_SIZE
#define HASH_SIZE 10
#endif

struct Record {
    uint8_t hash[HASH_SIZE];
    uint8_t nonce[NONCE_SIZE];
};
static_assert(sizeof(Record) == (NONCE_SIZE + HASH_SIZE), "Record size must be NONCE_SIZE+HASH_SIZE");

static inline int cmp_hash(const uint8_t* a, const uint8_t* b) { return std::memcmp(a, b, HASH_SIZE); }
static inline bool rec_less(const Record& A, const Record& B) { return cmp_hash(A.hash, B.hash) < 0; }

static int logical_cores() {
    unsigned n = std::thread::hardware_concurrency();
    return n ? (int)n : 1;
}

static void blake3_hash_trunc(const uint8_t nonce[NONCE_SIZE], uint8_t out[HASH_SIZE]) {
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, nonce, NONCE_SIZE);
    uint8_t full[BLAKE3_OUT_LEN];
    blake3_hasher_finalize(&hasher, full, BLAKE3_OUT_LEN);
    std::memcpy(out, full, HASH_SIZE);
}

struct Options {
    std::string approach = "for";
    int threads = 0;
    int io_threads = 1;         // reserved for later I/O pool
    int compression = 0;        // reserved for extra credit
    int exponent_k = 26;        // total records = 2^k
    size_t mem_mb = 256;        // memory cap (MB)
    std::string final_file = "output.bin";
    std::string temp_file  = "temp"; // prefix for run files
    bool debug = false;
    size_t batch_size = 262144; // records per worker chunk (not critical here)
    size_t print_n = 0;
    size_t search_n = 0;        // reserved for search
    int difficulty = 3;         // reserved for search
    bool verify = false;
};

static void print_help() {
    std::printf(
"Usage: ./vaultx [OPTIONS]\n"
"  -a, --approach [task|for]\n"
"  -t, --threads NUM\n"
"  -i, --iothreads NUM\n"
"  -c, --compression NUM\n"
"  -k, --exponent NUM\n"
"  -m, --memory NUM    (MB)\n"
"  -f, --file NAME     (final output)\n"
"  -g, --file_final NAME (temp prefix for runs)\n"
"  -b, --batch-size NUM\n"
"  -p, --print NUM\n"
"  -s, --search NUM    (TODO)\n"
"  -q, --difficulty NUM\n"
"  -v, --verify [true|false]\n"
"  -d, --debug [true|false]\n"
"  -h, --help\n");
}

static Options parse_args(int argc, char** argv) {
    Options o;
    const char* short_opts = "a:t:i:c:k:m:f:g:b:p:s:q:v:d:h";
    const option long_opts[] = {
        {"approach",   required_argument, nullptr, 'a'},
        {"threads",    required_argument, nullptr, 't'},
        {"iothreads",  required_argument, nullptr, 'i'},
        {"compression",required_argument, nullptr, 'c'},
        {"exponent",   required_argument, nullptr, 'k'},
        {"memory",     required_argument, nullptr, 'm'},
        {"file",       required_argument, nullptr, 'f'},
        {"file_final", required_argument, nullptr, 'g'},
        {"batch-size", required_argument, nullptr, 'b'},
        {"print",      required_argument, nullptr, 'p'},
        {"search",     required_argument, nullptr, 's'},
        {"difficulty", required_argument, nullptr, 'q'},
        {"verify",     required_argument, nullptr, 'v'},
        {"debug",      required_argument, nullptr, 'd'},
        {"help",       no_argument,       nullptr, 'h'},
        {nullptr,0,nullptr,0}
    };
    while (true) {
        int idx=0; int c = getopt_long(argc, argv, short_opts, long_opts, &idx);
        if (c == -1) break;
        switch (c) {
            case 'a': o.approach    = optarg; break;
            case 't': o.threads     = std::max(0, std::atoi(optarg)); break;
            case 'i': o.io_threads  = std::max(1, std::atoi(optarg)); break;
            case 'c': o.compression = std::max(0, std::atoi(optarg)); break;
            case 'k': o.exponent_k  = std::max(1, std::atoi(optarg)); break;
            case 'm': o.mem_mb      = std::max(1, std::atoi(optarg)); break;
            case 'f': o.final_file  = optarg; break;
            case 'g': o.temp_file   = optarg; break;
            case 'b': o.batch_size  = std::max<size_t>(1, std::strtoull(optarg,nullptr,10)); break;
            case 'p': o.print_n     = std::strtoull(optarg,nullptr,10); break;
            case 's': o.search_n    = std::strtoull(optarg,nullptr,10); break;
            case 'q': o.difficulty  = std::max(1, std::atoi(optarg)); break;
            case 'v': o.verify      = (std::string(optarg)=="true"); break;
            case 'd': o.debug       = (std::string(optarg)=="true"); break;
            case 'h': print_help(); std::exit(0);
            default:  print_help(); std::exit(1);
        }
    }
    if (o.compression < 0 || o.compression > HASH_SIZE) {
        std::fprintf(stderr, "Invalid --compression; must be 0..%d\n", HASH_SIZE);
        std::exit(1);
    }
    return o;
}

static void print_config(const Options& o) {
    const size_t rec_size   = HASH_SIZE + NONCE_SIZE;
    const double file_recs  = std::pow(2.0, o.exponent_k);
    const double target_b   = file_recs * rec_size;
    const double target_gb  = target_b / (1024.0*1024.0*1024.0);

    std::printf("Selected Approach : %s\n", o.approach.c_str());
    std::printf("Number of Threads : %d\n", (o.threads>0? o.threads : logical_cores()));
    std::printf("Exponent K : %d\n", o.exponent_k);
    std::printf("File Size (GB) : %.2f\n", target_gb);
    std::printf("File Size (bytes) : %.0f\n", target_b);
    std::printf("Memory Size (MB) : %zu\n", o.mem_mb);
    std::printf("Memory Size (bytes) : %llu\n", (unsigned long long)(o.mem_mb * 1024ULL * 1024ULL));
    std::printf("Size of HASH : %d\n", HASH_SIZE);
    std::printf("Size of NONCE : %d\n", NONCE_SIZE);
    std::printf("Size of MemoRecord : %zu\n", rec_size);
    std::printf("BATCH_SIZE : %zu\n", o.batch_size);
    std::printf("Temporary File Prefix : %s\n", o.temp_file.c_str());
    std::printf("Final Output File : %s\n", o.final_file.c_str());
}

static void gen_range(uint64_t base_nonce, size_t start, size_t end, Record* out) {
    for (size_t i = start; i < end; ++i) {
        uint64_t v = base_nonce + i;
        Record r{};
        for (int b=0; b<NONCE_SIZE; ++b) { r.nonce[b] = (uint8_t)(v & 0xFF); v >>= 8; }
        blake3_hash_trunc(r.nonce, r.hash);
        out[i] = r;
    }
}

static std::string run_name(const std::string& prefix, int idx) {
    return prefix + ".run" + std::to_string(idx);
}

struct RunReader {
    std::ifstream in;
    std::vector<Record> buf;
    size_t pos = 0;
    bool eof = false;
    size_t cap;
    RunReader(const std::string& path, size_t chunk_records)
        : in(path, std::ios::binary), buf(chunk_records), cap(chunk_records) {
        if (!in) throw std::runtime_error("cannot open run " + path);
        refill();
    }
    void refill() {
        if (eof) return;
        buf.resize(cap); // ensure capacity-sized read
        in.read(reinterpret_cast<char*>(buf.data()), (std::streamsize)(cap*sizeof(Record)));
        std::streamsize got = in.gcount();
        size_t recs = (size_t)got / sizeof(Record);
        buf.resize(recs);
        pos = 0;
        if (recs == 0) { eof = true; buf.clear(); }
    }
    bool has() const { return !eof && pos < buf.size(); }
    const Record& peek() const { return buf[pos]; }
    void pop() {
        ++pos;
        if (pos >= buf.size()) refill();
    }
};

static void merge_runs(const std::vector<std::string>& runs, const std::string& final_file, size_t merge_buf_records=65536) {
    struct Node { Record rec; int run; };
    struct Cmp { bool operator()(const Node& a, const Node& b) const {
        return cmp_hash(a.rec.hash, b.rec.hash) > 0; // min-heap
    }};
    std::vector<std::unique_ptr<RunReader>> readers;
    readers.reserve(runs.size());
    for (auto& r : runs) readers.emplace_back(new RunReader(r, merge_buf_records));

    std::priority_queue<Node, std::vector<Node>, Cmp> pq;
    for (int i=0;i<(int)readers.size();++i) if (readers[i]->has()) { pq.push(Node{readers[i]->peek(), i}); readers[i]->pop(); }

    std::ofstream out(final_file, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot open final file");

    std::vector<Record> outbuf; outbuf.reserve(merge_buf_records);
    while (!pq.empty()) {
        Node n = pq.top(); pq.pop();
        outbuf.push_back(n.rec);
        if (outbuf.size() >= merge_buf_records) {
            out.write(reinterpret_cast<const char*>(outbuf.data()), (std::streamsize)(outbuf.size()*sizeof(Record)));
            outbuf.clear();
        }
        int r = n.run;
        if (readers[r]->has()) { pq.push(Node{readers[r]->peek(), r}); readers[r]->pop(); }
    }
    if (!outbuf.empty()) out.write(reinterpret_cast<const char*>(outbuf.data()), (std::streamsize)(outbuf.size()*sizeof(Record)));
    out.close();
}

static bool verify_sorted(const std::string& final_file, double& mbps) {
    std::ifstream in(final_file, std::ios::binary);
    if (!in) return false;
    const size_t CHUNK = 1<<18; // 262,144 records (~4 MB)
    std::vector<Record> buf(CHUNK);

    auto t0 = std::chrono::high_resolution_clock::now();
    bool ok=true; Record prev{}; bool have_prev=false; size_t total=0;
    while (true) {
        in.read(reinterpret_cast<char*>(buf.data()), (std::streamsize)(buf.size()*sizeof(Record)));
        std::streamsize got = in.gcount();
        size_t recs = (size_t)got / sizeof(Record);
        if (recs == 0) break;
        for (size_t i=0;i<recs;i++) {
            if (have_prev && cmp_hash(prev.hash, buf[i].hash) > 0) { ok=false; break; }
            prev = buf[i]; have_prev=true; total++;
        }
        if (!ok) break;
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(t1-t0).count();
    std::ifstream::pos_type size_bytes = 0;
    { std::ifstream f(final_file, std::ios::binary|std::ios::ate); if (f) size_bytes = f.tellg(); }
    mbps = (size_bytes / (1024.0*1024.0)) / (sec>0?sec:1.0);
    return ok;
}

static void print_first(const std::string& final_file, size_t N) {
    std::ifstream in(final_file, std::ios::binary);
    if (!in) { std::cerr << "cannot open " << final_file << "\n"; return; }
    for (size_t i=0; i<N; ++i) {
        Record r;
        in.read(reinterpret_cast<char*>(&r), sizeof(r));
        if (!in) break;
        std::cout << "[" << (i*sizeof(Record)) << "] ";
        for (int j=0;j<HASH_SIZE;j++) std::cout << std::hex << std::setw(2) << std::setfill('0') << (int)r.hash[j];
        std::cout << std::dec << " nonce=";
        unsigned long long dec=0;
        for (int b=NONCE_SIZE-1;b>=0;--b) dec = (dec<<8) | r.nonce[b];
        std::cout << dec << "\n";
    }
}

int main(int argc, char** argv) {
    Options opt = parse_args(argc, argv);
    print_config(opt);

    const size_t rec_size = sizeof(Record);
    const uint64_t total_records = (uint64_t)1 << opt.exponent_k;

    size_t max_bytes = opt.mem_mb * 1024ULL * 1024ULL;
    size_t max_recs_per_run = std::max<size_t>(1, max_bytes / rec_size);

    int T = (opt.threads>0? opt.threads : logical_cores());

    auto t0 = std::chrono::high_resolution_clock::now();
    std::vector<std::string> runs; runs.reserve((size_t)((total_records + max_recs_per_run - 1) / max_recs_per_run));

    uint64_t produced = 0;
    int run_idx = 0;
    while (produced < total_records) {
        uint64_t todo = std::min<uint64_t>(max_recs_per_run, total_records - produced);
        std::vector<Record> buf; buf.resize((size_t)todo);

        size_t chunk = (todo + T - 1) / T;
        std::vector<std::thread> pool; pool.reserve(T);
        for (int th=0; th<T; ++th) {
            size_t start = th * chunk;
            if (start >= todo) break;
            size_t end = std::min<uint64_t>(todo, start + chunk);
            pool.emplace_back(gen_range, produced, start, end, buf.data());
        }
        for (auto& th: pool) th.join();

        std::sort(buf.begin(), buf.end(), rec_less);

        std::string rname = run_name(opt.temp_file, run_idx++);
        std::ofstream out(rname, std::ios::binary | std::ios::trunc);
        if (!out) { std::cerr << "cannot open " << rname << " for write\n"; return 1; }
        out.write(reinterpret_cast<const char*>(buf.data()), (std::streamsize)(buf.size()*sizeof(Record)));
        out.close();

        runs.push_back(rname);
        produced += todo;

        if (opt.debug) {
            double pct = (100.0 * produced) / (double)total_records;
            std::cerr << "[run " << (run_idx-1) << "] wrote " << todo << " recs (" << pct << "%)\n";
        }
    }

    merge_runs(runs, opt.final_file, 65536);
    for (auto& r: runs) std::remove(r.c_str());

    auto t1 = std::chrono::high_resolution_clock::now();
    double total_sec = std::chrono::duration<double>(t1 - t0).count();
    if (total_sec <= 0) total_sec = 1e-9;

    double mh_s = (total_records / 1e6) / total_sec;
    double mb_s = ((total_records * rec_size) / (1024.0*1024.0)) / total_sec;

    if (opt.verify) {
        double vmbps=0.0;
        bool ok = verify_sorted(opt.final_file, vmbps);
        std::cout << (ok ? "verify: OK " : "verify: FAIL ") << "read_MBps=" << std::fixed << std::setprecision(2) << vmbps << "\n";
    }
    if (opt.print_n > 0) print_first(opt.final_file, opt.print_n);

    std::printf("vaultx t%d i%d m%zu k%d %.2f %.2f %.6f\n",
        (opt.threads>0? opt.threads : logical_cores()),
        opt.io_threads, opt.mem_mb, opt.exponent_k, mh_s, mb_s, total_sec);
    return 0;
}
