#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <random>
#include <chrono>
#include <getopt.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef NONCE_SIZE
#define NONCE_SIZE 6
#endif
#ifndef HASH_SIZE
#define HASH_SIZE 10
#endif
static constexpr size_t REC_SIZE = HASH_SIZE + NONCE_SIZE;

static int cmp_hash(const uint8_t* a, const uint8_t* b){ return std::memcmp(a,b,HASH_SIZE); }

static bool read_hash_at(int fd, uint64_t idx, uint8_t out_hash[HASH_SIZE]){
    uint8_t buf[REC_SIZE];
    off_t off = (off_t)(idx * REC_SIZE);
    ssize_t n = pread(fd, buf, REC_SIZE, off);
    if(n != (ssize_t)REC_SIZE) return false;
    std::memcpy(out_hash, buf, HASH_SIZE);
    return true;
}

static uint64_t lower_bound_fd(int fd, uint64_t N, const uint8_t key[HASH_SIZE],
                               uint64_t& comps, uint64_t& seeks, uint64_t& reads_ok){
    uint64_t lo=0, hi=N;
    while(lo < hi){
        uint64_t mid = lo + ((hi-lo)>>1);
        uint8_t h[HASH_SIZE];
        seeks++; comps++;
        if(!read_hash_at(fd, mid, h)) break;
        reads_ok++;
        if(cmp_hash(h,key) < 0) lo = mid+1; else hi = mid;
    }
    return lo;
}

static uint64_t upper_bound_fd(int fd, uint64_t N, const uint8_t key[HASH_SIZE],
                               uint64_t& comps, uint64_t& seeks, uint64_t& reads_ok){
    uint64_t lo=0, hi=N;
    while(lo < hi){
        uint64_t mid = lo + ((hi-lo)>>1);
        uint8_t h[HASH_SIZE];
        seeks++; comps++;
        if(!read_hash_at(fd, mid, h)) break;
        reads_ok++;
        if(cmp_hash(h,key) <= 0) lo = mid+1; else hi = mid;
    }
    return lo;
}

static void make_prefix_bounds(const uint8_t prefix[], int D,
                               uint8_t low[HASH_SIZE], uint8_t high[HASH_SIZE]){
    std::memset(low, 0x00, HASH_SIZE);
    std::memset(high,0xFF, HASH_SIZE);
    std::memcpy(low,  prefix, D);
    std::memcpy(high, prefix, D);
}

struct Opt { int k=26; std::string file; size_t searches=1000; int diff=3; bool debug=false; };
static Opt parse(int argc, char** argv){
    Opt o; const char* s="k:f:s:q:d:h";
    const option l[]={{"k",required_argument,nullptr,'k'},
                      {"file",required_argument,nullptr,'f'},
                      {"searches",required_argument,nullptr,'s'},
                      {"difficulty",required_argument,nullptr,'q'},
                      {"debug",required_argument,nullptr,'d'},
                      {"help",no_argument,nullptr,'h'},{nullptr,0,nullptr,0}};
    while(true){
        int i=0,c=getopt_long(argc,argv,s,l,&i);
        if(c==-1) break;
        if(c=='k') o.k=std::max(1,atoi(optarg));
        else if(c=='f') o.file=optarg;
        else if(c=='s') o.searches=strtoull(optarg,nullptr,10);
        else if(c=='q') o.diff=std::max(1,atoi(optarg));
        else if(c=='d') o.debug=(std::string(optarg)=="true");
        else { std::fprintf(stderr,"Usage: ./searchx -k K -f FILE -s N -q D [-d true|false]\n"); std::exit(1);}
    }
    if(o.file.empty()){ std::fprintf(stderr,"Missing -f FILE\n"); std::exit(1); }
    if(o.diff>HASH_SIZE) o.diff=HASH_SIZE;
    return o;
}

int main(int argc, char** argv){
    Opt opt = parse(argc, argv);
    int fd = ::open(opt.file.c_str(), O_RDONLY);
    if(fd<0){ perror("open"); return 1; }
    struct stat st{}; if(fstat(fd,&st)!=0){ perror("fstat"); ::close(fd); return 1; }
    if(st.st_size % REC_SIZE != 0){ std::fprintf(stderr,"File size not multiple of %zu\n", REC_SIZE); ::close(fd); return 1; }
    uint64_t N = (uint64_t)st.st_size / REC_SIZE;

    std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint32_t> u8(0,255);

    uint64_t total_seeks=0,total_comps=0,total_reads_ok=0;
    uint64_t total_matches=0,found_q=0,notfound=0;

    auto T0=std::chrono::high_resolution_clock::now();
    if(opt.debug){
        std::printf("searches=%zu difficulty=%d\n", opt.searches, opt.diff);
        std::printf("Hash Size : %d  Nonce Size : %d  Rec Size : %zu\n", HASH_SIZE, NONCE_SIZE, REC_SIZE);
        std::printf("Number of Hashes : %llu  File Size : %llu bytes\n",
            (unsigned long long)N, (unsigned long long)st.st_size);
    }

    for(size_t q=0;q<opt.searches;++q){
        uint8_t prefix[HASH_SIZE]={0};
        for(int i=0;i<opt.diff;++i) prefix[i]=(uint8_t)u8(rng);
        uint8_t low[HASH_SIZE], high[HASH_SIZE];
        make_prefix_bounds(prefix,opt.diff,low,high);

        uint64_t seeks=0, comps=0, reads_ok=0;
        uint64_t lo = lower_bound_fd(fd,N,low,comps,seeks,reads_ok);
        uint64_t hi = upper_bound_fd(fd,N,high,comps,seeks,reads_ok);
        uint64_t matches = (hi>lo?hi-lo:0);

        total_seeks+=seeks; total_comps+=comps; total_reads_ok+=reads_ok;
        total_matches+=matches; if(matches>0) ++found_q; else ++notfound;

        if(opt.debug){
            char hex[7]={0}; for(int i=0;i<3 && i<opt.diff;++i) std::sprintf(hex+2*i,"%02x",prefix[i]);
            if(matches) std::printf("[%zu] %s MATCHES=%llu comps=%llu seeks=%llu\n", q, hex,
                (unsigned long long)matches,(unsigned long long)comps,(unsigned long long)seeks);
            else        std::printf("[%zu] %s NOTFOUND comps=%llu seeks=%llu\n", q, hex,
                (unsigned long long)comps,(unsigned long long)seeks);
        }
    }
    auto T1=std::chrono::high_resolution_clock::now();
    double total_s = std::chrono::duration<double>(T1-T0).count();
    double avg_ms  = (opt.searches? (total_s*1000.0/opt.searches):0.0);
    double qps     = (total_s>0? (opt.searches/total_s):0.0);
    uint64_t total_bytes_read = total_reads_ok * REC_SIZE;
    double avg_bytes_per_search = (opt.searches? (double)total_bytes_read/opt.searches:0.0);

    std::printf("Search Summary: requested=%zu performed=%zu found_queries=%llu total_matches=%llu notfound=%llu\n",
        opt.searches,opt.searches,(unsigned long long)found_q,(unsigned long long)total_matches,(unsigned long long)notfound);
    std::printf("total_time=%.6f s avg_ms=%.3f ms searches/sec=%.2f total_seeks=%llu\n",
        total_s,avg_ms,qps,(unsigned long long)total_seeks);
    std::printf("avg_seeks_per_search=%.3f total_comps=%llu avg_comps_per_search=%.3f\n",
        (opt.searches? (double)total_seeks/opt.searches:0.0),
        (unsigned long long)total_comps,
        (opt.searches? (double)total_comps/opt.searches:0.0));
    std::printf("avg_bytes_read_per_search=%.1f\n", avg_bytes_per_search);
    ::close(fd);
    return 0;
}
