// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <algorithm>
#include <chrono>
#include <thread>
#include <fstream>
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#include <sys/types.h>
#include <gflags/gflags.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>

#include "dLSM/cache.h"
#include "db/table_cache.h"
#include "dLSM/comparator.h"
#include "dLSM/db.h"
#include "dLSM/env.h"
#include "dLSM/filter_policy.h"
#include "dLSM/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      seekordered   -- N ordered seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
DEFINE_string(
    benchmarks, 
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,", "");


DEFINE_int32(num, 1000000, "Number of key/values to place in database");
DEFINE_int32(reads, -1, "Number of read operations to do.  If negative, do FLAGS_num reads");
DEFINE_int32(threads, 1, "Number of concurrent threads to run");
DEFINE_int32(key_size, 20, "Size of each value");
DEFINE_int32(value_size, 400, "Size of each value");
DEFINE_double(compression_ratio, 0.5, "Arrange to generate values that shrink to this fraction of"
                                      "their original size after compression");
DEFINE_bool(histogram, false, "Print histogram of operation timings");
DEFINE_bool(comparisons, false, "Count the number of string comparisons performed");
DEFINE_int32(compute_node_id, 0, "");
DEFINE_int32(cache_size, -1, "Number of bytes to use as a table_cache of uncompressed data, Negative means use no table_cache");
DEFINE_int32(open_files, 0, "Maximum number of files to keep open at the same time (use default if == 0)");
DEFINE_int32(block_restart_interval, 1, "sstable data block restart interval");
DEFINE_int32(bloom_bits, 10, "Bloom filter bits per key, Negative means use default settings");
DEFINE_int32(key_prefix, 0, "Common key prefix length");
DEFINE_bool(enable_numa, false, "whether the writer threads aware of the NUMA archetecture");
DEFINE_bool(reuse_logs, false, "If true, reuse existing log/MANIFEST files when re-opening a database");
DEFINE_int32(readwritepercent, 90, "");
DEFINE_int32(ops_between_duration_checks, 2000, "");
DEFINE_int32(duration, 0, "");
DEFINE_string(db, "", "Use the db with the following name");

// if larger than 0, the compute node will always have this number of shards no matter how many memory
// nodes are there. THe shards will be distributed to the memory nodes in a round robin manner.
DEFINE_int32(fixed_compute_shards_num, 0, "see code comment");

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
DEFINE_bool(use_existing_db, false, "see code comment");

// following are initialized to default value by `main`
DEFINE_int32(write_buffer_size, 0, "Number of bytes to buffer in memtable before compacting");
DEFINE_int32(max_file_size, 0, "Number of bytes written to each file");
DEFINE_int32(block_size, 0, "Approximate size of user data packed per block (before compression)");

// flush related
DEFINE_int32(max_background_flushes, dLSM::Options().max_background_flushes, "max_background_flushes");
DEFINE_int32(max_background_compactions, dLSM::Options().max_background_compactions, "max_backgroud_compactions");

/*******  for write stall test   ******/
DEFINE_bool(record_speed, false , "whetherto record speed info");
DEFINE_int32(record_interval, 1000, "speed info record interval (ms)");
DEFINE_string(record_dump_path, "record_speed_result.txt", "speed info record dump path");
DEFINE_bool(memtable_only, false, "only test memtable, write only");
namespace dLSM {

namespace {
dLSM::Env* g_env = nullptr;
dLSM::RDMA_Manager* rdma_mg = nullptr;
uint64_t number_of_key_total;
uint64_t number_of_key_per_compute;
uint64_t number_of_key_per_shard;
class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = g_env->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = g_env->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};
class CountComparator : public Comparator {
 public:
  CountComparator(const Comparator* wrapped) : wrapped_(wrapped) {}
  ~CountComparator() override {}
  int Compare(const Slice& a, const Slice& b) const override {
    count_.fetch_add(1, std::memory_order_relaxed);
    return wrapped_->Compare(a, b);
  }
  const char* Name() const override { return wrapped_->Name(); }
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    wrapped_->FindShortestSeparator(start, limit);
  }

  void FindShortSuccessor(std::string* key) const override {
    return wrapped_->FindShortSuccessor(key);
  }

  size_t comparisons() const { return count_.load(std::memory_order_relaxed); }

  void reset() { count_.store(0, std::memory_order_relaxed); }

 private:
  mutable std::atomic<size_t> count_{0};
  const Comparator* const wrapped_;
};

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

class KeyBuffer {
 public:
  KeyBuffer() {
    assert(FLAGS_key_prefix < sizeof(buffer_));
    memset(buffer_, 'a', FLAGS_key_prefix);
  }
  KeyBuffer& operator=(KeyBuffer& other) = delete;
  KeyBuffer(KeyBuffer& other) = delete;

  void Set(int k) {
    std::snprintf(buffer_ + FLAGS_key_prefix,
                  sizeof(buffer_) - FLAGS_key_prefix, "%020d", k); //%016d means preceeding with 0s
  }

  Slice slice() const { return Slice(buffer_, FLAGS_key_prefix + 20); }

 private:
  char buffer_[1024];
};

#if defined(__linux)
static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit - 1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}
#endif

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  std::atomic<uint64_t> done_;
  uint64_t next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }
  
  inline uint64_t GetFinishedOps() {
    return done_.load(std::memory_order_relaxed);
  }

  void Start() {
    next_report_ = 100;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    message_.clear();
    start_ = finish_ = last_op_finish_ = g_env->NowMicros();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = g_env->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        std::fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        std::fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_.fetch_add(1, std::memory_order_relaxed);
    if (GetFinishedOps() >= next_report_) {
      if (next_report_ < 1000)
        next_report_ += 100;
      else if (next_report_ < 5000)
        next_report_ += 500;
      else if (next_report_ < 10000)
        next_report_ += 1000;
      else if (next_report_ < 50000)
        next_report_ += 5000;
      else if (next_report_ < 100000)
        next_report_ += 10000;
      else if (next_report_ < 500000)
        next_report_ += 50000;
      else
        next_report_ += 100000;
      std::fprintf(stderr, "... finished %lu ops%30s\r", GetFinishedOps(), "");
      std::fflush(stderr);
    }
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;
    double elapsed = (finish_ - start_) * 1e-6;
    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.

      char rate[100];
      std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
                    (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    std::fprintf(stdout, "%-12s : %11.3f micros/op; %ld ops/sec;%s%s\n",
                 name.ToString().c_str(), seconds_ * 1e6 / done_, (long)(done_/elapsed),
                 (extra.empty() ? "" : " "), extra.c_str());

    if (FLAGS_histogram) {
      std::fprintf(stdout, "Microseconds per op:\n%s\n",
                   hist_.ToString().c_str());
    }
    std::fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv GUARDED_BY(mu);
  int total GUARDED_BY(mu);

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized GUARDED_BY(mu);
  int num_done GUARDED_BY(mu);
  bool start GUARDED_BY(mu);

  SharedState(int total)
      : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;      // 0..n-1 when running in n threads
  Random64 rand;  // Has different seeds for different threads
//  Random rand;
  Stats stats;
  SharedState* shared;

  ThreadState(int index, int seed) : tid(index), rand(seed), shared(nullptr) {}
};

}  // namespace

class Benchmark {
 private:
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;
  CountComparator count_comparator_;
  int total_thread_count_;
  std::vector<std::string> validation_keys;

  void PrintHeader() {
    const int kKeySize = 16 + FLAGS_key_prefix;
    PrintEnvironment();
    std::fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    std::fprintf(
        stdout, "Values:     %d bytes each (%d bytes after compression)\n",
        FLAGS_value_size,
        static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    std::fprintf(stdout, "Entries:    %d\n", num_);
    std::fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                 ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_) /
                  1048576.0));
    std::fprintf(
        stdout, "FileSize:   %.1f MB (estimated)\n",
        (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_) /
         1048576.0));
    PrintWarnings();
    std::fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    std::fprintf(
        stdout,
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    std::fprintf(
        stdout,
        "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      std::fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      std::fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    std::fprintf(stderr, "dLSM:    version %d.%d\n", kMajorVersion,
                 kMinorVersion);

#if defined(__linux)
    time_t now = time(nullptr);
    std::fprintf(stderr, "Date:       %s",
                 ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = std::fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "table_cache size") {
          cache_size = val.ToString();
        }
      }
      std::fclose(cpuinfo);
      std::fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      std::fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
      : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : nullptr),
        filter_policy_(FLAGS_bloom_bits >= 0
                           ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                           : nullptr),
        db_(nullptr),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        entries_per_batch_(1),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        heap_counter_(0),
        count_comparator_(BytewiseComparator()),
        total_thread_count_(0) {
    std::vector<std::string> files;
    g_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        g_env->RemoveFile(FLAGS_db + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }
  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[FLAGS_key_size];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), FLAGS_key_size);
  }
  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard, size_t key_size) {
    char* data = new char[key_size];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size);
  }
  void GenerateKeyFromInt(uint64_t v, Slice* key) {

    char* start = const_cast<char*>(key->data());
    char* pos = start;
//    if (keys_per_prefix_ > 0) {
//      int64_t num_prefix = num_keys / keys_per_prefix_;
//      int64_t prefix = v % num_prefix;
//      int bytes_to_fill = std::min(prefix_size_, 8);
//      if (port::kLittleEndian) {
//        for (int i = 0; i < bytes_to_fill; ++i) {
//          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
//        }
//      } else {
//        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
//      }
//      if (prefix_size_ > 8) {
//        // fill the rest with 0s
//        memset(pos + 8, '0', prefix_size_ - 8);
//      }
//      pos += prefix_size_;
//    }

    int bytes_to_fill = std::min(FLAGS_key_size, 8);
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
    pos += bytes_to_fill;
    if (FLAGS_key_size > pos - start) {
      memset(pos, '0', FLAGS_key_size - (pos - start));
    }
  }
  void Run() {

    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks.c_str();
//    Validation_Write();
    while (benchmarks != nullptr) {

      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == nullptr) {
        name = benchmarks;
        benchmarks = nullptr;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overridden below
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = nullptr;
      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      if (name == Slice("open")) {
        method = &Benchmark::OpenBench;
        num_ /= 10000;
        if (num_ < 1) num_ = 1;
      } else if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillrandomshard")) {
        fresh_db = true;
        method = &Benchmark::WriteRandomSharded;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readrandomshard")) {
        method = &Benchmark::ReadRandom_Sharded;
      } else if (name == Slice("readrandomwriterandom")) {
        method = &Benchmark::ReadRandomWriteRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("seekordered")) {
        method = &Benchmark::SeekOrdered;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("dLSM.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("dLSM.sstables");
      } else {
        if (!name.empty()) {  // No error message for empty name
          std::fprintf(stderr, "unknown benchmark '%s'\n",
                       name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          std::fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                       name.ToString().c_str());
          method = nullptr;
        } else {
          delete db_;
          db_ = nullptr;
          DestroyDB(FLAGS_db, Options());
          Open();
          DEBUG("The second open finished.\n");
        }
      }

      if (method != nullptr) {
#ifdef PROCESSANALYSIS
        if (method == &Benchmark::ReadRandom || method == &Benchmark::ReadWhileWriting){
          TableCache::CleanAll();
        }
#endif
        DEBUG("The benchmark start.\n");
        RunBenchmark(num_threads, name, method);
        DEBUG("Benchmark finished\n");

      }
    }
//    Validation_Read();
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    printf("Wait for thread start\n");
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }
    printf("Threads start to run\n");
    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, Slice name, void (Benchmark::*method)(ThreadState*)) {
  printf("Bechmark start\n");
  if (method == &Benchmark::WriteRandom || method == &Benchmark::WriteRandomSharded)
      Validation_Write();

    SharedState shared(n);

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
#ifdef NUMA
      if (FLAGS_enable_numa) {
        // Performs a local allocation of memory to threads in numa node.
        int n_nodes = numa_num_task_nodes();  // Number of nodes in NUMA.
        numa_exit_on_error = 1;
        int numa_node = i % n_nodes;
        bitmask* nodes = numa_allocate_nodemask();
        numa_bitmask_clearall(nodes);
        numa_bitmask_setbit(nodes, numa_node);
        // numa_bind() call binds the process to the node and these
        // properties are passed on to the thread that is created in
        // StartThread method called later in the loop.
        numa_bind(nodes);
        numa_set_strict(1);
        numa_free_nodemask(nodes);
      }
#endif
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      ++total_thread_count_;
      // Seed the thread's random state deterministically based upon thread
      // creation across all benchmarks. This ensures that the seeds are unique
      // but reproducible when rerunning the same set of benchmarks.
      arg[i].thread = new ThreadState(i, /*seed=*/1000 + total_thread_count_);
      arg[i].thread->shared = &shared;
      printf("start front-end threads\n");
      g_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }
    //sync with all the other compute nodes here
    rdma_mg->sync_with_computes_Cside();

    // start record speed
    auto speed_record_thread = std::thread([&]() {
      if (!FLAGS_record_speed) {
        return;
      }
      std::vector<uint64_t> ops_done;
      ops_done.reserve(512);
      std::chrono::milliseconds interval(FLAGS_record_interval);
      auto next_exec_time = std::chrono::steady_clock::now();
      while(true) {
        next_exec_time += interval;
        uint64_t tmp = 0;
        for (int i = 0; i < n; i++) {
          tmp += arg[i].thread->stats.GetFinishedOps();
        }
        ops_done.push_back(tmp);
        std::this_thread::sleep_until(next_exec_time);
        MutexLock l(&shared.mu);
        if (shared.num_done == n) {
          break;
        }
      }
      std::ofstream f(FLAGS_record_dump_path, std::ios::out);
      if (!f.is_open()) {
        std::cerr << "fail to open speed record file, " << FLAGS_record_dump_path << std::endl;
        return;
      }
      for (auto op : ops_done) {
        f << op << std::endl;
      }
      f.close();
    });
    

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);
    if (FLAGS_comparisons) {
      fprintf(stdout, "Comparisons: %zu\n", count_comparator_.comparisons());
      count_comparator_.reset();
      fflush(stdout);
    }

    speed_record_thread.join();

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
    db_->WaitforAllbgtasks(false);
    if (method == &Benchmark::WriteRandom || method == &Benchmark::WriteRandomSharded)
      sleep(3); // wait for the last sstable disgestion.

    if (method == &Benchmark::ReadRandom || method == &Benchmark::ReadRandom_Sharded)
      Validation_Read();
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp();
      bytes += size;
    }
    // Print so result is not dead
    std::fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                    (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                   uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
    assert(db_ == nullptr);
    Options options;
    options.env = g_env;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_file_size = FLAGS_max_file_size;
    options.block_size = FLAGS_block_size;
    options.bloom_bits = FLAGS_bloom_bits;
    
    options.memtable_only = FLAGS_memtable_only;

    options.max_background_flushes = FLAGS_max_background_flushes;
    options.max_background_compactions = FLAGS_max_background_compactions;

    options.block_restart_interval = FLAGS_block_restart_interval;
    if (FLAGS_comparisons) {
      options.comparator = &count_comparator_;
    }
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    options.reuse_logs = FLAGS_reuse_logs;
    //
    rdma_mg = Env::Default()->rdma_mg.get();
    number_of_key_total = FLAGS_num*FLAGS_threads; // whole range.
    number_of_key_per_compute =
        number_of_key_total /rdma_mg->compute_nodes.size();

    if (FLAGS_fixed_compute_shards_num > 0){
      options.ShardInfo = new std::vector<std::pair<Slice,Slice>>();
      number_of_key_per_shard = number_of_key_per_compute
                                /FLAGS_fixed_compute_shards_num;
      for (int i = 0; i < FLAGS_fixed_compute_shards_num; ++i) {
        char* data_low = new char[FLAGS_key_size];
        char* data_up = new char[FLAGS_key_size];
        Slice key_low  = Slice(data_low, FLAGS_key_size);
        Slice key_up  = Slice(data_up, FLAGS_key_size);
        uint64_t lower_bound = number_of_key_per_compute*(rdma_mg->node_id -1)/2
                               + i*number_of_key_per_shard;
        uint64_t upper_bound = number_of_key_per_compute*(rdma_mg->node_id -1)/2
                               + (i+1)*number_of_key_per_shard;
        if (i == FLAGS_fixed_compute_shards_num-1){
          upper_bound = number_of_key_per_compute*(rdma_mg->node_id + 1)/2;
        }
        GenerateKeyFromInt(lower_bound, &key_low);
        GenerateKeyFromInt(upper_bound, &key_up);
        options.ShardInfo->emplace_back(key_low,key_up);
      }
    }else if (rdma_mg->memory_nodes.size()> 1){
      options.ShardInfo = new std::vector<std::pair<Slice,Slice>>();
      number_of_key_per_shard = number_of_key_per_compute
                                /rdma_mg->memory_nodes.size();
      for (int i = 0; i < rdma_mg->memory_nodes.size(); ++i) {
        char* data_low = new char[FLAGS_key_size];
        char* data_up = new char[FLAGS_key_size];
        Slice key_low  = Slice(data_low, FLAGS_key_size);
        Slice key_up  = Slice(data_up, FLAGS_key_size);
        uint64_t lower_bound = number_of_key_per_compute*(rdma_mg->node_id -1)/2
                               + i*number_of_key_per_shard;
        uint64_t upper_bound = number_of_key_per_compute*(rdma_mg->node_id -1)/2
                               + (i+1)*number_of_key_per_shard;
        if (i == rdma_mg->memory_nodes.size()-1){
          // in case that the number_of_key_pershard round down.
          upper_bound = number_of_key_per_compute*(rdma_mg->node_id + 1)/2;
        }
        GenerateKeyFromInt(lower_bound, &key_low);
        GenerateKeyFromInt(upper_bound, &key_up);
        options.ShardInfo->emplace_back(key_low,key_up);
      }




    }

    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      std::fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      std::exit(1);
    }
  }

  void OpenBench(ThreadState* thread) {
    for (int i = 0; i < num_; i++) {
      delete db_;
      Open();
      thread->stats.FinishedSingleOp();
    }
  }

  void WriteSeq(ThreadState* thread) { DoWrite(thread, true); }

  void WriteRandom(ThreadState* thread) { DoWrite(thread, false); }
  void WriteRandomSharded(ThreadState* thread) { DoWrite_Sharded(thread, false); }
  void Validation_Write() {
    Random64 rand(123);
    RandomGenerator gen;
    Status s;
    std::unique_ptr<const char[]> key_guard;
    WriteBatch batch;
    Slice key = AllocateKey(&key_guard, FLAGS_key_size+1);
    for (int i = 0; i < 1000; i++) {
      batch.Clear();
//      //The key range should be adjustable.
////        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
//      const int k = rand.Next()%(FLAGS_num*FLAGS_threads);
      GenerateKeyFromInt(i, &key);
      key.Reset(key.data(), key.size()-1);
      char to_be_append = 'v';// add an extra char to make key different from write bench.
      assert(key.size() == FLAGS_key_size);
      key.append(&to_be_append, 1);
////      batch.Put(key, gen.Generate(value_size_));
//      batch.Put(key, key);

      s = db_->Put(write_options_, key, key);
      validation_keys.push_back(key.ToString());
    }
    printf("validation write finished\n");
  }
  void Validation_Read() {
    ReadOptions options;
    //TODO(ruihong): specify the table_cache option.
    std::string value;
    int not_found = 0;
//    KeyBuffer key;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    for (int i = 0; i < 1000; i++) {
      key = validation_keys[i];
      if (db_->Get(options, key, &value).ok()) {
        if (value != validation_keys[i]){
          printf("The fetched value is not correct");
        }
      }else{
//        printf("Validation failed\n");
        not_found++;
//        assert(false);
      }
    }
    printf("validation read finished, not found num %d\n", not_found);
  }
  void DoWrite(ThreadState* thread, bool seq) {
    if (num_ != FLAGS_num) {
      char msg[100];
      std::snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
//    KeyBuffer key;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        //The key range should be adjustable.
//        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
        const int k = seq ? i + j : thread->rand.Next()%(FLAGS_num*FLAGS_threads);

//        key.Set(k);
        GenerateKeyFromInt(k, &key);
//        batch.Put(key.slice(), gen.Generate(value_size_));
        batch.Put(key, gen.Generate(value_size_));
//        bytes += value_size_ + key.slice().size();
        bytes += value_size_ + key.size();
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        std::exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }
  void DoWrite_Sharded(ThreadState* thread, bool seq) {
    if (num_ != FLAGS_num) {
      char msg[100];
      std::snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    //    KeyBuffer key;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
//    int total_number_of_inserted_key = FLAGS_num*FLAGS_threads;
    uint64_t shard_number_among_computes = (RDMA_Manager::node_id - 1)/2;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        //The key range should be adjustable.
        //        const int k = seq ? i + j : thread->rand.Uniform(FLAGS_num*FLAGS_threads);
        const int k = seq ? i + j : thread->rand.Next()%(number_of_key_per_compute);

        //        key.Set(k);
        GenerateKeyFromInt(
            k + shard_number_among_computes * number_of_key_per_compute, &key);
        //        batch.Put(key.slice(), gen.Generate(value_size_));
        batch.Put(key, gen.Generate(value_size_));
        //        bytes += value_size_ + key.slice().size();
        bytes += value_size_ + key.size();
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        std::exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }
  void ReadSequential(ThreadState* thread) {
#ifdef BYTEADDRESSABLE
    Iterator* iter = db_->NewSEQIterator(ReadOptions());
#endif
#ifndef BYTEADDRESSABLE
    Iterator* iter = db_->NewIterator(ReadOptions());
#endif
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    ReadOptions options;
    //TODO(ruihong): specify the table_cache option.
    std::string value;
    int found = 0;
//    KeyBuffer key;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    for (int i = 0; i < reads_; i++) {
//      const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);// make it uniform as write.
      const int k = thread->rand.Next()%(FLAGS_num*FLAGS_threads);
//
//            key.Set(k);
      GenerateKeyFromInt(k, &key);
//      if (db_->Get(options, key.slice(), &value).ok()) {
//        found++;
//      }
      if (db_->Get(options, key, &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }
  void ReadRandom_Sharded(ThreadState* thread) {
    ReadOptions options;
    //TODO(ruihong): specify the cache option.
    std::string value;
    int found = 0;
    //    KeyBuffer key;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
//    int total_number_of_inserted_key = FLAGS_num*FLAGS_threads;
    uint64_t shard_number_among_computes = (RDMA_Manager::node_id - 1)/2;
    for (int i = 0; i < reads_; i++) {
      //      const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);// make it uniform as write.
      const int k = thread->rand.Next()%(number_of_key_per_compute);
      //
      //            key.Set(k);
      GenerateKeyFromInt(k + shard_number_among_computes * number_of_key_per_compute,
                         &key);
      //      if (db_->Get(options, key.slice(), &value).ok()) {
      //        found++;
      //      }
      if (db_->Get(options, key, &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }
  // This is different from ReadWhileWriting because it does not use
  // an extra thread.
  void ReadRandomWriteRandom(ThreadState* thread) {
    ReadOptions options;
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, FLAGS_num);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    printf("Read percentage is %%%d\n", FLAGS_readwritepercent);
    uint64_t time_elapse;
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
//      DB* db = SelectDB(thread);
GenerateKeyFromInt(thread->rand.Next() % (FLAGS_num * FLAGS_threads), &key);
      if (get_weight == 0 && put_weight == 0) {
        // one batch completed, reinitialize for next batch
        get_weight = FLAGS_readwritepercent;
        put_weight = 100 - get_weight;
      }
      if (get_weight > 0) {
        // do all the gets first
        //TODO: remove the time print.
//        auto start = std::chrono::high_resolution_clock::now();
        Status s = db_->Get(options, key, &value);
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//        time_elapse+=duration.count();

        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
        }
        get_weight--;
        reads_done++;
        thread->stats.FinishedSingleOp();
//        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      } else  if (put_weight > 0) {
        // then do all the corresponding number of puts
        // for all the gets we have done earlier
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        put_weight--;
        writes_done++;
        thread->stats.FinishedSingleOp();
//        thread->stats.FinishedOps(nullptr, db, 1, kWrite);
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( reads:%" PRIu64 " writes:%" PRIu64 \
                               " total:%" PRIu64 " found:%" PRIu64 ")",
             reads_done, writes_done, FLAGS_num, found);
//    printf("Read average latency is %lu\n", time_elapse/reads_done);
    thread->stats.AddMessage(msg);
  }
  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      Slice s = Slice(key.slice().data(), key.slice().size() - 1);
      db_->Get(options, s, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      const int k = thread->rand.Uniform(range);
      key.Set(k);
      db_->Get(options, key.slice(), &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    int found = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      const int k = thread->rand.Uniform(FLAGS_num);
      key.Set(k);
      iter->Seek(key.slice());
      if (iter->Valid() && iter->key() == key.slice()) found++;
      delete iter;
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void SeekOrdered(ThreadState* thread) {
    ReadOptions options;
    Iterator* iter = db_->NewIterator(options);
    int found = 0;
    int k = 0;
    KeyBuffer key;
    for (int i = 0; i < reads_; i++) {
      k = (k + (thread->rand.Uniform(100))) % FLAGS_num;
      key.Set(k);
      iter->Seek(key.slice());
      if (iter->Valid() && iter->key() == key.slice()) found++;
      thread->stats.FinishedSingleOp();
    }
    delete iter;
    char msg[100];
    std::snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    KeyBuffer key;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i + j : (thread->rand.Uniform(FLAGS_num));
        key.Set(k);
        batch.Delete(key.slice());
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        std::fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        std::exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) { DoDelete(thread, true); }

  void DeleteRandom(ThreadState* thread) { DoDelete(thread, false); }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      KeyBuffer key;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Uniform(FLAGS_num*FLAGS_threads);
        key.Set(k);
        Status s =
            db_->Put(write_options_, key.slice(), gen.Generate(value_size_));
        if (!s.ok()) {
          std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          std::exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) { db_->CompactRange(nullptr, nullptr); }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    std::fprintf(stdout, "\n%s\n", stats.c_str());
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    std::snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db.c_str(),
                  ++heap_counter_);
    WritableFile* file;
    Status s = g_env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      std::fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      std::fprintf(stderr, "heap profiling not supported\n");
      g_env->RemoveFile(fname);
    }
  }
};

}  // namespace dLSM

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  dLSM::RDMA_Manager::node_id = 2*FLAGS_compute_node_id+1;

  FLAGS_write_buffer_size = dLSM::Options().write_buffer_size;
  FLAGS_max_file_size = dLSM::Options().max_file_size;
  FLAGS_block_size = dLSM::Options().block_size;
  FLAGS_open_files = dLSM::Options().max_open_files;
  std::string default_db_path;
  dLSM::g_env = dLSM::Env::Default();

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    dLSM::g_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path.c_str();
  }

  dLSM::Benchmark benchmark;
  benchmark.Run();

  gflags::ShutDownCommandLineFlags();
  return 0;
}
