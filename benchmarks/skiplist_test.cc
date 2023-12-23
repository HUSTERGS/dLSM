#include <atomic>
#include <memory>
#include <pthread.h>
#include <sched.h>
#include <vector>
#include <string>
#include <map>
#include <gflags/gflags.h>

#include "db/dbformat.h"
#include "util/mutexlock.h"
#include "util/testutil.h"
#include "util/random.h"
#include "db/memtable.h"
#include "include/dLSM/comparator.h"

using namespace dLSM;

DEFINE_int32(num, 100, "entry num");
DEFINE_int32(threads, 1, "threads");
DEFINE_int32(key_size, 20, "key size");
DEFINE_int32(value_size, 400, "value size");
DEFINE_bool(table_per_thread, false, "each thread insert into single memtable");
DEFINE_bool(seq_write, true, "if it is seq write");
DEFINE_bool(fake_run, false, "not actually insert");
DEFINE_bool(bind_cpu, true, "bind cpu");
DEFINE_string(db, "skiplist", "db, now support skiplist and c++ map");

void print_parameters() {
  std::cout << "run " << FLAGS_db << " with parameters: \n"
               "entry num:        \t" << FLAGS_num                << std::endl << 
               "threads:          \t" << FLAGS_threads            << std::endl <<
               "key size:         \t" << FLAGS_key_size           << std::endl <<
               "value size:       \t" << FLAGS_value_size         << std::endl <<
               "table per thread: \t" << FLAGS_table_per_thread   << std::endl <<
               "seq write:        \t" << FLAGS_seq_write          << std::endl <<
               "fake rune:        \t" << FLAGS_fake_run           << std::endl <<
               "bind cpu:         \t" << FLAGS_bind_cpu           << std::endl;
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
      test::CompressibleString(&rnd, 0.5, 100, &piece);
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

class WriteOnlyDb {
public:
    virtual void Put(const Slice& key, const Slice& value) = 0;
};

class MemTableWrapper : public WriteOnlyDb {
public:
    MemTableWrapper(MemTable *m) : table_(m) {}
    void Put(const Slice& key, const Slice& value) override {
        table_->Add(0, dLSM::kTypeValue, key, value);
    }
private:
    std::shared_ptr<MemTable> table_;
};

class CppMapWrapper : public WriteOnlyDb {
public:
    void Put(const Slice& key, const Slice& value) override {
        SpinLock lock(&mu_);
        m.insert({key.ToString(), value.ToString()});
    }
private:
    SpinMutex mu_;
    std::map<std::string, std::string> m;
};


class KvSperateMemTable : public WriteOnlyDb {
public:
    static const size_t INIT_BUFF_SIZE = 1024 * 1024 * 128;
    KvSperateMemTable(MemTable* m) : table_(m), buff_(new char[INIT_BUFF_SIZE]) {}
    void Put(const Slice& key, const Slice& value) override {
        auto loc = offset_.fetch_add(value.size(), std::memory_order_relaxed);
        std::memcpy(buff_.get() + loc, value.data(), value.size());
        uint64_t vs[2] = {loc, value.size()};
        Slice v_ptr(reinterpret_cast<char *>(vs), sizeof(vs));
        table_->Add(0, dLSM::kTypeValue, key, v_ptr);
    }
private:
    std::atomic<uint64_t> offset_;
    std::shared_ptr<MemTable> table_;
    std::unique_ptr<char []> buff_;
};

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::vector<std::thread> ts;
    
    Env* g_env = Env::Default();
    
    
    int nums_per_thread = FLAGS_num / FLAGS_threads;
    RandomGenerator gen;
    size_t table_num = FLAGS_table_per_thread ? FLAGS_threads : 1;
    std::vector<std::shared_ptr<WriteOnlyDb>> dbs;
    for (size_t i = 0; i < table_num; i++) {
      if (FLAGS_db == "skiplist") {
        dbs.emplace_back(std::make_shared<MemTableWrapper>(new MemTable(InternalKeyComparator(BytewiseComparator()))));
      } else if (FLAGS_db == "cppmap") {
        dbs.emplace_back(std::make_shared<CppMapWrapper>());
      } else if (FLAGS_db == "kvs_skiplist") {
        dbs.emplace_back(std::make_shared<KvSperateMemTable>(new MemTable(InternalKeyComparator(BytewiseComparator()))));
      } else {
        std::cerr << "unknown db : " << FLAGS_db << std::endl;
        exit(1);
      }
    }

    std::cout << "\n\n\nbenchmark start..." << std::endl;
    std::cout << "nums per threads : " << nums_per_thread << std::endl;

    double start = g_env->NowMicros();
    for (int tn = 0; tn < FLAGS_threads; tn++) {
        ts.emplace_back([&, tn]() {
            Random64 rand_gen(tn);
            std::unique_ptr<const char[]> key_guard;
            Slice key = AllocateKey(&key_guard);
            auto db = FLAGS_table_per_thread ? dbs[tn] : dbs.front();

            for (int i = 0; i < nums_per_thread; i++) {
                const int k = FLAGS_seq_write ? i : rand_gen.Next() % FLAGS_num;    
                GenerateKeyFromInt(k, &key);
                if (FLAGS_fake_run) {
                  (void) key;
                } else {
                  db->Put(key, gen.Generate(FLAGS_value_size));
                }
            }
        });
    }

    if (FLAGS_bind_cpu) {
      int num_cores = std::thread::hardware_concurrency();
      for (size_t i = 0; i < ts.size(); i++) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i % num_cores, &cpuset);
        pthread_setaffinity_np(ts[i].native_handle(), sizeof(cpu_set_t), &cpuset);
      }
    }

    for (auto &t : ts) {
        t.join();
    }

    double elapsed = (g_env->NowMicros() - start) * 1e-6;
    char rate[100];
    uint64_t bytes = 1ull * (FLAGS_key_size + FLAGS_value_size) * FLAGS_num;
    std::cout << "bytes : " << bytes << std::endl;
    std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
                    (bytes / 1048576.0) / elapsed);
    
    std::cout << std::endl << FLAGS_num << " entry takes " << elapsed << " seconds to insert, " << std::string(rate) << std::endl;
    
    print_parameters();
    gflags::ShutDownCommandLineFlags();
    return 0;
}