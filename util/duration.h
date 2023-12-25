#include <chrono>
#include <atomic>
#include <vector>
#include <iostream>
#include <fstream>

#include "util/mutexlock.h"

using namespace dLSM;

struct RecordDuration {
    enum DurationType : int {
        FlushDuration, // time spent on flush (memtable to L0)
        L0StopDuration, //  write stall due to too much imm OR L0 sst
        L0SlowDuration, // write stall duo to too much L0 sst
        MNCompactionDuration, // compaction process on mn
        UNDEFINED,
    };
    
    struct Clock {
      Clock() {}
      Clock(bool boot) {
        if (boot)
          start();
      }
      uint64_t start_;
      void start() {
        start_ = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
      }
      std::pair<uint64_t, uint64_t> end() {
        return std::make_pair(start_, std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count());
      }
    };

    // 基本逻辑是，记录micro sec级别的数据，可以与ops那个地方对齐
    SpinMutex mutex;
    std::atomic<uint64_t> start_time{UINT64_MAX};
    // <start_time, end_time>
    std::vector<std::pair<uint64_t, uint64_t>> data;
    DurationType type;
    int level;
    void add(uint64_t start_time, uint64_t end_time) {
        SpinLock lock(&mutex);
        data.push_back(std::make_pair(start_time, end_time));
    }
    
    void add(std::pair<uint64_t, uint64_t> pair) {
      data.push_back(pair);
    }

    static std::string type_to_str(DurationType type, int level = 0) {
      if (type == FlushDuration) {
        return "FlushDuration";
      } else if (type == L0StopDuration) {
        return "L0StopDuration";
      } else if (type == L0SlowDuration) {
        return "L0SlowDuration";
      } else if (type == MNCompactionDuration) {
        return "MNCompactionDuration_" + std::to_string(level);
      } else {
        return "undefined";
      }
    }

    RecordDuration() : type(UNDEFINED) {}
    RecordDuration(DurationType type) : type(type) {}
    RecordDuration(DurationType type, int level) : type(type), level(level) {}

    ~RecordDuration() {
        to_file();
    }

    void to_file() {
        std::string dump_path = type_to_str(type, level) + ".txt";
        std::ofstream f(dump_path, std::ios::out);
        if (!f.is_open()) {
            std::cerr << "fail to open speed record file, " << dump_path << std::endl;
            return;
        }
        
        for (auto &p : data) {
            f << p.first << "," << p.second << std::endl;
        }
        f.close();
    }
};