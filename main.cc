/*
 * Copyright (c) 2021 Triad National Security, LLC, as operator of Los Alamos
 * National Laboratory with the U.S. Department of Energy/National Nuclear
 * Security Administration. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "iostats.h"
#include "pthread-helper.h"
#include "time.h"

#include <duckdb.hpp>
#include <duckdb/common/local_file_system.hpp>
#include <duckdb/common/serializer/buffered_serializer.hpp>
#include <duckdb/common/string_util.hpp>

#include <dirent.h>
#include <errno.h>
#include <getopt.h>
#include <map>
#include <stddef.h>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace c2 {

struct ReadStats {
  ReadStats() : read_ops(0), read_bytes(0) {}
  // Number of read operations
  uint64_t read_ops;
  // Number of bytes read
  uint64_t read_bytes;
};

class MonitoredFileHandle : public duckdb::FileHandle {
 public:
  std::unique_ptr<duckdb::FileHandle> base_;
  ReadStats* stats_;

  MonitoredFileHandle(duckdb::FileSystem& file_system, const std::string& path,
                      std::unique_ptr<duckdb::FileHandle> base,
                      ReadStats* stats)
      : FileHandle(file_system, path), base_(std::move(base)), stats_(stats) {}

  ~MonitoredFileHandle() override { Close(); }

  void Close() override {
    if (base_) {
      base_->Close();
    }
  };
};

class MonitoredFileSystem : public duckdb::FileSystem {
 public:
  explicit MonitoredFileSystem(std::unique_ptr<duckdb::FileSystem> base)
      : base_(std::move(base)) {}

  std::unique_ptr<duckdb::FileHandle> OpenFile(
      const std::string& path, uint8_t flags, duckdb::FileLockType lock,
      duckdb::FileCompressionType compression,
      duckdb::FileOpener* opener) override {
    std::unique_ptr<duckdb::FileHandle> r =
        base_->OpenFile(path, flags, lock, compression, opener);
    ReadStats* const mystats = new ReadStats();
    stats_.push_back(mystats);
    return std::make_unique<MonitoredFileHandle>(*this, path, std::move(r),
                                                 mystats);
  }

  int64_t GetFileSize(duckdb::FileHandle& handle) override {
    return base_->GetFileSize(
        *dynamic_cast<MonitoredFileHandle&>(handle).base_);
  }

  void Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
            idx_t location) override {
    MonitoredFileHandle* h = &dynamic_cast<MonitoredFileHandle&>(handle);
    base_->Read(*h->base_, buffer, nr_bytes, location);
    h->stats_->read_bytes += nr_bytes;
    h->stats_->read_ops += 1;
  }

  time_t GetLastModifiedTime(duckdb::FileHandle& handle) override {
    return base_->GetLastModifiedTime(handle);
  }

  std::vector<std::string> Glob(const std::string& path,
                                duckdb::FileOpener* opener) override {
    return base_->Glob(path, opener);
  }

  void RegisterSubSystem(duckdb::FileCompressionType compression_type,
                         std::unique_ptr<FileSystem> sub_fs) override {
    // Ignore
  }

  bool CanSeek() override { return base_->CanSeek(); }

  bool OnDiskFile(duckdb::FileHandle& handle) override {
    return base_->OnDiskFile(*dynamic_cast<MonitoredFileHandle&>(handle).base_);
  }

  std::string GetName() const override { return "MonitoredFileSystem"; }

  uint64_t GetTotalReadOps() const {
    uint64_t ops = 0;
    for (ReadStats* it : stats_) {
      ops += it->read_ops;
    }
    return ops;
  }

  uint64_t GetTotalReadBytes() const {
    uint64_t bytes = 0;
    for (ReadStats* it : stats_) {
      bytes += it->read_bytes;
    }
    return bytes;
  }

  ~MonitoredFileSystem() override {
    for (ReadStats* it : stats_) {
      delete it;
    }
  }

 private:
  std::vector<ReadStats*> stats_;
  std::unique_ptr<duckdb::FileSystem> base_;
};

std::string ToSql(const std::string& filename, const char* filter) {
  char tmp[500];
  snprintf(tmp, sizeof(tmp), "SELECT * FROM '%s' WHERE %s", filename.c_str(),
           filter);
  return tmp;
}

int RunQuery(ReadStats* stats, const std::string& filename, const char* filter,
             int print = 1, int print_binary = 1) {
  int nrows = 0;
  duckdb::DBConfig conf;
  conf.file_system = std::make_unique<MonitoredFileSystem>(
      std::make_unique<duckdb::LocalFileSystem>());
  conf.maximum_threads = 1;
  duckdb::DuckDB db(nullptr, &conf);
  duckdb::Connection con(db);
  duckdb::BufferedSerializer ser(1024 * 1024);
  std::unique_ptr<duckdb::QueryResult> r =
      con.SendQuery(ToSql(filename, filter));
  std::unique_ptr<duckdb::DataChunk> d = r->FetchRaw();
  while (d) {
    if (print) {
      if (print_binary) {
        ser.Reset();
        for (idx_t i = 0; i < d->ColumnCount(); i++) {
          d->data[i].Serialize(d->size(), ser);
        }
        fwrite(ser.blob.data.get(), ser.blob.size, 1, stdout);
        fflush(stdout);
      } else {
        d->Print();
      }
    }
    nrows += int(d->size());
    d = r->FetchRaw();
  }
  MonitoredFileSystem* fs =
      &dynamic_cast<MonitoredFileSystem&>(db.GetFileSystem());
  stats->read_bytes = fs->GetTotalReadBytes();
  stats->read_ops = fs->GetTotalReadOps();
  return nrows;
}

class QueryRunner {
 public:
  QueryRunner(const char* query_filter, int max_jobs);
  ~QueryRunner();
  uint64_t TotalReadOps() const { return stats_.read_ops; }
  uint64_t TotalReadBytes() const { return stats_.read_bytes; }
  int TotalRows() const { return nrows_; }
  void AddTask(const std::string& input_file);
  void Wait();

 private:
  struct Task {
    QueryRunner* me;
    std::string input_file;
    const char* filter;
  };
  static void RunTask(void*);
  QueryRunner(const QueryRunner&);
  void operator=(const QueryRunner& other);
  const char* const query_filter_;
  ThreadPool* const pool_;
  // State below protected by cv_;
  ReadStats stats_;
  int nrows_;  // Total number of rows returned
  port::Mutex mu_;
  port::CondVar cv_;
  int bg_scheduled_;
  int bg_completed_;
};

QueryRunner::QueryRunner(const char* query_filter, int max_jobs)
    : query_filter_(query_filter),
      pool_(new ThreadPool(max_jobs)),
      nrows_(0),
      cv_(&mu_),
      bg_scheduled_(0),
      bg_completed_(0) {}

void QueryRunner::Wait() {
  MutexLock ml(&mu_);
  while (bg_completed_ < bg_scheduled_) {
    cv_.Wait();
  }
}

void QueryRunner::AddTask(const std::string& input_file) {
  Task* const t = new Task;
  t->me = this;
  t->input_file = input_file;
  t->filter = query_filter_;
  MutexLock ml(&mu_);
  bg_scheduled_++;
  printf("Scheduling scan::%s[%s]...\n", input_file.c_str(), query_filter_);
  pool_->Schedule(RunTask, t);
}

void QueryRunner::RunTask(void* arg) {
  Task* const t = static_cast<Task*>(arg);
  ReadStats stats;
  int n = 0;
  try {
    n = RunQuery(&stats, t->input_file, t->filter);
  } catch (const std::exception& e) {
    fprintf(stderr, "Error running query: %s\n", e.what());
  }
  QueryRunner* const me = t->me;
  {
    MutexLock ml(&me->mu_);
    printf("scan::%s[%s] done!\n", t->input_file.c_str(), t->filter);
    me->bg_completed_++;
    me->stats_.read_bytes += stats.read_bytes;
    me->stats_.read_ops += stats.read_ops;
    me->nrows_ += n;
    me->cv_.SignalAll();
  }
  delete t;
}

QueryRunner::~QueryRunner() {
  {
    MutexLock ml(&mu_);
    while (bg_completed_ < bg_scheduled_) {
      cv_.Wait();
    }
  }
  delete pool_;
}

}  // namespace c2

void process_dir(char** datadir, int c, const char* filter, int j) {
  c2::QueryRunner runner(filter, j);
  std::vector<std::string> files;
  for (int i = 0; i < c; i++) {
    std::string scratch = datadir[i];
    const size_t scratch_prefix = scratch.length();
    DIR* const dir = opendir(scratch.c_str());
    if (!dir) {
      fprintf(stderr, "Fail to open data dir %s: %s\n", datadir,
              strerror(errno));
      exit(EXIT_FAILURE);
    }
    struct dirent* entry = readdir(dir);
    while (entry) {
      if (entry->d_type == DT_REG) {
        scratch.resize(scratch_prefix);
        scratch += "/";
        scratch += entry->d_name;
        files.push_back(scratch);
      }
      entry = readdir(dir);
    }
    closedir(dir);
  }
  const uint64_t start = CurrentMicros();
  for (const std::string& file : files) {
    runner.AddTask(file);
  }
  runner.Wait();
  const uint64_t end = CurrentMicros();
  fprintf(stderr, "Predicate: %s\n", filter);
  fprintf(stderr, "Threads: %d\n", j);
  fprintf(stderr, "Query time: %.2f s\n", double(end - start) / 1000000);
  fprintf(stderr, "Total rows: %d\n", runner.TotalRows());
  fprintf(stderr, "Total read ops: %lld\n",
          static_cast<long long unsigned>(runner.TotalReadOps()));
  fprintf(stderr, "Total read bytes: %lld\n",
          static_cast<long long unsigned>(runner.TotalReadBytes()));
  fprintf(stderr, "Done\n");
}

void collect_and_report(
    const std::map<std::string, struct iostats>& diskstats) {
  if (!diskstats.empty()) {
    long long total_ops = 0, total_sectors = 0, total_ticks = 0;
    for (const auto& it : diskstats) {
      long long diff = 0;
      char path[50];
      snprintf(path, sizeof(path), "/sys/block/%s/stat", it.first.c_str());
      struct iostats stats;
      memset(&stats, 0, sizeof(stats));
      GetDiskStats(path, &stats);
      diff = stats.read_ops - it.second.read_ops;
      fprintf(stderr, "%s_read_ops: %lld\n", path, diff);
      total_ops += diff;
      diff = stats.read_sectors - it.second.read_sectors;
      fprintf(stderr, "%s_read_sectors: %lld\n", path, diff);
      total_sectors += diff;
      diff = stats.read_ticks - it.second.read_ticks;
      fprintf(stderr, "%s_read_ticks: %lld ms\n", path, diff);
      total_ticks += diff;
    }
    fprintf(stderr, "Total_read_ops: %lld\n", total_ops);
    fprintf(stderr, "Total_read_sectors: %lld\n", total_sectors);
    fprintf(stderr, "Total_read_ticks: %lld ms\n", total_ticks);
  }
}

/*
 * Usage: ./duckdb-runner data_dir
 */
int main(int argc, char* argv[]) {
  const char* ke = "0.5";
  {
    const char* env = getenv("Env_ke");
    if (env && env[0]) {
      ke = env;
    }
  }
  int j = 32;
  {
    const char* env = getenv("Env_jobs");
    if (env && env[0]) {
      j = atoi(env);
      if (j < 1) {
        j = 1;
      }
    }
  }
  std::map<std::string, struct iostats> diskstats;
  {
    const char* env = getenv("Env_mon_disks");
    if (env && env[0]) {
#ifdef __linux__
      std::vector<std::string> disks = duckdb::StringUtil::Split(env, ",");
      for (const auto& disk : disks) {
        char path[50];
        snprintf(path, sizeof(path), "/sys/block/%s/stat", disk.c_str());
        struct iostats stats;
        memset(&stats, 0, sizeof(stats));
        GetDiskStats(path, &stats);
        diskstats.emplace(disk, stats);
      }
#else
      fprintf(stderr, "WARN: DISK STATS MON NOT ENABLED\n");
#endif
    }
  }
  char tmp[50];
  snprintf(tmp, sizeof(tmp), "ke > %s", ke);
  process_dir(&argv[1], argc - 1, tmp, j);
  collect_and_report(diskstats);
  return 0;
}
