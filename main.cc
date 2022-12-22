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
#include "pthread-helper.h"

#include <duckdb.hpp>

#include <dirent.h>
#include <errno.h>
#include <getopt.h>
#include <stddef.h>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <vector>

namespace c2 {

std::string ToSql(const std::string& filename, const char* filter) {
  char tmp[500];
  snprintf(tmp, sizeof(tmp), "SELECT * FROM '%s' WHERE %s", filename.c_str(),
           filter);
  return tmp;
}

int RunQuery(const std::string& filename, const char* filter) {
  int nrows = 0;
  duckdb::DBConfig conf;
  duckdb::DuckDB db(nullptr, &conf);
  duckdb::Connection con(db);
  std::unique_ptr<duckdb::QueryResult> r =
      con.SendQuery(ToSql(filename, filter));
  std::unique_ptr<duckdb::DataChunk> d = r->FetchRaw();
  while (d) {
    nrows += int(d->size());
    d = r->FetchRaw();
  }
  return nrows;
}

class QueryRunner {
 public:
  QueryRunner(const char* query_filter, int max_jobs);
  ~QueryRunner();
  int total() const { return nrows_; }
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
  int n = 0;
  try {
    n = RunQuery(t->input_file, t->filter);
  } catch (const std::exception& e) {
    fprintf(stderr, "Error running query: %s\n", e.what());
  }
  QueryRunner* const me = t->me;
  {
    MutexLock ml(&me->mu_);
    printf("scan::%s[%s] done!\n", t->input_file.c_str(), t->filter);
    me->bg_completed_++;
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

void process_dir(const char* datadir, const char* filter, int j) {
  c2::QueryRunner runner(filter, j);
  DIR* const dir = opendir(datadir);
  if (!dir) {
    fprintf(stderr, "Fail to open data dir %s: %s\n", datadir, strerror(errno));
    exit(EXIT_FAILURE);
  }
  std::string scratch = datadir;
  size_t scratch_prefix = scratch.length();
  struct dirent* entry = readdir(dir);
  while (entry) {
    if (entry->d_type == DT_REG) {
      scratch.resize(scratch_prefix);
      scratch += "/";
      scratch += entry->d_name;
      runner.AddTask(scratch);
    }
    entry = readdir(dir);
  }
  closedir(dir);
  runner.Wait();
  fprintf(stderr, "Query Predicate: %s\n", filter);
  fprintf(stderr, "Total rows: %d\n", runner.total());
  fprintf(stderr, "Done\n");
}

int main(int argc, char* argv[]) {
  char tmp[50];
  snprintf(tmp, sizeof(tmp), "ke > %s", argv[2]);
  process_dir(argv[1], tmp, 32);
  return 0;
}
