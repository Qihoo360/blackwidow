//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <vector>
#include <thread>
#include <functional>

#include "blackwidow/blackwidow.h"

#define KEYLENGTH 1024 * 10
#define VALUELENGTH 1024 * 10
#define THREADNUM 20

static const std::string key(KEYLENGTH, 'a'); 
static const std::string value(VALUELENGTH, 'a');

void BenchSet(int kv_num) {
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (s.ok()) {
    printf("Open success\n"); 
  } else {
    printf("Open failed, error: %s\n", s.ToString().c_str()); 
    return;
  }

  std::vector<std::thread> jobs;
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t kv_num) {
      for (size_t j = 0; j < kv_num; ++j) {
      db.Set(key, value); 
      }
    }, kv_num);
  }

  for (auto& job : jobs) {
    job.join(); 
  }
}

int main(int argc, char** argv) {
  auto start = std::chrono::system_clock::now();
  BenchSet(10000);
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> elapsed_seconds = end-start;
  auto cost = std::chrono::duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Set " << THREADNUM * 10000 << " Cost: "<< cost << "s QPS: " << (THREADNUM *10000)/cost << std::endl;

  start = std::chrono::system_clock::now();
  BenchSet(100000);
  end = std::chrono::system_clock::now();
  elapsed_seconds = end-start;
  cost = std::chrono::duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Set " << THREADNUM * 100000 << " Cost: "<< cost << "s QPS: " << (THREADNUM *100000)/cost << std::endl;
}
