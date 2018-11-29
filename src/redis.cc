//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"

namespace blackwidow {

Redis::Redis(BlackWidow* const bw, const DataType& type)
    : bw_(bw),
      type_(type),
      lock_mgr_(new LockMgr(1000, 0, std::make_shared<MutexFactoryImpl>())),
      db_(nullptr),
      small_compaction_threshold_(5000) {
  statistics_store_.max_size_ = 0;
  scan_cursors_store_.max_size_ = 5000;
  default_compact_range_options_.exclusive_manual_compaction = false;
  default_compact_range_options_.change_level = true;
}

Redis::~Redis() {
  delete db_;
  delete lock_mgr_;
}

Status Redis::GetScanStartPoint(const Slice& key,
                                const Slice& pattern,
                                int64_t cursor,
                                std::string* start_point) {
  slash::MutexLock l(&scan_cursors_mutex_);
  std::string index_key =
    key.ToString() + "_" + pattern.ToString() + "_" + std::to_string(cursor);
  if (scan_cursors_store_.map_.find(index_key)
    == scan_cursors_store_.map_.end()) {
    return Status::NotFound();
  } else {
    *start_point = scan_cursors_store_.map_[index_key];
  }
  return Status::OK();
}

Status Redis::StoreScanNextPoint(const Slice& key,
                                 const Slice& pattern,
                                 int64_t cursor,
                                 const std::string& next_point) {
  slash::MutexLock l(&scan_cursors_mutex_);
  std::string index_key =
    key.ToString() + "_" + pattern.ToString() +  "_" + std::to_string(cursor);
  if (scan_cursors_store_.list_.size() > scan_cursors_store_.max_size_) {
    std::string tail = scan_cursors_store_.list_.back();
    scan_cursors_store_.map_.erase(tail);
    scan_cursors_store_.list_.pop_back();
  }

  scan_cursors_store_.map_[index_key] = next_point;
  scan_cursors_store_.list_.remove(index_key);
  scan_cursors_store_.list_.push_front(index_key);
  return Status::OK();
}

Status Redis::SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys) {
  slash::MutexLock l(&statistics_mutex_);
  statistics_store_.max_size_ = max_cache_statistic_keys;
  while (statistics_store_.list_.size() > statistics_store_.max_size_) {
    std::string tail = statistics_store_.list_.back();
    statistics_store_.map_.erase(tail);
    statistics_store_.list_.pop_back();
  }
  return Status::OK();
}

Status Redis::SetSmallCompactionThreshold(size_t small_compaction_threshold) {
  slash::MutexLock l(&statistics_mutex_);
  small_compaction_threshold_ = small_compaction_threshold;
  return Status::OK();
}

Status Redis::UpdateSpecificKeyStatistics(const std::string& key,
                                          uint32_t count) {
  uint32_t total = 0;
  if (statistics_store_.max_size_ == 0) {
    return Status::OK();
  }

  slash::MutexLock l(&statistics_mutex_);
  if (statistics_store_.map_.find(key) == statistics_store_.map_.end()) {
    statistics_store_.map_[key] = count;
    statistics_store_.list_.push_front(key);
  } else {
    statistics_store_.map_[key] += count;
    statistics_store_.list_.remove(key);
    statistics_store_.list_.push_front(key);
  }
  total = statistics_store_.map_[key];

  if (statistics_store_.list_.size() > statistics_store_.max_size_) {
    std::string tail = statistics_store_.list_.back();
    statistics_store_.map_.erase(tail);
    statistics_store_.list_.pop_back();
  }
  AddCompactKeyTaskIfNeeded(key, total);
  return Status::OK();
}

Status Redis::AddCompactKeyTaskIfNeeded(const std::string& key,
                                        uint32_t total) {
  if (total < small_compaction_threshold_) {
    return Status::OK();
  } else {
    bw_->AddBGTask({type_, kCompactKey, key});
    statistics_store_.map_.erase(key);
    statistics_store_.list_.remove(key);
  }
  return Status::OK();
}

}  // namespace blackwidow
