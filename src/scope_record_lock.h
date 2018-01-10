//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SCOPE_RECORD_LOCK_H_
#define SRC_SCOPE_RECORD_LOCK_H_

#include "src/lock_mgr.h"

namespace blackwidow {
class ScopeRecordLock {
 public:
  ScopeRecordLock(LockMgr* lock_mgr, const Slice& key) :
    lock_mgr_(lock_mgr), key_(key) {
    lock_mgr_->TryLock(key_.ToString());
  }
  ~ScopeRecordLock() {
    lock_mgr_->UnLock(key_.ToString());
  }
 private:
  LockMgr* const lock_mgr_;
  Slice key_;
  ScopeRecordLock(const ScopeRecordLock&);
  void operator=(const ScopeRecordLock&);
};

}  // namespace blackwidow
#endif  // SRC_SCOPE_RECORD_LOCK_H_
