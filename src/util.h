//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_UTIL_H_
#define SRC_UTIL_H_

#include <string>
#include <cstring>
#include <cmath>

namespace blackwidow {

  uint32_t Digits10(uint64_t v);
  int Int64ToStr(char* dst, size_t dstlen, int64_t svalue);
  int StrToInt64(const char* s, size_t slen, int64_t* value);

  int StrToLongDouble(const char* s, size_t slen, long double* ldval);
  int LongDoubleToStr(long double ldval, std::string* value);
}

#endif  //  SRC_REDIS_HASHES_H_
