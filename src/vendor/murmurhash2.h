/* Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include <stdint.h>

// NOLINTBEGIN

/* MurmurHash2, 64 bit version.
 * It was modified for Redis in order to provide the same result in
 * big and little endian archs (endian neutral). */
inline uint64_t HllMurMurHash64A(const void *key, int len, uint32_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = seed ^ (len * m);
  const auto *data = (const uint8_t *)key;
  const uint8_t *end = data + (len - (len & 7));

  while (data != end) {
    uint64_t k = 0;

#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    memcpy(&k, data, sizeof(uint64_t));
#else
    k = (uint64_t)data[0];
    k |= (uint64_t)data[1] << 8;
    k |= (uint64_t)data[2] << 16;
    k |= (uint64_t)data[3] << 24;
    k |= (uint64_t)data[4] << 32;
    k |= (uint64_t)data[5] << 40;
    k |= (uint64_t)data[6] << 48;
    k |= (uint64_t)data[7] << 56;
#endif

    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    data += 8;
  }

  switch (len & 7) {
    case 7:
      h ^= (uint64_t)data[6] << 48; /* fall-thru */
    case 6:
      h ^= (uint64_t)data[5] << 40; /* fall-thru */
    case 5:
      h ^= (uint64_t)data[4] << 32; /* fall-thru */
    case 4:
      h ^= (uint64_t)data[3] << 24; /* fall-thru */
    case 3:
      h ^= (uint64_t)data[2] << 16; /* fall-thru */
    case 2:
      h ^= (uint64_t)data[1] << 8; /* fall-thru */
    case 1:
      h ^= (uint64_t)data[0];
      h *= m; /* fall-thru */
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

// NOLINTEND
