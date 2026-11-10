//
// Created by Ayman Geneidy on 08/11/2026.
//

#ifndef LAPTIME_COMMON_CLOCK_H_
#define LAPTIME_COMMON_CLOCK_H_

#include <chrono>
#include <cstdint>

namespace laptime {


inline int64_t ToNanos(const std::chrono::steady_clock::time_point time) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             time.time_since_epoch())
      .count();
}

// Monotonic Clock.
inline int64_t SteadyNanos() {
  return ToNanos(std::chrono::steady_clock::now());
}

}  // namespace laptime

#endif  // LAPTIME_COMMON_CLOCK_H_
