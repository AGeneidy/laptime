//
// Created by Ayman Geneidy on 08/11/2026.
//

#ifndef LAPTIME_COMMON_CLOCK_H_
#define LAPTIME_COMMON_CLOCK_H_

#include <chrono>
#include <cstdint>

namespace laptime {

// Monotonic Clock.
inline int64_t SteadyNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

}  // namespace laptime

#endif  // LAPTIME_COMMON_CLOCK_H_
