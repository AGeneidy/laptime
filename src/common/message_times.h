//
// Created by Ayman Geneidy on 10/11/2026.
//

#ifndef LAPTIME_COMMON_MESSAGE_TIMES_H_
#define LAPTIME_COMMON_MESSAGE_TIMES_H_

#include <cstdint>

namespace laptime {

// When one message passed each point, in steady_clock nanoseconds (the same
// clock as SteadyNanos). 0 means the source could not tell.
struct MessageTimes {
  int64_t wake_ns{};   // IXWebSocket: the socket had data
  int64_t read_ns{};   // IXWebSocket: the read and TLS decryption finished
  int64_t start_ns{};  // Stream::OnMessage started (t0)
};

}  // namespace laptime

#endif  // LAPTIME_COMMON_MESSAGE_TIMES_H_
