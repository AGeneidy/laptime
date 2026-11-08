//
// Created by Ayman Geneidy on 06/11/2026.
//

#include "common/stop_signal.h"

#include <atomic>
#include <csignal>

namespace laptime {

namespace {

// Set by the signal handler. A handler may only touch lock-free atomics;
// anything else (printing, locking, allocating) is unsafe inside it.
std::atomic<bool> stop_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free);

void RequestStop(const int signal) {
  stop_requested = true;
  std::signal(signal, SIG_DFL);  // a second Ctrl+C quits immediately
}

}  // namespace

void InstallStopSignalHandlers() {
  std::signal(SIGINT, RequestStop);   // Ctrl+C, and CLion's Stop button
  std::signal(SIGTERM, RequestStop);  // `kill <pid>`, `docker stop`
}

bool StopRequested() { return stop_requested; }

}  // namespace laptime
