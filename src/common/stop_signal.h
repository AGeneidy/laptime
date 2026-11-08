//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_COMMON_STOP_SIGNAL_H_
#define LAPTIME_COMMON_STOP_SIGNAL_H_

namespace laptime {

// Makes Ctrl+C (SIGINT) and `kill` (SIGTERM) request a clean stop instead of
// killing the process. A second signal kills it the normal way.
void InstallStopSignalHandlers();

// True once a stop signal has arrived.
bool StopRequested();

}  // namespace laptime

#endif  // LAPTIME_COMMON_STOP_SIGNAL_H_
