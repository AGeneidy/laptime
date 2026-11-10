//
// Created by Ayman Geneidy on 01/11/2026.
//

#ifndef LAPTIME_SINKS_SINK_H_
#define LAPTIME_SINKS_SINK_H_

#include <string_view>

#include "common/message_times.h"

namespace laptime {

class Sink {
 public:
  Sink() = default;
  Sink(Sink&&) = delete;
  Sink(const Sink&) = delete;
  Sink& operator=(const Sink&) = delete;

  virtual ~Sink() = default;

  virtual void Publish(std::string_view, std::string_view,
                       const MessageTimes&) = 0;
  virtual void Flush() = 0;
};

}  // namespace laptime

#endif  // LAPTIME_SINKS_SINK_H_
