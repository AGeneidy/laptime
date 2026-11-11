//
// Created by Ayman Geneidy on 11/11/2026.
//

#ifndef LAPTIME_SINKS_DISCARD_DISCARD_SINK_H_
#define LAPTIME_SINKS_DISCARD_DISCARD_SINK_H_

#include "sinks/sink.h"

namespace laptime {

class DiscardSink final : public Sink {
 public:
  void Publish(std::string_view, std::string_view,
               const MessageTimes&) override {}
  void Flush() override {}
};

}  // namespace laptime

#endif  // LAPTIME_SINKS_DISCARD_DISCARD_SINK_H_
