//
// Created by Ayman Geneidy on 01/11/2026.
//

#ifndef LAPTIME_SINKS_SINK_FACTORY_H_
#define LAPTIME_SINKS_SINK_FACTORY_H_

#include <memory>
#include <variant>

#include "model/config/sink_config.h"
#include "sinks/kafka/kafka_sink.h"
#include "sinks/sink.h"

namespace laptime {

inline std::unique_ptr<Sink> SinkOf(const KafkaSinkConfig& config) {
  return std::make_unique<KafkaSink>(config);
}

inline std::unique_ptr<Sink> CreateSink(const SinkConfig& config) {
  return std::visit(
      [&](const auto& local_config) { return SinkOf(local_config); }, config);
}

}  // namespace laptime

#endif  // LAPTIME_SINKS_SINK_FACTORY_H_
