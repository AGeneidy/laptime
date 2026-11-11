//
// Created by Ayman Geneidy on 01/11/2026.
//

#ifndef LAPTIME_MODEL_CONFIG_SINK_CONFIG_H_
#define LAPTIME_MODEL_CONFIG_SINK_CONFIG_H_

#include <string>
#include <variant>

namespace laptime {

struct DiscardSinkConfig {};

struct KafkaSinkConfig {
  std::string brokers;
  std::string topic;
};

using SinkConfig = std::variant<KafkaSinkConfig, DiscardSinkConfig>;

}  // namespace laptime

#endif  // LAPTIME_MODEL_CONFIG_SINK_CONFIG_H_
