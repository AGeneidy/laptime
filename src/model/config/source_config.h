//
// Created by Ayman Geneidy on 31/10/2026.
//

#ifndef LAPTIME_MODEL_CONFIG_SOURCE_CONFIG_H_
#define LAPTIME_MODEL_CONFIG_SOURCE_CONFIG_H_

#include <string>
#include <variant>

namespace laptime {

struct FileConfig {
  std::string path;
};

struct WebSocketConfig {
  std::string url;
};

using SourceConfig = std::variant<WebSocketConfig, FileConfig>;

}  // namespace laptime

#endif  // LAPTIME_MODEL_CONFIG_SOURCE_CONFIG_H_
