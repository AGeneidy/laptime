//
// Created by Ayman Geneidy on 31/10/2026.
//

#ifndef LAPTIME_SOURCES_SOURCE_FACTORY_H_
#define LAPTIME_SOURCES_SOURCE_FACTORY_H_

#include <iostream>
#include <memory>
#include <string_view>
#include <variant>

#include "model/config/source_config.h"
#include "sources/source.h"
#include "sources/websocket/websocket_source.h"

namespace laptime {

inline const Source::MessageHandler kDefaultOnMessage =
    [](const std::string_view content, const MessageTimes&) {
      std::cout << "Message Received : " << content << std::endl;
    };

inline std::unique_ptr<Source> SourceOf(
    const WebSocketConfig &config, const Source::MessageHandler &on_message) {
  return std::make_unique<WebSocketSource>(config, on_message);
}

inline std::unique_ptr<Source> CreateSource(
    const SourceConfig &config,
    const Source::MessageHandler &on_message = kDefaultOnMessage) {
  return std::visit([&](const auto &c) { return SourceOf(c, on_message); },
                    config);
}

}  // namespace laptime

#endif  // LAPTIME_SOURCES_SOURCE_FACTORY_H_
