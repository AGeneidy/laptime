//
// Created by Ayman Geneidy on 31/10/2026.
//

#ifndef LAPTIME_SOURCES_WEBSOCKET_WEBSOCKET_SOURCE_H_
#define LAPTIME_SOURCES_WEBSOCKET_WEBSOCKET_SOURCE_H_

#include <utility>

#include "ixwebsocket/IXNetSystem.h"
#include "ixwebsocket/IXWebSocket.h"
#include "model/config/source_config.h"
#include "sources/source.h"

namespace laptime {

class WebSocketSource final : public Source {
 public:
  explicit WebSocketSource(const WebSocketConfig& config,
                           MessageHandler on_message)
      : on_message_(std::move(on_message)) {
    ix::initNetSystem();
    ws_.setUrl(config.url);
    ws_.setOnMessageCallback([this](const ix::WebSocketMessagePtr& msg) {
      this->OnSocketMessage(msg);
    });
  }
  void Start() override {
    stopped_ = false;
    ws_.start();
  }

  void Stop() override {
    if (stopped_) return;
    stopped_ = true;
    ws_.stop();
  }

 private:
  void OnSocketMessage(const ix::WebSocketMessagePtr& msg) const;
  MessageHandler on_message_;
  ix::WebSocket ws_;
  bool stopped_ = false;
};

}  // namespace laptime

#endif  // LAPTIME_SOURCES_WEBSOCKET_WEBSOCKET_SOURCE_H_
