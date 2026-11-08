//
// Created by Ayman Geneidy on 31/10/2026.
//

#include "sources/websocket/websocket_source.h"

#include <iostream>

namespace laptime {

void WebSocketSource::OnSocketMessage(
    const ix::WebSocketMessagePtr& msg) const {
  if (msg->type == ix::WebSocketMessageType::Message) {
    this->on_message_(msg->str);
  } else if (msg->type == ix::WebSocketMessageType::Open) {
    std::cout << "Connection established" << std::endl;
  } else if (msg->type == ix::WebSocketMessageType::Error) {
    // Maybe SSL is not configured properly
    std::cout << "Connection error: " << msg->errorInfo.reason << std::endl;
  }
}

}  // namespace laptime
