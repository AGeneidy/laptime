//
// Created by Ayman Geneidy on 07/11/2026.
//

#ifndef LAPTIME_MODEL_CONFIG_FEED_HANDLER_CONFIG_H_
#define LAPTIME_MODEL_CONFIG_FEED_HANDLER_CONFIG_H_
#include <vector>

#include "model/config/stream_config.h"

namespace laptime {

struct FeedHandlerConfig {
  std::vector<StreamConfig> streams;
};

}  // namespace laptime

#endif  // LAPTIME_MODEL_CONFIG_FEED_HANDLER_CONFIG_H_
