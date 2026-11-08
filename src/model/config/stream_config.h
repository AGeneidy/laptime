//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_MODEL_CONFIG_STREAM_CONFIG_H_
#define LAPTIME_MODEL_CONFIG_STREAM_CONFIG_H_
#include <string>

#include "model/config/adaptor_config.h"
#include "model/config/encoder_config.h"
#include "model/config/sink_config.h"
#include "model/config/source_config.h"

namespace laptime {

struct StreamConfig {
  std::string name;
  SourceConfig source;
  SinkConfig sink;
  AdaptorConfig adaptor;
  EncoderConfig encoder;
};

}  // namespace laptime
#endif  // LAPTIME_MODEL_CONFIG_STREAM_CONFIG_H_
