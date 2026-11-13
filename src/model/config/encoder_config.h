//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_MODEL_CONFIG_ENCODER_CONFIG_H_
#define LAPTIME_MODEL_CONFIG_ENCODER_CONFIG_H_

#include <variant>

namespace laptime {

struct FormatJsonEncoderConfig {};

struct JsonEncoderConfig {};

using EncoderConfig = std::variant<JsonEncoderConfig, FormatJsonEncoderConfig>;

}  // namespace laptime

#endif  // LAPTIME_MODEL_CONFIG_ENCODER_CONFIG_H_
