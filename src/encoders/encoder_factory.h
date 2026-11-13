//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_ENCODERS_ENCODER_FACTORY_H_
#define LAPTIME_ENCODERS_ENCODER_FACTORY_H_
#include "encoders/encoder.h"
#include "encoders/json/trade_json.h"
#include "model/config/encoder_config.h"

namespace laptime {


inline Encoder EncoderOf(const JsonEncoderConfig&) { return ToJson; }
inline Encoder EncoderOf(const FormatJsonEncoderConfig&) { return ToJson; }

inline Encoder CreateEncoder(const EncoderConfig& config) {
  return std::visit([](const auto& c) { return EncoderOf(c); }, config);
}

}  // namespace laptime

#endif  // LAPTIME_ENCODERS_ENCODER_FACTORY_H_
