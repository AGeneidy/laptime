//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_ENCODERS_ENCODER_H_
#define LAPTIME_ENCODERS_ENCODER_H_
#include <cstddef>
#include <span>
#include <string_view>

#include "model/trade.h"

namespace laptime {

// Every encoder's output for one trade must fit in a buffer this big.
inline constexpr size_t kEncodeBufferBytes = 1024;

using Encoder = std::string_view (*)(const Trade&, std::span<char>);

}  // namespace laptime
#endif  // LAPTIME_ENCODERS_ENCODER_H_
