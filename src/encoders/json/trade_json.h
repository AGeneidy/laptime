//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_ENCODERS_JSON_TRADE_JSON_H_
#define LAPTIME_ENCODERS_JSON_TRADE_JSON_H_

#include <span>
#include <string_view>

#include "model/trade.h"

namespace laptime {

std::string_view ToJson(const Trade& trade, std::span<char> out);

}  // namespace laptime

#endif  // LAPTIME_ENCODERS_JSON_TRADE_JSON_H_
