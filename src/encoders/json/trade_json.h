//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_ENCODERS_JSON_TRADE_JSON_H_
#define LAPTIME_ENCODERS_JSON_TRADE_JSON_H_

#include <string>

#include "model/trade.h"

namespace laptime {

std::string ToJson(const Trade& trade);

}  // namespace laptime

#endif  // LAPTIME_ENCODERS_JSON_TRADE_JSON_H_
