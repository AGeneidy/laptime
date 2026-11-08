//
// Created by Ayman Geneidy on 06/11/2026.
//

#include "encoders/json/trade_json.h"

#include <string>

#include "nlohmann/json.hpp"

namespace laptime {

NLOHMANN_JSON_SERIALIZE_ENUM(Side, {
                                       {Side::kBuy, "buy"},
                                       {Side::kSell, "sell"},
                                   })

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Trade, exchange, symbol, trade_id, price,
                                   quantity, taker_side, trade_time_us,
                                   event_time_us)

std::string ToJson(const Trade &trade) {
  // ordered_json keeps the fields in the order listed above.
  return nlohmann::ordered_json(trade).dump();
}

}  // namespace laptime
