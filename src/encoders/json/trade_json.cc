//
// Created by Ayman Geneidy on 06/11/2026.
//

#include "encoders/json/trade_json.h"

#include <algorithm>
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

std::string_view ToJson(const Trade& trade, std::span<char> out) {
  // ordered_json keeps the fields in the order listed above.
  const std::string str = nlohmann::ordered_json(trade).dump();
  if (out.size() < str.size()) {
    return {};
  }
  std::ranges::copy(str, out.begin());
  return {out.data(), str.size()};
}

}  // namespace laptime
