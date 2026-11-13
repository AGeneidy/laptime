//
// Created by Ayman Geneidy on 06/11/2026.
//

#include "encoders/json/trade_json.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "encoders/encoder.h"
#include "nlohmann/json.hpp"

namespace laptime {

namespace {

// int64_t: 19 digits and a sign.
constexpr size_t kMaxInt64Chars =
    std::numeric_limits<int64_t>::digits10 + 1 + 1;
// double: a sign, 17 digits, '.' and "e-308".
constexpr size_t kMaxDoubleChars =
    1 + std::numeric_limits<double>::max_digits10 + 1 + 5;
// taker_side: "sell".
constexpr size_t kMaxSideChars = 4;

// Every byte of a trade's JSON that isn't a value.
constexpr std::string_view kFixedText =
    R"({"exchange":"","symbol":"","trade_id":,"price":,"quantity":,)"
    R"("taker_side":"","trade_time_us":,"event_time_us":})";

constexpr size_t kMaxTradeJsonBytes = kFixedText.size() + 3 * kMaxInt64Chars +
                                      2 * kMaxDoubleChars + kMaxSideChars +
                                      kMaxExchangeBytes + kMaxSymbolBytes;
static_assert(kMaxTradeJsonBytes <= kEncodeBufferBytes,
              "the longest trade's JSON must fit the encode buffer");

}  // namespace

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

std::string_view ToJsonWithFormat(const Trade& trade, std::span<char> out) {
    const auto [_, size] = std::format_to_n(
        out.data(), out.size(),
        R"({{"exchange":"{}","symbol":"{}","trade_id":{},"price":{},)"
        R"("quantity":{},"taker_side":"{}","trade_time_us":{},)"
        R"("event_time_us":{}}})",
        trade.exchange, trade.symbol, trade.trade_id, trade.price,
        trade.quantity,
        trade.taker_side == Side::kBuy ? "buy" : "sell", trade.trade_time_us,
        trade.event_time_us);

    // result.size is the whole JSON's length, even when only part of it fit.
    if (static_cast<size_t>(size) > out.size()) return {};
    return {out.data(), static_cast<size_t>(size)};
}

}  // namespace laptime
