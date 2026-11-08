//
// Created by Ayman Geneidy on 05/11/2026.
//

#include "adaptors/binance/binance_adaptor.h"

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "nlohmann/json.hpp"

namespace laptime {

using Json = nlohmann::json;

namespace {
std::optional<double> ToDouble(const std::string& text) {
  char* end = nullptr;
  const double value = std::strtod(text.c_str(), &end);
  if (end == text.c_str() || *end != '\0') return std::nullopt;
  return value;
}
}  // namespace

std::vector<Trade> BinanceAdaptor::Parse(const std::string_view text) noexcept {
  Json msg_json = Json::parse(text, nullptr, false);
  if (msg_json.is_discarded() || !msg_json.is_object()) return {};

  const auto event = msg_json.find("e");
  if (event == msg_json.end() || *event != "trade") return {};

  try {
    // Price and quantity arrive as text.
    const std::optional<double> price =
        ToDouble(msg_json.at("p").get<std::string>());
    const std::optional<double> quantity =
        ToDouble(msg_json.at("q").get<std::string>());
    if (!price || !quantity) return {};

    return {Trade{
        .exchange = "binance",
        .symbol = msg_json.at("s").get<std::string>(),
        .trade_id = msg_json.at("t").get<int64_t>(),
        .price = *price,
        .quantity = *quantity,
        .taker_side = msg_json.at("m").get<bool>() ? Side::kSell : Side::kBuy,
        .trade_time_us = msg_json.at("T").get<int64_t>(),
        .event_time_us = msg_json.at("E").get<int64_t>(),
    }};
  } catch (const Json::exception&) {
    return {};  // says "trade" but a field is missing or has the wrong type
  }
}

}  // namespace laptime
