//
// Created by Ayman Geneidy on 05/11/2026.
//

#ifndef LAPTIME_MODEL_TRADE_H_
#define LAPTIME_MODEL_TRADE_H_

#include <cstdint>
#include <string>

namespace laptime {

enum class Side : uint8_t { kBuy, kSell };

struct Trade {
  std::string exchange;  // "binance" (the adapter sets it)
  std::string symbol;
  int64_t trade_id = 0;  // "t"  +1 per trade on each symbol
  double price = 0;      // "p"  sent as text
  double quantity = 0;   // "q"  sent as text
  Side taker_side =
      Side::kBuy;             // who caused the trade; Binance: m == true → Sell
  int64_t trade_time_us = 0;  // "T"  when Binance matched it
  int64_t event_time_us = 0;  // "E"  when Binance sent it
};

}  // namespace laptime

#endif  // LAPTIME_MODEL_TRADE_H_
