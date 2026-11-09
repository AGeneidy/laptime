cmake_minimum_required(VERSION 3.30)
project(laptime)

set(CMAKE_CXX_STANDARD 20)
include(FetchContent)


if(APPLE)
    set(OPENSSL_ROOT_DIR /opt/homebrew/opt/openssl@3)
endif()

# --- IXWebSocket ---
set(USE_TLS ON CACHE BOOL "" FORCE)
set(USE_OPEN_SSL ON CACHE BOOL "" FORCE)
FetchContent_Declare(ixwebsocket
        GIT_REPOSITORY https://github.com/machinezone/IXWebSocket.git
        GIT_TAG        v12.0.1
        GIT_SHALLOW    TRUE)

# --- librdkafka ---
set(RDKAFKA_BUILD_STATIC   ON  CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RDKAFKA_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
FetchContent_Declare(librdkafka
        GIT_REPOSITORY https://github.com/confluentinc/librdkafka.git
        GIT_TAG        v2.16.0
        GIT_SHALLOW    TRUE)

# --- nlohmann_json ---
FetchContent_Declare(json
        URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz)

FetchContent_MakeAvailable(ixwebsocket librdkafka json)


add_executable(laptime src/main.cc
        src/adaptors/adaptor.h
        src/adaptors/adaptor_factory.h
        src/adaptors/binance/binance_adaptor.cc
        src/adaptors/binance/binance_adaptor.h
        src/common/stop_signal.cc
        src/common/stop_signal.h
        src/model/config/adaptor_config.h
        src/model/config/sink_config.h
        src/model/config/source_config.h
        src/model/trade.h
        src/encoders/json/trade_json.cc
        src/encoders/json/trade_json.h
        src/sinks/kafka/kafka_sink.cc
        src/sinks/kafka/kafka_sink.h
        src/sinks/sink.h
        src/sinks/sink_factory.h
        src/sources/source.h
        src/sources/source_factory.h
        src/sources/websocket/websocket_source.cc
        src/sources/websocket/websocket_source.h
        src/feed/stream.cc
        src/feed/stream.h
        src/model/config/stream_config.h
        src/encoders/encoder_factory.h
        src/model/config/encoder_config.h
        src/encoders/encoder.h
        src/feed/feed_handler.h
        src/model/config/feed_handler_config.h
        src/common/clock.h
        src/common/latency_recorder.cc
        src/common/latency_recorder.h
)
target_include_directories(laptime PUBLIC src)
target_link_libraries(laptime PUBLIC ixwebsocket::ixwebsocket rdkafka++ nlohmann_json::nlohmann_json)
target_compile_options(laptime PRIVATE -Wall -Wextra)
