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
set(IXWEBSOCKET_PATCH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/ixwebsocket/receive-times.patch)
file(SHA256 ${IXWEBSOCKET_PATCH} IXWEBSOCKET_PATCH_SHA256)
FetchContent_Declare(ixwebsocket
        GIT_REPOSITORY https://github.com/machinezone/IXWebSocket.git
        GIT_TAG        64fae7676bd8fe31f7cb4bcde7a6841892dad65e  # v12.0.1
        GIT_SHALLOW    TRUE
        # Reset first, so a re-run never applies the patch on top of itself.
        PATCH_COMMAND  git reset --hard --quiet
        COMMAND  git apply ${IXWEBSOCKET_PATCH}
        # The hash is part of the declaration, so editing the patch re-applies it.
        COMMAND  ${CMAKE_COMMAND} -E echo "IXWebSocket patch ${IXWEBSOCKET_PATCH_SHA256}")

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

add_library(laptime_core STATIC
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
        src/common/message_times.h
        src/sinks/discard/discard_sink.h
        src/sources/file/file_source.cc
        src/sources/file/file_source.h
)
target_include_directories(laptime_core PUBLIC src)
target_link_libraries(laptime_core PUBLIC ixwebsocket::ixwebsocket rdkafka++ nlohmann_json::nlohmann_json)
target_compile_options(laptime_core PRIVATE -Wall -Wextra)

add_executable(laptime src/main.cc)
target_link_libraries(laptime PRIVATE laptime_core)
target_compile_options(laptime PRIVATE -Wall -Wextra)

# --- Google Benchmark ---
option(LAPTIME_BUILD_BENCHMARKS "Build the micro-benchmarks in bench/" ON)
if(LAPTIME_BUILD_BENCHMARKS)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(benchmark
            GIT_REPOSITORY https://github.com/google/benchmark.git
            GIT_TAG        a6b4a9f00b36c19af9997d68a1cf6e0b5b3d032c  # v1.10.0
            GIT_SHALLOW    TRUE)
    FetchContent_MakeAvailable(benchmark)

    add_executable(laptime_bench bench/hot_path_bench.cc)
    target_link_libraries(laptime_bench PRIVATE laptime_core benchmark::benchmark_main)
    target_compile_options(laptime_bench PRIVATE -Wall -Wextra)
    target_compile_definitions(laptime_bench PRIVATE
            LAPTIME_TESTDATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/testdata")
endif()
