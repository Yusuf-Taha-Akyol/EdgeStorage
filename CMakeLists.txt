cmake_minimum_required(VERSION 3.16)

project(EdgeStorage
    VERSION 0.1.0
    DESCRIPTION "Lightweight compressed telemetry storage for edge devices"
    LANGUAGES C
)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

add_library(edgestorage
    src/api/edgestorage.c
    src/core/runtime.c
    src/core/stream_registry.c
    src/storage/storage_writer.c
    src/storage/storage_reader.c
    src/storage/segment_format.c
)

target_include_directories(edgestorage
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src/internal
)

add_executable(edgestorage_example
    examples/basic_example.c
)

target_link_libraries(edgestorage_example
    PRIVATE edgestorage
)

enable_testing()

add_executable(edgestorage_smoke_test
    tests/smoke_test.c
)

target_link_libraries(edgestorage_smoke_test
    PRIVATE edgestorage
)

add_test(NAME edgestorage_smoke_test COMMAND edgestorage_smoke_test)

add_executable(edgestorage_storage_flow_test
    tests/storage_flow_test.c
)

target_link_libraries(edgestorage_storage_flow_test
    PRIVATE edgestorage
)

add_test(NAME edgestorage_storage_flow_test COMMAND edgestorage_storage_flow_test)

add_executable(edgestorage_compression_delta_test
    tests/compression_delta_test.c
)

target_link_libraries(edgestorage_compression_delta_test
    PRIVATE edgestorage
)

add_test(NAME edgestorage_compression_delta_test COMMAND edgestorage_compression_delta_test)

add_executable(edgestorage_read_query_test
    tests/read_query_test.c
)

target_link_libraries(edgestorage_read_query_test
    PRIVATE edgestorage
)

add_test(NAME edgestorage_read_query_test COMMAND edgestorage_read_query_test)

add_executable(edgestorage_benchmark
    benchmarks/basic_benchmark.c
)

target_link_libraries(edgestorage_benchmark
    PRIVATE edgestorage
)

add_executable(edgestorage_segment_header_test
    tests/segment_header_test.c
)

target_link_libraries(edgestorage_segment_header_test
    PRIVATE edgestorage
)

target_include_directories(edgestorage_segment_header_test
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/internal
)

add_test(NAME edgestorage_segment_header_test COMMAND edgestorage_segment_header_test)