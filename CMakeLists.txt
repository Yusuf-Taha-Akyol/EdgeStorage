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
)

target_include_directories(edgestorage
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
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