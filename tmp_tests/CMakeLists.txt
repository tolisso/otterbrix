project(tmp_test_insert_large CXX)

set(${PROJECT_NAME}_SOURCES
        test_insert_large.cpp
        test_jsonbench.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/integration/cpp/test
)

# Path to the sample JSON file (relative to the project source root).
target_compile_definitions(${PROJECT_NAME} PRIVATE
        "TEST_SAMPLE_FILE=\"${CMAKE_SOURCE_DIR}/test_sample_100_000_filtered.json\""
)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::table
        otterbrix::document
        otterbrix::expressions
        otterbrix::file
        otterbrix::types
        otterbrix::vector
        Catch2::Catch2
        magic_enum::magic_enum
        cpp_otterbrix
        otterbrix::log
        otterbrix::memory_storage
        otterbrix::collection
        otterbrix::cursor
        otterbrix::session
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::disk
        otterbrix::locks
        spdlog::spdlog
        msgpackc-cxx
        absl::crc32c
        absl::int128
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        ZLIB::ZLIB
        BZip2::BZip2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
