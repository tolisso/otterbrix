#include <catch2/catch.hpp>

#include <components/table/data_table.hpp>
#include <components/table/insert_jsons.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <fstream>
#include <string>
#include <vector>

using namespace components::table;
using namespace components::types;
using namespace components::vector;

#ifndef TEST_SAMPLE_FILE
#define TEST_SAMPLE_FILE "test_sample_100_000_filtered.json"
#endif

// ---------------------------------------------------------------------------
// TEST: insert all JSON lines from test_sample_100_000_filtered.json
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: large file insertion") {
    std::ifstream in(TEST_SAMPLE_FILE);
    INFO("Cannot open sample file: " TEST_SAMPLE_FILE);
    REQUIRE(in.is_open());

    std::vector<std::string> jsons;
    {
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                jsons.push_back(std::move(line));
            }
        }
    }
    REQUIRE(!jsons.empty());
    const std::size_t expected_rows = jsons.size();

    // --- storage setup ---
    std::pmr::synchronized_pool_resource resource;
    core::filesystem::local_file_system_t fs;
    auto buffer_pool =
        storage::buffer_pool_t(&resource, uint64_t(1) << 33, false, uint64_t(1) << 24);
    auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
    auto block_manager =
        storage::in_memory_block_manager_t(buffer_manager, storage::DEFAULT_BLOCK_ALLOC_SIZE);

    auto table = std::make_unique<data_table_t>(&resource,
                                                block_manager,
                                                std::vector<column_definition_t>{},
                                                "sample");

    // --- insert all rows ---
    table = insert_jsons(std::move(table), &resource, jsons);

    // --- basic sanity ---
    REQUIRE(table->calculate_size() == expected_rows);
    REQUIRE(table->column_count() > 0);

    const auto& cols = table->columns();

    // --- "did" column must exist (present in every bluesky event) ---
    std::size_t did_idx = std::numeric_limits<std::size_t>::max();
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (cols[i].name() == "did") {
            did_idx = i;
            break;
        }
    }
    REQUIRE(did_idx != std::numeric_limits<std::size_t>::max());
    REQUIRE(cols[did_idx].type().type() == logical_type::STRING_LITERAL);

    // --- "time_us" column must exist and be BIGINT ---
    std::size_t time_idx = std::numeric_limits<std::size_t>::max();
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (cols[i].name() == "time_us") {
            time_idx = i;
            break;
        }
    }
    REQUIRE(time_idx != std::numeric_limits<std::size_t>::max());
    REQUIRE(cols[time_idx].type().type() == logical_type::BIGINT);

    // --- batched scan: count null "did" and "time_us" across all rows ---
    std::vector<storage_index_t> col_ids;
    col_ids.emplace_back(static_cast<int64_t>(did_idx));
    col_ids.emplace_back(static_cast<int64_t>(time_idx));

    auto types = table->copy_types();
    // Subset chunk to only the two columns we care about.
    std::pmr::vector<complex_logical_type> scan_types(&resource);
    scan_types.push_back(types[did_idx]);
    scan_types.push_back(types[time_idx]);

    table_scan_state state(&resource);
    table->initialize_scan(state, col_ids);

    std::size_t scanned_rows   = 0;
    std::size_t did_null_count = 0;
    std::size_t time_null_count = 0;

    data_chunk_t chunk(&resource, scan_types, DEFAULT_VECTOR_CAPACITY);
    while (true) {
        chunk.reset();
        table->scan(chunk, state);
        if (chunk.size() == 0) {
            break;
        }
        chunk.flatten();
        const auto& v_did  = chunk.data[0];
        const auto& v_time = chunk.data[1];
        for (uint64_t r = 0; r < chunk.size(); ++r) {
            if (!v_did.validity().row_is_valid(r)) {
                ++did_null_count;
            }
            if (!v_time.validity().row_is_valid(r)) {
                ++time_null_count;
            }
        }
        scanned_rows += chunk.size();
    }

    INFO("Total rows scanned : " << scanned_rows);
    INFO("did  null count    : " << did_null_count);
    INFO("time_us null count : " << time_null_count);

    REQUIRE(scanned_rows == expected_rows);
    // "did" and "time_us" must be present in every bluesky event
    REQUIRE(did_null_count == 0);
    REQUIRE(time_null_count == 0);
}
