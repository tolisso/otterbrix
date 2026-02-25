#include <catch2/catch.hpp>

#include <components/table/data_table.hpp>
#include <components/table/insert_jsons.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <stdexcept>

using namespace components::table;
using namespace components::types;
using namespace components::vector;

// ---------------------------------------------------------------------------
// Shared test fixture
// ---------------------------------------------------------------------------
struct TableFixture {
    std::pmr::synchronized_pool_resource resource;
    core::filesystem::local_file_system_t fs;
    storage::buffer_pool_t buffer_pool;
    storage::standard_buffer_manager_t buffer_manager;
    storage::in_memory_block_manager_t block_manager;

    TableFixture()
        : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager(&resource, fs, buffer_pool)
        , block_manager(buffer_manager, storage::DEFAULT_BLOCK_ALLOC_SIZE) {}

    std::unique_ptr<data_table_t> make_empty_table(std::string name = "test") {
        return std::make_unique<data_table_t>(&resource,
                                              block_manager,
                                              std::vector<column_definition_t>{},
                                              std::move(name));
    }

    // Return the column type for a given name (asserts column exists).
    logical_type col_type(const data_table_t& tbl, const std::string& name) {
        for (const auto& col : tbl.columns()) {
            if (col.name() == name) {
                return col.type().type();
            }
        }
        FAIL("column not found: " + name);
        return logical_type::INVALID;
    }

    // Scan the full table and return col_name -> vector<logical_value_t>.
    std::map<std::string, std::vector<logical_value_t>> scan(data_table_t& tbl) {
        std::size_t nc = static_cast<std::size_t>(tbl.column_count());
        std::size_t nr = static_cast<std::size_t>(tbl.calculate_size());

        std::map<std::string, std::vector<logical_value_t>> result;
        for (const auto& col : tbl.columns()) {
            result[col.name()] = {};
        }
        if (nr == 0) {
            return result;
        }

        std::vector<storage_index_t> col_ids;
        col_ids.reserve(nc);
        for (std::size_t i = 0; i < nc; ++i) {
            col_ids.emplace_back(static_cast<int64_t>(i));
        }

        auto types = tbl.copy_types();
        table_scan_state state(&resource);
        data_chunk_t chunk(&resource, types, static_cast<uint64_t>(nr));
        tbl.initialize_scan(state, col_ids);
        tbl.scan(chunk, state);

        // Flatten so that validity bits are directly accessible.
        chunk.flatten();

        const auto& cols = tbl.columns();
        for (std::size_t c = 0; c < nc; ++c) {
            const auto& vec = chunk.data[c];
            for (std::size_t r = 0; r < chunk.size(); ++r) {
                if (!vec.validity().row_is_valid(static_cast<uint64_t>(r))) {
                    result[cols[c].name()].push_back(logical_value_t{});
                } else {
                    result[cols[c].name()].push_back(
                        chunk.value(static_cast<uint64_t>(c), static_cast<uint64_t>(r)));
                }
            }
        }
        return result;
    }
};

// ---------------------------------------------------------------------------
// TEST 1: flat single-level objects — check column types
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: flat documents with correct types") {
    TableFixture f;
    auto table = f.make_empty_table();

    std::vector<std::string> jsons = {
        R"({"did":"user1","kind":"commit","time_us":12345,"active":true})",
        R"({"did":"user2","kind":"follow","time_us":12346,"active":false})",
        R"({"did":"user3","kind":"commit","time_us":12347,"active":true})",
    };

    table = insert_jsons(std::move(table), &f.resource, jsons);

    REQUIRE(table->column_count() == 4);
    REQUIRE(table->calculate_size() == 3);

    // Columns appear in alphabetical order: active, did, kind, time_us
    REQUIRE(f.col_type(*table, "active") == logical_type::BOOLEAN);
    REQUIRE(f.col_type(*table, "did") == logical_type::STRING_LITERAL);
    REQUIRE(f.col_type(*table, "kind") == logical_type::STRING_LITERAL);
    REQUIRE(f.col_type(*table, "time_us") == logical_type::BIGINT);

    auto rows = f.scan(*table);
    REQUIRE(rows["did"][0].value<std::string*>()->compare("user1") == 0);
    REQUIRE(rows["did"][1].value<std::string*>()->compare("user2") == 0);
    REQUIRE(rows["time_us"][0].value<int64_t>() == 12345);
    REQUIRE(rows["time_us"][2].value<int64_t>() == 12347);
    REQUIRE(rows["active"][0].value<bool>() == true);
    REQUIRE(rows["active"][1].value<bool>() == false);
}

// ---------------------------------------------------------------------------
// TEST 2: nested objects → dot-separated column names, correct types
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: nested objects") {
    TableFixture f;
    auto table = f.make_empty_table();

    std::vector<std::string> jsons = {
        R"({"did":"user1","commit":{"collection":"app.bsky.feed.post","operation":"create","rev":42}})",
        R"({"did":"user2","commit":{"collection":"app.bsky.feed.like","operation":"create","rev":43}})",
    };

    table = insert_jsons(std::move(table), &f.resource, jsons);

    // Columns: commit.collection, commit.operation, commit.rev, did
    REQUIRE(table->column_count() == 4);
    REQUIRE(f.col_type(*table, "commit.collection") == logical_type::STRING_LITERAL);
    REQUIRE(f.col_type(*table, "commit.operation") == logical_type::STRING_LITERAL);
    REQUIRE(f.col_type(*table, "commit.rev") == logical_type::BIGINT);
    REQUIRE(f.col_type(*table, "did") == logical_type::STRING_LITERAL);

    auto rows = f.scan(*table);
    REQUIRE(rows["commit.collection"][0].value<std::string_view>() == "app.bsky.feed.post");
    REQUIRE(rows["commit.rev"][0].value<int64_t>() == 42);
    REQUIRE(rows["commit.rev"][1].value<int64_t>() == 43);
}

// ---------------------------------------------------------------------------
// TEST 3: missing fields → NULL values
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: missing fields become NULL") {
    TableFixture f;
    auto table = f.make_empty_table();

    std::vector<std::string> jsons = {
        R"({"did":"user1","kind":"commit","score":3.14})",
        R"({"did":"user2","kind":"follow"})",         // no "score"
        R"({"did":"user3","score":2.71})",            // no "kind"
    };

    table = insert_jsons(std::move(table), &f.resource, jsons);

    REQUIRE(f.col_type(*table, "score") == logical_type::DOUBLE);
    REQUIRE(f.col_type(*table, "kind") == logical_type::STRING_LITERAL);

    auto rows = f.scan(*table);

    REQUIRE_FALSE(rows["score"][0].is_null());
    REQUIRE(rows["score"][1].is_null());
    REQUIRE_FALSE(rows["score"][2].is_null());

    REQUIRE_FALSE(rows["kind"][0].is_null());
    REQUIRE_FALSE(rows["kind"][1].is_null());
    REQUIRE(rows["kind"][2].is_null());
}

// ---------------------------------------------------------------------------
// TEST 4: arrays → bracket-indexed column names
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: arrays produce bracket column names") {
    TableFixture f;
    auto table = f.make_empty_table();

    std::vector<std::string> jsons = {
        R"({"id":1,"tags":["sports","news","tech"]})",
        R"({"id":2,"tags":["music","news"]})",
    };

    table = insert_jsons(std::move(table), &f.resource, jsons);

    // id → BIGINT, tags[0..2] → STRING_LITERAL
    REQUIRE(f.col_type(*table, "id") == logical_type::BIGINT);
    REQUIRE(f.col_type(*table, "tags[0]") == logical_type::STRING_LITERAL);
    REQUIRE(f.col_type(*table, "tags[1]") == logical_type::STRING_LITERAL);
    REQUIRE(f.col_type(*table, "tags[2]") == logical_type::STRING_LITERAL);

    auto rows = f.scan(*table);
    REQUIRE(rows["tags[0]"][0].value<std::string_view>() == "sports");
    REQUIRE(rows["tags[0]"][1].value<std::string_view>() == "music");
    REQUIRE(rows["tags[2]"][0].value<std::string_view>() == "tech");
    REQUIRE(rows["tags[2]"][1].is_null()); // row 1 has no tags[2]
}

// ---------------------------------------------------------------------------
// TEST 5: double values
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: double column") {
    TableFixture f;
    auto table = f.make_empty_table();

    std::vector<std::string> jsons = {
        R"({"x":1.5,"y":2.5})",
        R"({"x":3.0,"y":4.0})",
    };

    table = insert_jsons(std::move(table), &f.resource, jsons);

    REQUIRE(f.col_type(*table, "x") == logical_type::DOUBLE);
    REQUIRE(f.col_type(*table, "y") == logical_type::DOUBLE);

    auto rows = f.scan(*table);
    REQUIRE(rows["x"][0].value<double>() == Approx(1.5));
    REQUIRE(rows["x"][1].value<double>() == Approx(3.0));
    REQUIRE(rows["y"][1].value<double>() == Approx(4.0));
}

// ---------------------------------------------------------------------------
// TEST 6: type conflict across documents → std::runtime_error
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: type conflict throws") {
    TableFixture f;
    auto table = f.make_empty_table();

    // "value" is int in doc 0, string in doc 1 → conflict
    std::vector<std::string> jsons = {
        R"({"value":42})",
        R"({"value":"hello"})",
    };

    REQUIRE_THROWS_AS(insert_jsons(std::move(table), &f.resource, jsons),
                      std::runtime_error);
}

// ---------------------------------------------------------------------------
// TEST 7: existing column type mismatch → std::runtime_error
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: existing column type mismatch throws") {
    TableFixture f;

    // Pre-create a table with a BIGINT column named "x".
    std::vector<column_definition_t> existing;
    existing.emplace_back("x", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&f.resource,
                                                f.block_manager,
                                                std::move(existing),
                                                "t");

    // Try to insert JSONs where "x" is a string → type mismatch
    std::vector<std::string> jsons = {
        R"({"x":"hello"})",
    };

    REQUIRE_THROWS_AS(insert_jsons(std::move(table), &f.resource, jsons),
                      std::runtime_error);
}

// ---------------------------------------------------------------------------
// TEST 8: compatible existing column (same type) — no error
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: compatible existing column accepted") {
    TableFixture f;

    std::vector<column_definition_t> existing;
    existing.emplace_back("x", logical_type::BIGINT);
    auto table = std::make_unique<data_table_t>(&f.resource,
                                                f.block_manager,
                                                std::move(existing),
                                                "t");

    // "x" is also integer in the JSON → compatible
    std::vector<std::string> jsons = {
        R"({"x":10,"y":"hello"})",
        R"({"x":20,"y":"world"})",
    };

    REQUIRE_NOTHROW(table = insert_jsons(std::move(table), &f.resource, jsons));
    REQUIRE(table->calculate_size() == 2);
    REQUIRE(table->column_count() == 2); // x (pre-existing) + y (new)

    auto rows = f.scan(*table);
    REQUIRE(rows["x"][0].value<int64_t>() == 10);
    REQUIRE(rows["x"][1].value<int64_t>() == 20);
    REQUIRE(rows["y"][0].value<std::string_view>() == "hello");
}

// ---------------------------------------------------------------------------
// TEST 9: multiple insert_jsons calls accumulate rows; new columns added
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: multiple calls accumulate rows") {
    TableFixture f;
    auto table = f.make_empty_table();

    table = insert_jsons(std::move(table), &f.resource, {
        R"({"did":"user1","kind":"commit"})",
        R"({"did":"user2","kind":"follow"})",
    });
    REQUIRE(table->calculate_size() == 2);
    REQUIRE(table->column_count() == 2);

    table = insert_jsons(std::move(table), &f.resource, {
        R"({"did":"user3","kind":"commit","extra":99})",
    });
    REQUIRE(table->calculate_size() == 3);
    REQUIRE(table->column_count() == 3);
    REQUIRE(f.col_type(*table, "extra") == logical_type::BIGINT);

    auto rows = f.scan(*table);
    REQUIRE(rows["extra"][0].is_null());
    REQUIRE(rows["extra"][1].is_null());
    REQUIRE(rows["extra"][2].value<int64_t>() == 99);
}

// ---------------------------------------------------------------------------
// TEST 10: empty input → table returned unchanged
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: empty input") {
    TableFixture f;
    std::vector<column_definition_t> existing;
    existing.emplace_back("col_a", logical_type::STRING_LITERAL);
    auto table = std::make_unique<data_table_t>(&f.resource,
                                                f.block_manager,
                                                std::move(existing),
                                                "t");
    table = insert_jsons(std::move(table), &f.resource, {});

    REQUIRE(table->column_count() == 1);
    REQUIRE(table->calculate_size() == 0);
}

// ---------------------------------------------------------------------------
// TEST 11: JSON null values stored as NULL in the table
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: JSON null values") {
    TableFixture f;
    auto table = f.make_empty_table();

    std::vector<std::string> jsons = {
        R"({"key":"value","nullable":null})",
        R"({"key":"other","nullable":"present"})",
    };

    // "nullable" is null in row 0 and string in row 1 → STRING_LITERAL column
    table = insert_jsons(std::move(table), &f.resource, jsons);

    REQUIRE(f.col_type(*table, "nullable") == logical_type::STRING_LITERAL);

    auto rows = f.scan(*table);
    REQUIRE(rows["nullable"][0].is_null());
    REQUIRE_FALSE(rows["nullable"][1].is_null());
    REQUIRE(rows["nullable"][1].value<std::string_view>() == "present");
}

// ---------------------------------------------------------------------------
// TEST 12: bool / int type conflict → error
// ---------------------------------------------------------------------------
TEST_CASE("insert_jsons: bool vs int conflict throws") {
    TableFixture f;
    auto table = f.make_empty_table();

    std::vector<std::string> jsons = {
        R"({"flag":true})",
        R"({"flag":1})",   // int, not bool
    };

    REQUIRE_THROWS_AS(insert_jsons(std::move(table), &f.resource, jsons),
                      std::runtime_error);
}
