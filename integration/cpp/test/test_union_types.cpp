#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

TEST_CASE("integration::cpp::test_union_types") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;

    auto resource = std::pmr::synchronized_pool_resource();

    std::cout << "\n=== Union Types Test ===" << std::endl;

    // Define union type with INTEGER, STRING, DOUBLE
    std::vector<complex_logical_type> union_fields;
    union_fields.emplace_back(logical_type::INTEGER, "int");
    union_fields.emplace_back(logical_type::STRING_LITERAL, "string");
    union_fields.emplace_back(logical_type::DOUBLE, "double");
    complex_logical_type union_type = complex_logical_type::create_union(union_fields, "value_union");

    std::cout << "✓ Created union type with 3 variants: INTEGER, STRING, DOUBLE" << std::endl;

    // Create table with union column
    core::filesystem::local_file_system_t fs;
    auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
    auto block_manager = storage::in_memory_block_manager_t(buffer_manager, storage::DEFAULT_BLOCK_ALLOC_SIZE);

    std::vector<column_definition_t> columns;
    columns.emplace_back("id", logical_type::UBIGINT);
    columns.emplace_back("value", union_type);

    auto data_table = std::make_unique<data_table_t>(&resource, block_manager, std::move(columns));
    std::cout << "✓ Created table with columns: id (UBIGINT), value (UNION)" << std::endl;

    // Insert 3 rows with different types
    {
        data_chunk_t chunk(&resource, data_table->copy_types(), 3);
        chunk.set_cardinality(3);

        // Row 0: INTEGER value = 42
        chunk.set_value(0, 0, logical_value_t{uint64_t(1)});
        chunk.set_value(1, 0, logical_value_t::create_union(union_fields, 0, logical_value_t{int32_t(42)}));

        // Row 1: STRING value = "hello"
        chunk.set_value(0, 1, logical_value_t{uint64_t(2)});
        chunk.set_value(1, 1, logical_value_t::create_union(union_fields, 1, logical_value_t{std::string("hello")}));

        // Row 2: DOUBLE value = 3.14
        chunk.set_value(0, 2, logical_value_t{uint64_t(3)});
        chunk.set_value(1, 2, logical_value_t::create_union(union_fields, 2, logical_value_t{3.14}));

        table_append_state state(&resource);
        data_table->append_lock(state);
        data_table->initialize_append(state);
        data_table->append(chunk, state);
        data_table->finalize_append(state);

        std::cout << "✓ Inserted 3 rows:" << std::endl;
        std::cout << "  - Row 1: id=1, value=42 (INTEGER)" << std::endl;
        std::cout << "  - Row 2: id=2, value='hello' (STRING)" << std::endl;
        std::cout << "  - Row 3: id=3, value=3.14 (DOUBLE)" << std::endl;
    }

    // Scan and verify
    {
        std::vector<storage_index_t> column_indices;
        column_indices.emplace_back(0);
        column_indices.emplace_back(1);
        table_scan_state state(&resource);
        data_chunk_t result(&resource, data_table->copy_types(), 3);
        data_table->initialize_scan(state, column_indices);
        data_table->scan(result, state);

        REQUIRE(result.size() == 3);
        std::cout << "\n✓ Scanned 3 rows from table" << std::endl;

        // Verify Row 0: INTEGER
        {
            logical_value_t id_val = result.data[0].value(0);
            logical_value_t union_val = result.data[1].value(0);

            REQUIRE(id_val.value<uint64_t>() == 1);
            REQUIRE(union_val.type().type() == logical_type::UNION);

            auto tag = union_val.children()[0].value<uint8_t>();
            REQUIRE(tag == 0); // INTEGER variant
            REQUIRE(union_val.children()[1].value<int32_t>() == 42);

            std::cout << "  Row 1: id=" << id_val.value<uint64_t>()
                      << ", value=" << union_val.children()[1].value<int32_t>()
                      << " (tag=" << int(tag) << " INTEGER)" << std::endl;
        }

        // Verify Row 1: STRING
        {
            logical_value_t id_val = result.data[0].value(1);
            logical_value_t union_val = result.data[1].value(1);

            REQUIRE(id_val.value<uint64_t>() == 2);
            REQUIRE(union_val.type().type() == logical_type::UNION);

            auto tag = union_val.children()[0].value<uint8_t>();
            REQUIRE(tag == 1); // STRING variant
            std::string str_value = *union_val.children()[2].value<std::string*>();
            REQUIRE(str_value == "hello");

            std::cout << "  Row 2: id=" << id_val.value<uint64_t>()
                      << ", value='" << str_value << "'"
                      << " (tag=" << int(tag) << " STRING)" << std::endl;
        }

        // Verify Row 2: DOUBLE
        {
            logical_value_t id_val = result.data[0].value(2);
            logical_value_t union_val = result.data[1].value(2);

            REQUIRE(id_val.value<uint64_t>() == 3);
            REQUIRE(union_val.type().type() == logical_type::UNION);

            auto tag = union_val.children()[0].value<uint8_t>();
            REQUIRE(tag == 2); // DOUBLE variant
            REQUIRE(union_val.children()[3].value<double>() == 3.14);

            std::cout << "  Row 3: id=" << id_val.value<uint64_t>()
                      << ", value=" << union_val.children()[3].value<double>()
                      << " (tag=" << int(tag) << " DOUBLE)" << std::endl;
        }
    }

    std::cout << "\n✅ All tests PASSED! Union types work perfectly!" << std::endl;
    std::cout << "    Same column 'value' stores INTEGER, STRING, and DOUBLE" << std::endl;
}
