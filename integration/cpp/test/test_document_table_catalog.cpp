#include "test_config.hpp"
#include <catch2/catch.hpp>

static const database_name_t database_name = "catalog_testdb";
static const collection_name_t collection_name = "catalog_test";

namespace types = components::types;

// Helper to check if schema contains a field with given name and type
bool schema_has_field(const types::complex_logical_type& schema,
                      const std::string& field_name,
                      types::logical_type expected_type) {
    return types::complex_logical_type::contains(schema, [&](const types::complex_logical_type& type) {
        return type.alias() == field_name && type.type() == expected_type;
    });
}

TEST_CASE("document_table catalog integration") {
    auto config = test_create_config("/tmp/test_document_table_catalog");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    SECTION("Catalog schema is updated after INSERT") {
        // Create database and table
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.users() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // Check schema is empty before INSERT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("users"))});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            // Schema should be empty or minimal
        }

        // INSERT data
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.users (_id, name, age) VALUES ('u1', 'Alice', 30);");
            REQUIRE(cur->is_success());
        }

        // Check schema is updated with new fields
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("users"))});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);

            auto schema = cur->type_data()[0];

            // Verify fields are in catalog
            REQUIRE(schema_has_field(schema, "_id", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "name", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "age", types::logical_type::BIGINT));
        }
    }

    SECTION("Schema evolution - new fields added to catalog") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.evolving() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // First INSERT with initial fields
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.evolving (_id, field1) VALUES ('e1', 'value1');");
            REQUIRE(cur->is_success());
        }

        // Check initial schema
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("evolving"))});
            REQUIRE(cur->is_success());
            auto schema = cur->type_data()[0];
            REQUIRE(schema_has_field(schema, "_id", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "field1", types::logical_type::STRING_LITERAL));
        }

        // Second INSERT with additional field
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.evolving (_id, field1, field2) VALUES ('e2', 'value2', 42);");
            REQUIRE(cur->is_success());
        }

        // Check evolved schema
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("evolving"))});
            REQUIRE(cur->is_success());
            auto schema = cur->type_data()[0];
            REQUIRE(schema_has_field(schema, "_id", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "field1", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "field2", types::logical_type::BIGINT));
        }

        // Third INSERT with yet another field
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.evolving (_id, field3) VALUES ('e3', 3.14);");
            REQUIRE(cur->is_success());
        }

        // Check final schema
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("evolving"))});
            REQUIRE(cur->is_success());
            auto schema = cur->type_data()[0];
            REQUIRE(schema_has_field(schema, "_id", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "field1", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "field2", types::logical_type::BIGINT));
            REQUIRE(schema_has_field(schema, "field3", types::logical_type::DOUBLE));
        }
    }

    SECTION("Type conflict detection - same path different type should fail") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.typed() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // First INSERT with integer value
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.typed (_id, value) VALUES ('t1', 42);");
            REQUIRE(cur->is_success());
        }

        // Verify value is BIGINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("typed"))});
            REQUIRE(cur->is_success());
            auto schema = cur->type_data()[0];
            REQUIRE(schema_has_field(schema, "value", types::logical_type::BIGINT));
        }

        // Second INSERT with string value for same field - should FAIL
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.typed (_id, value) VALUES ('t2', 'string_value');");

            // This should fail with type mismatch error
            REQUIRE_FALSE(cur->is_success());

            // Check error message contains type mismatch info
            auto error_msg = std::string(cur->get_error().what);
            bool has_type_error = error_msg.find("type") != std::string::npos ||
                                  error_msg.find("Type") != std::string::npos ||
                                  error_msg.find("mismatch") != std::string::npos;
            REQUIRE(has_type_error);
        }
    }

    SECTION("Multiple data types in schema") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.multitypes() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // INSERT with various types
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.multitypes "
                "(_id, str_field, int_field, float_field, bool_field) VALUES "
                "('m1', 'text', 42, 3.14, 1);");
            REQUIRE(cur->is_success());
        }

        // Verify all types in schema
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("multitypes"))});
            REQUIRE(cur->is_success());
            auto schema = cur->type_data()[0];

            REQUIRE(schema_has_field(schema, "_id", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "str_field", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "int_field", types::logical_type::BIGINT));
            REQUIRE(schema_has_field(schema, "float_field", types::logical_type::DOUBLE));
            // bool_field is stored as integer (1/0)
            REQUIRE(schema_has_field(schema, "bool_field", types::logical_type::BIGINT));
        }
    }

    SECTION("INSERT same type twice is OK") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.same_type() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // First INSERT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.same_type (_id, count) VALUES ('s1', 10);");
            REQUIRE(cur->is_success());
        }

        // Second INSERT with same type - should succeed
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.same_type (_id, count) VALUES ('s2', 20);");
            REQUIRE(cur->is_success());
        }

        // Verify data is correct
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM catalog_testdb.same_type;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }

    SECTION("Batch INSERT updates catalog") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.batch() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // Batch INSERT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.batch (_id, name, score) VALUES "
                "('b1', 'Alice', 100), "
                "('b2', 'Bob', 90), "
                "('b3', 'Charlie', 80);");
            REQUIRE(cur->is_success());
        }

        // Verify schema
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->get_schema(
                session,
                {std::make_pair(std::string("catalog_testdb"), std::string("batch"))});
            REQUIRE(cur->is_success());
            auto schema = cur->type_data()[0];

            REQUIRE(schema_has_field(schema, "_id", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "name", types::logical_type::STRING_LITERAL));
            REQUIRE(schema_has_field(schema, "score", types::logical_type::BIGINT));
        }

        // Verify data
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM catalog_testdb.batch;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }

    SECTION("Nested field type conflict") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.nested() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // INSERT with nested path as integer
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.nested (_id, data_dot_value) VALUES ('n1', 100);");
            REQUIRE(cur->is_success());
        }

        // Try INSERT same path as string - should fail
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.nested (_id, data_dot_value) VALUES ('n2', 'text');");
            REQUIRE_FALSE(cur->is_success());
        }
    }

    SECTION("SELECT after type-checked INSERT") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.select_test() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // Insert data
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.select_test (_id, name, amount) VALUES "
                "('st1', 'Item1', 100), "
                "('st2', 'Item2', 200);");
            REQUIRE(cur->is_success());
        }

        // SELECT with WHERE
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM catalog_testdb.select_test WHERE amount > 150;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        // SELECT with ORDER BY
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM catalog_testdb.select_test ORDER BY amount DESC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }

    SECTION("Aggregation with catalog") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE catalog_testdb.agg_test() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }

        // Insert data
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO catalog_testdb.agg_test (_id, category, amount) VALUES "
                "('a1', 'A', 100), "
                "('a2', 'A', 200), "
                "('a3', 'B', 150);");
            REQUIRE(cur->is_success());
        }

        // GROUP BY
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT category, SUM(amount) FROM catalog_testdb.agg_test GROUP BY category;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        // COUNT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT COUNT(*) FROM catalog_testdb.agg_test;");
            REQUIRE(cur->is_success());
        }
    }
}
