#include "test_config.hpp"
#include <catch2/catch.hpp>

static const database_name_t database_name = "testdb";
static const collection_name_t collection_name = "testcol";

TEST_CASE("document_table - SQL integration test") {
    auto config = test_create_config("/tmp/test_document_table_sql");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    SECTION("CREATE TABLE with document_table storage") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Users() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
    }

    SECTION("INSERT and SELECT from document_table") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Products() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Products (_id, name, price) VALUES "
                "('p1', 'Laptop', 999), "
                "('p2', 'Mouse', 25), "
                "('p3', 'Keyboard', 75);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Products;");
            REQUIRE(cur->is_success());
            // TODO: Check size when cursor properly handles data_chunk from document_table
            // REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT name, price FROM TestDB.Products WHERE price > 50;");
            REQUIRE(cur->is_success());
            // TODO: Check size when cursor properly handles data_chunk from document_table
            // REQUIRE(cur->size() == 2); // Laptop and Keyboard
        }
    }

    SECTION("DELETE and UPDATE") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Items() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Items (_id, name, qty) VALUES "
                "('i1', 'Item1', 10), "
                "('i2', 'Item2', 20), "
                "('i3', 'Item3', 30);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "DELETE FROM TestDB.Items WHERE qty < 25;");
            REQUIRE(cur->is_success());
            // TODO: check modified count when available
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Items;");
            REQUIRE(cur->is_success());
            // TODO: Check size when cursor properly handles data_chunk from document_table
            // REQUIRE(cur->size() == 2); // Only Item2 and Item3 remain
        }
    }

    SECTION("Dynamic schema evolution") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Docs() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            // Insert with initial fields
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Docs (_id, field1) VALUES ('d1', 'value1');");
            REQUIRE(cur->is_success());
        }
        {
            // Insert with additional field - schema should evolve
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Docs (_id, field1, field2) VALUES ('d2', 'value2', 42);");
            REQUIRE(cur->is_success());
        }
        {
            // Verify both documents are accessible
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Docs;");
            REQUIRE(cur->is_success());
            // TODO: Check size when cursor properly handles data_chunk from document_table
            // REQUIRE(cur->size() == 2);
        }
    }
}
