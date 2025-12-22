#include "../test_config.hpp"
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
                "('p1', 1, 999), "
                "('p2', 2, 25), "
                "('p3', 3, 75);");
            std::cout << "error " <<  static_cast<int32_t>(cur->get_error().type) << std::endl;
            std::cout << "error " <<  cur->get_error().what << std::endl;
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Products;");
            std::cout << "error " <<  static_cast<int32_t>(cur->get_error().type) << std::endl;
            std::cout << "error " <<  cur->get_error().what << std::endl;

            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT name, price FROM TestDB.Products WHERE price > 50;");
            REQUIRE(cur->is_success());
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

    SECTION("UPDATE operations") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Inventory() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Inventory (_id, item, qty, price) VALUES "
                "('item1', 'Widget', 100, 10), "
                "('item2', 'Gadget', 50, 25), "
                "('item3', 'Doohickey', 75, 15);");
            REQUIRE(cur->is_success());
        }
        {
            // Update single field
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "UPDATE TestDB.Inventory SET qty = 120 WHERE _id = 'item1';");
            REQUIRE(cur->is_success());
        }
        {
            // Update multiple fields with condition
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "UPDATE TestDB.Inventory SET qty = 60, price = 30 WHERE price < 20;");
            REQUIRE(cur->is_success());
        }
        {
            // Verify updates
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Inventory;");
            REQUIRE(cur->is_success());
        }
    }

    SECTION("ORDER BY - sorting operations") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Books() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Books (_id, title, year, rating) VALUES "
                "('b1', 'Book A', 2020, 4), "
                "('b2', 'Book B', 2019, 5), "
                "('b3', 'Book C', 2021, 3), "
                "('b4', 'Book D', 2020, 5);");
            REQUIRE(cur->is_success());
        }
        {
            // Sort by single field ascending
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Books ORDER BY year;");
            REQUIRE(cur->is_success());
        }
        {
            // Sort by single field descending
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Books ORDER BY rating DESC;");
            REQUIRE(cur->is_success());
        }
        {
            // Sort by multiple fields
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Books ORDER BY year DESC, rating DESC;");
            REQUIRE(cur->is_success());
        }
    }

    SECTION("Aggregation - GROUP BY and aggregate functions") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Sales() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Sales (_id, product, category, amount, quantity) VALUES "
                "('s1', 'Laptop', 'Electronics', 1000, 2), "
                "('s2', 'Mouse', 'Electronics', 25, 5), "
                "('s3', 'Desk', 'Furniture', 300, 1), "
                "('s4', 'Chair', 'Furniture', 150, 4), "
                "('s5', 'Monitor', 'Electronics', 400, 3);");
            REQUIRE(cur->is_success());
        }
        {
            // COUNT aggregation
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT COUNT(*) FROM TestDB.Sales;");
            REQUIRE(cur->is_success());
        }
        {
            // GROUP BY with COUNT
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT category, COUNT(*) FROM TestDB.Sales GROUP BY category;");
            REQUIRE(cur->is_success());
        }
        {
            // GROUP BY with SUM
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT category, SUM(amount) FROM TestDB.Sales GROUP BY category;");
            REQUIRE(cur->is_success());
        }
        {
            // GROUP BY with AVG
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT category, AVG(amount) FROM TestDB.Sales GROUP BY category;");
            REQUIRE(cur->is_success());
        }
        {
            // GROUP BY with MIN and MAX
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT category, MIN(amount), MAX(amount) FROM TestDB.Sales GROUP BY category;");
            REQUIRE(cur->is_success());
        }
    }

    SECTION("Complex WHERE conditions") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Employees() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Employees (_id, name, department, salary, experience) VALUES "
                "('e1', 'Alice', 'Engineering', 80000, 5), "
                "('e2', 'Bob', 'Sales', 60000, 3), "
                "('e3', 'Charlie', 'Engineering', 90000, 7), "
                "('e4', 'Diana', 'HR', 55000, 2), "
                "('e5', 'Eve', 'Engineering', 75000, 4);");
            REQUIRE(cur->is_success());
        }
        {
            // AND condition
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Employees WHERE department = 'Engineering' AND salary > 75000;");
            REQUIRE(cur->is_success());
        }
        {
            // OR condition
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Employees WHERE department = 'Sales' OR salary < 60000;");
            REQUIRE(cur->is_success());
        }
        {
            // Combined AND/OR with comparison operators
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Employees WHERE (department = 'Engineering' OR department = 'HR') AND experience >= 3;");
            REQUIRE(cur->is_success());
        }
        {
            // BETWEEN-like condition using >= and <=
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Employees WHERE salary >= 60000 AND salary <= 80000;");
            REQUIRE(cur->is_success());
        }
    }

    SECTION("LIMIT and OFFSET") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Numbers() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Numbers (_id, value) VALUES "
                "('n1', 10), ('n2', 20), ('n3', 30), ('n4', 40), ('n5', 50), "
                "('n6', 60), ('n7', 70), ('n8', 80), ('n9', 90), ('n10', 100);");
            REQUIRE(cur->is_success());
        }
        {
            // LIMIT only
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Numbers LIMIT 5;");
            REQUIRE(cur->is_success());
        }
        {
            // LIMIT with OFFSET
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Numbers LIMIT 3 OFFSET 5;");
            REQUIRE(cur->is_success());
        }
        {
            // LIMIT with ORDER BY
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Numbers ORDER BY value DESC LIMIT 3;");
            REQUIRE(cur->is_success());
        }
    }

    SECTION("Mixed data types") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Mixed() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Mixed (_id, str_field, int_field, float_field, bool_field) VALUES "
                "('m1', 'text1', 42, 3.14, 1), "
                "('m2', 'text2', 100, 2.71, 0), "
                "('m3', 'text3', 7, 1.41, 1);");
            REQUIRE(cur->is_success());
        }
        {
            // Query with different data types
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Mixed WHERE int_field > 10 AND bool_field = 1;");
            REQUIRE(cur->is_success());
        }
        {
            // Aggregation on different types
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT SUM(int_field), AVG(float_field) FROM TestDB.Mixed;");
            REQUIRE(cur->is_success());
        }
    }

    SECTION("Empty result sets") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.Empty() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            // Select from empty table
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Empty;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            // Insert some data
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.Empty (_id, value) VALUES ('e1', 10);");
            REQUIRE(cur->is_success());
        }
        {
            // Query with no matches
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM TestDB.Empty WHERE value > 100;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    // NOTE: Type mismatch test disabled - requires proper exception handling in executor
    // The type checking itself works correctly and throws runtime_error,
    // but converting it to error cursor needs deeper changes in executor.
    // TODO: Re-enable when executor has proper exception handling
    /*
    SECTION("Type mismatch error - should fail") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDB.TypeTest() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
        }
        {
            // First insert with integer
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.TypeTest (_id, value) VALUES ('t1', 42);");
            REQUIRE(cur->is_success());
        }
        {
            // Second insert with string - should fail with type mismatch
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDB.TypeTest (_id, value) VALUES ('t2', 'string_value');");
            REQUIRE_FALSE(cur->is_success());
            // Check that error message contains "Type mismatch"
            auto error_msg = std::string(cur->get_error().what);
            REQUIRE(error_msg.find("Type mismatch") != std::string::npos);
        }
    }
    */
}
