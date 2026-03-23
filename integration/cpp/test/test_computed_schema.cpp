#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/catalog/catalog.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>

// Tests for computed_schema (dynamic per-type columnar storage)
// A computed-schema table is created with CREATE TABLE db.t() — no fixed columns.
// Each INSERT can bring new (field_name, type) pairs; each becomes a separate physical column.
// Subsequent INSERTs with the same field_name but different type create additional columns.

static const database_name_t cs_db = "cs_testdb";

TEST_CASE("integration::cpp::test_computed_schema::basic_insert_and_select") {
    auto config = test_create_config("/tmp/test_computed_schema/basic");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t1 ();");
        REQUIRE(cur->is_success());
    }

    // First INSERT: introduces id (bigint) and name (string)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.t1 (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // SELECT * should return 2 rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }

    // Second INSERT: same schema
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.t1 (id, name) VALUES (3, 'Charlie');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // SELECT * should now return 3 rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::evolving_schema") {
    auto config = test_create_config("/tmp/test_computed_schema/evolving");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t2 ();");
        REQUIRE(cur->is_success());
    }

    // INSERT with only 'id' column
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t2 (id) VALUES (1), (2), (3);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    // INSERT with 'id' and 'value' — 'value' is a new column
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t2 (id, value) VALUES (4, 100);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // All 4 rows should be returned; rows 1-3 have NULL for 'value'
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }

    // WHERE on 'value' should find only row 4
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t2 WHERE value = 100;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::delete_rows") {
    auto config = test_create_config("/tmp/test_computed_schema/delete");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t3 ();");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO cs_testdb.t3 (id, name) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM cs_testdb.t3 WHERE id <= 2;");
        REQUIRE(cur->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::multi_type_field") {
    auto config = test_create_config("/tmp/test_computed_schema/multi_type");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE cs_testdb.t4 ();");
        REQUIRE(cur->is_success());
    }

    // Insert 'id' as bigint
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t4 (id, val) VALUES (1, 1), (2, 2);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // Insert 'id' as string — creates a second physical column id__STRING_LITERAL
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO cs_testdb.t4 (id, val) VALUES (3, 'hello');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // SELECT * must fail: 'id' has two physical types (bigint and string)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t4;");
        REQUIRE_FALSE(cur->is_success());
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().what == "column 'val' has multiple types; use explicit column selection");
    }

    // Select val as string: rows 1-2 have NULL for val (no string was inserted), row 3 has "hello"
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT id, val::string FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).is_null());
        REQUIRE(cur->chunk_data().value(1, 1).is_null());
        { auto v = cur->chunk_data().value(1, 2); REQUIRE(v.value<const std::string&>() == "hello"); }
    }

    // Select val as bigint: rows 1-2 have (1,2), row 3 has NULL for val::bigint (no bigint was inserted)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT id, val::bigint FROM cs_testdb.t4 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 2);
        REQUIRE(cur->chunk_data().value(1, 2).is_null());
    }

    // WHERE val::bigint
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT id, val::bigint FROM cs_testdb.t4 WHERE val::bigint > 0 ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 2);
    }
}

TEST_CASE("integration::cpp::test_computed_schema::sparse_columns") {
    // Create a computing table with sparse_threshold=5 via C++ API.
    // Inserting a column with < 5 rows should route it to a shadow collection.
    auto config = test_create_config("/tmp/test_computed_schema/sparse");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    collection_full_name_t main_coll{"cs_testdb", "t_sparse"};

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE cs_testdb;");
    }

    // Create computing table with sparse_threshold=5 via C++ API
    {
        auto session = otterbrix::session_id_t();
        auto create_node = components::logical_plan::make_node_create_collection(resource, main_coll, 5);
        auto cur = dispatcher->execute_plan(session, create_node);
        REQUIRE(cur->is_success());
    }

    // Insert 2 rows: id (bigint=12) and name (string=13) — both below threshold=5, go to shadow
    {
        auto session = otterbrix::session_id_t();
        using namespace components::types;
        complex_logical_type id_type{logical_type::BIGINT};
        id_type.set_alias("id");
        complex_logical_type name_type{logical_type::STRING_LITERAL};
        name_type.set_alias("name");

        std::pmr::vector<complex_logical_type> types(resource);
        types.push_back(id_type);
        types.push_back(name_type);

        components::vector::data_chunk_t chunk(resource, types, 2);
        chunk.set_value(0, 0, logical_value_t{resource, int64_t(1)});
        chunk.set_value(0, 1, logical_value_t{resource, int64_t(2)});
        chunk.set_value(1, 0, logical_value_t{resource, std::string("Alice")});
        chunk.set_value(1, 1, logical_value_t{resource, std::string("Bob")});
        chunk.set_cardinality(2);

        auto insert_node = components::logical_plan::make_node_insert(resource, main_coll, std::move(chunk));
        auto cur = dispatcher->execute_plan(session, insert_node);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    // Main table should have __rowid__ column but no id/name columns (they went to shadow)
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t_sparse;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // Only __rowid__ column in main table
        REQUIRE(cur->chunk_data().column_count() == 1);
    }

    // Shadow collection for 'id' (BIGINT=14) should exist and contain 2 rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t_sparse__sp__id__14;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2); // __rowid__ + value
    }

    // Shadow collection for 'name' (STRING_LITERAL=35) should exist and contain 2 rows
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t_sparse__sp__name__35;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2); // __rowid__ + value
    }

    // Insert 4 more rows of id — cumulative count becomes 2+4=6, but at time of this INSERT count=2 < 5,
    // so these 4 rows still go to shadow. After this INSERT shadow has 6 id rows total.
    {
        auto session = otterbrix::session_id_t();
        using namespace components::types;
        complex_logical_type id_type{logical_type::BIGINT};
        id_type.set_alias("id");

        std::pmr::vector<complex_logical_type> col_types(resource);
        col_types.push_back(id_type);

        components::vector::data_chunk_t chunk(resource, col_types, 4);
        chunk.set_value(0, 0, logical_value_t{resource, int64_t(3)});
        chunk.set_value(0, 1, logical_value_t{resource, int64_t(4)});
        chunk.set_value(0, 2, logical_value_t{resource, int64_t(5)});
        chunk.set_value(0, 3, logical_value_t{resource, int64_t(6)});
        chunk.set_cardinality(4);

        auto insert_node = components::logical_plan::make_node_insert(resource, main_coll, std::move(chunk));
        auto cur = dispatcher->execute_plan(session, insert_node);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
    }

    // After two INSERTs, id count=6 accumulated in shadow. Main table still has only __rowid__.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t_sparse;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 6);
        REQUIRE(cur->chunk_data().column_count() == 1); // only __rowid__
    }

    // Now insert 1 more id row. At this point count=6 >= threshold=5, so it goes to main table.
    {
        auto session = otterbrix::session_id_t();
        using namespace components::types;
        complex_logical_type id_type{logical_type::BIGINT};
        id_type.set_alias("id");

        std::pmr::vector<complex_logical_type> col_types(resource);
        col_types.push_back(id_type);

        components::vector::data_chunk_t chunk(resource, col_types, 1);
        chunk.set_value(0, 0, logical_value_t{resource, int64_t(7)});
        chunk.set_cardinality(1);

        auto insert_node = components::logical_plan::make_node_insert(resource, main_coll, std::move(chunk));
        auto cur = dispatcher->execute_plan(session, insert_node);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // Main table now has __rowid__ + id columns (total 2), 7 rows total
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM cs_testdb.t_sparse;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 7);
        REQUIRE(cur->chunk_data().column_count() == 2); // __rowid__ + id
    }
}
