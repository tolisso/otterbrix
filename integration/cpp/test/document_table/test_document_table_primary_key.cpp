#include "../test_config.hpp"
#include <catch2/catch.hpp>
#include <chrono>
#include <set>
#include <iomanip>

static const database_name_t database_name = "test_pk_db";
static const collection_name_t collection_name = "users";

using components::cursor::cursor_t_ptr;
namespace types = components::types;

TEST_CASE("document_table: primary key scan - basic findOne", "[integration][document_table][primary_key]") {
    auto config = test_create_config("/tmp/test_pk_basic");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Create database and collection
    {
        auto session = otterbrix::session_id_t();
        dispatcher->create_database(session, database_name);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "CREATE TABLE test_pk_db.users() WITH (storage='document_table');");
        REQUIRE(cur->is_success());
    }

    // Insert test documents
    {
        auto session = otterbrix::session_id_t();
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
        
        for (int i = 0; i < 10; ++i) {
            auto doc = components::document::make_document(dispatcher->resource());
            
            // Generate unique ObjectId-like hex string (24 chars)
            char id_hex[25];
            snprintf(id_hex, sizeof(id_hex), "507f1f77bcf86cd7994%05d", i);
            
            doc->set("/_id", std::string(id_hex));
            doc->set("/name", "User" + std::to_string(i));
            doc->set("/age", static_cast<int64_t>(20 + i));
            doc->set("/email", "user" + std::to_string(i) + "@example.com");
            
            documents.push_back(doc);
        }
        
        dispatcher->insert_many(session, database_name, collection_name, documents);
        std::cout << "✓ Inserted " << documents.size() << " documents" << std::endl;
    }

    SECTION("SELECT with WHERE _id = '...' should use primary_key_scan") {
        auto session = otterbrix::session_id_t();
        
        // Search for specific document by _id
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM test_pk_db.users WHERE _id = '507f1f77bcf86cd799400005';");
        
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        
        // Verify we got the right document
        if (cur->uses_table_data()) {
            auto& chunk = cur->chunk_data();
            
            // Find the name column
            for (size_t col = 0; col < chunk.column_count(); ++col) {
                auto col_type = chunk.value(col, 0).type();
                if (col_type.alias() == "name") {
                    auto name_val = chunk.value(col, 0);
                    std::string name = std::string(name_val.value<std::string_view>());
                    REQUIRE(name == "User5");
                    std::cout << "✓ Found document by _id: name=" << name << std::endl;
                    break;
                }
            }
        }
    }

    SECTION("Search for non-existent _id should return empty") {
        auto session = otterbrix::session_id_t();
        
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM test_pk_db.users WHERE _id = '507f1f77bcf86cd799999999';");
        
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
        std::cout << "✓ Non-existent _id returned empty result" << std::endl;
    }
}

TEST_CASE("document_table: primary key scan - performance test", "[integration][document_table][primary_key][performance]") {
    auto config = test_create_config("/tmp/test_pk_performance");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Create database and collection
    {
        auto session = otterbrix::session_id_t();
        dispatcher->create_database(session, database_name);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "CREATE TABLE test_pk_db.users() WITH (storage='document_table');");
        REQUIRE(cur->is_success());
    }

    // Insert many documents
    const size_t NUM_DOCS = 10000;
    std::vector<std::string> inserted_ids;
    
    {
        auto session = otterbrix::session_id_t();
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Insert in batches
        constexpr size_t batch_size = 1000;
        for (size_t batch_start = 0; batch_start < NUM_DOCS; batch_start += batch_size) {
            size_t batch_end = std::min(batch_start + batch_size, NUM_DOCS);
            std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
            
            for (size_t i = batch_start; i < batch_end; ++i) {
                auto doc = components::document::make_document(dispatcher->resource());
                
                // Generate unique ObjectId-like hex string (24 chars)
                char id_hex[25];
                snprintf(id_hex, sizeof(id_hex), "507f1f77bcf86cd7%08zu", i);
                inserted_ids.push_back(id_hex);
                
                doc->set("/_id", std::string(id_hex));
                doc->set("/name", "User" + std::to_string(i));
                doc->set("/age", static_cast<int64_t>(20 + (i % 50)));
                doc->set("/email", "user" + std::to_string(i) + "@example.com");
                
                documents.push_back(doc);
            }
            
            dispatcher->insert_many(session, database_name, collection_name, documents);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        std::cout << "✓ Inserted " << NUM_DOCS << " documents in " 
                  << duration.count() << "ms (" 
                  << (NUM_DOCS * 1000.0 / duration.count()) << " rec/s)" << std::endl;
    }

    SECTION("primary_key_scan should be fast (< 10ms) on large dataset") {
        auto session = otterbrix::session_id_t();
        
        // Search for document in the middle
        std::string target_id = inserted_ids[NUM_DOCS / 2];
        
        auto start = std::chrono::high_resolution_clock::now();
        
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM test_pk_db.users WHERE _id = '" + target_id + "';");
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        
        std::cout << "✓ Primary key lookup in " << NUM_DOCS << " documents took: " 
                  << duration.count() << " μs" << std::endl;
        
        // Should be fast even on large dataset (< 10ms)
        REQUIRE(duration.count() < 10000); // < 10ms
    }

    SECTION("compare primary_key_scan vs full scan performance") {
        auto session = otterbrix::session_id_t();
        std::string target_id = inserted_ids[NUM_DOCS / 2];
        
        // Time primary key lookup
        auto pk_start = std::chrono::high_resolution_clock::now();
        auto pk_cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM test_pk_db.users WHERE _id = '" + target_id + "';");
        auto pk_end = std::chrono::high_resolution_clock::now();
        auto pk_duration = std::chrono::duration_cast<std::chrono::microseconds>(pk_end - pk_start);
        
        REQUIRE(pk_cur->is_success());
        REQUIRE(pk_cur->size() == 1);
        
        // Time full scan
        auto full_start = std::chrono::high_resolution_clock::now();
        auto full_cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM test_pk_db.users;");
        auto full_end = std::chrono::high_resolution_clock::now();
        auto full_duration = std::chrono::duration_cast<std::chrono::microseconds>(full_end - full_start);
        
        REQUIRE(full_cur->is_success());
        REQUIRE(full_cur->size() == NUM_DOCS);
        
        double speedup = (double)full_duration.count() / pk_duration.count();
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "Performance Comparison (" << NUM_DOCS << " documents):" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Primary key scan:  " << std::setw(8) << pk_duration.count() << " μs" << std::endl;
        std::cout << "Full scan:         " << std::setw(8) << full_duration.count() << " μs" << std::endl;
        std::cout << "Speedup:           " << std::setw(8) << std::fixed << std::setprecision(1) 
                  << speedup << "x faster" << std::endl;
        std::cout << "========================================\n" << std::endl;
        
        // Primary key scan should be significantly faster (at least 5x on 10K docs)
        REQUIRE(speedup > 5.0);
    }
}

TEST_CASE("document_table: primary key scan - multiple lookups", "[integration][document_table][primary_key]") {
    auto config = test_create_config("/tmp/test_pk_multiple");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Create database and collection
    {
        auto session = otterbrix::session_id_t();
        dispatcher->create_database(session, database_name);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "CREATE TABLE test_pk_db.users() WITH (storage='document_table');");
        REQUIRE(cur->is_success());
    }

    // Insert test documents
    std::vector<std::string> ids = {
        "507f1f77bcf86cd799400001",
        "507f1f77bcf86cd799400002",
        "507f1f77bcf86cd799400003",
        "507f1f77bcf86cd799400004",
        "507f1f77bcf86cd799400005"
    };
    
    {
        auto session = otterbrix::session_id_t();
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
        
        for (size_t i = 0; i < ids.size(); ++i) {
            auto doc = components::document::make_document(dispatcher->resource());
            doc->set("/_id", ids[i]);
            doc->set("/name", "User" + std::to_string(i));
            doc->set("/status", "active");
            documents.push_back(doc);
        }
        
        dispatcher->insert_many(session, database_name, collection_name, documents);
    }

    SECTION("consecutive lookups should all be fast") {
        auto session = otterbrix::session_id_t();
        
        for (const auto& id : ids) {
            auto start = std::chrono::high_resolution_clock::now();
            
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM test_pk_db.users WHERE _id = '" + id + "';");
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            
            std::cout << "✓ Lookup " << id << " took " << duration.count() << " μs" << std::endl;
            
            // Each lookup should be fast
            REQUIRE(duration.count() < 1000); // < 1ms
        }
    }
}

