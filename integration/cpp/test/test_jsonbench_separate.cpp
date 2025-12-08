#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <set>
#include <iomanip>
#include <algorithm>

static const database_name_t database_name = "bench_db";
static const collection_name_t collection_name = "records";

using components::cursor::cursor_t_ptr;
namespace types = components::types;

namespace {

// Helper to read NDJSON file
std::vector<std::string> read_ndjson_file(const std::string& filepath) {
    std::vector<std::string> lines;
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return lines;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    
    return lines;
}

} // anonymous namespace

TEST_CASE("JSONBench 1: INSERT Performance", "[jsonbench][insert]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║         JSONBench: INSERT Performance Comparison             ║" << std::endl;
    std::cout << "║                  " << json_lines.size() << " records                                    ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;

    long long dt_insert_time = 0;
    long long doc_insert_time = 0;

    // Test 1: document_table
    {
        std::cout << "[1/2] Testing document_table INSERT..." << std::endl;
        auto config = test_create_config("/tmp/bench_insert_dt");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        dispatcher->create_database(otterbrix::session_id_t(), database_name);
        auto cur = dispatcher->execute_sql(
            otterbrix::session_id_t(),
            "CREATE TABLE bench_db.records() WITH (storage='document_table');");
        REQUIRE(cur->is_success());

        auto session = otterbrix::session_id_t();
        constexpr size_t batch_size = 1000;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (size_t batch_start = 0; batch_start < json_lines.size(); batch_start += batch_size) {
            size_t batch_end = std::min(batch_start + batch_size, json_lines.size());
            std::pmr::vector<components::document::document_ptr> batch_documents(dispatcher->resource());
            
            for (size_t i = batch_start; i < batch_end; ++i) {
                auto doc = components::document::document_t::document_from_json(
                    json_lines[i], 
                    dispatcher->resource()
                );
                batch_documents.push_back(doc);
            }
            
            dispatcher->insert_many(session, database_name, collection_name, batch_documents);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        dt_insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        std::cout << "  ✓ document_table: " << dt_insert_time << "ms ("
                  << (json_lines.size() * 1000.0 / dt_insert_time) << " rec/s)" << std::endl;
    }

    // Test 2: document (B-tree)
    {
        std::cout << "[2/2] Testing document INSERT..." << std::endl;
        auto config = test_create_config("/tmp/bench_insert_doc");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        dispatcher->create_database(otterbrix::session_id_t(), database_name);
        auto cur = dispatcher->execute_sql(
            otterbrix::session_id_t(),
            "CREATE TABLE bench_db.records() WITH (storage='documents');");
        REQUIRE(cur->is_success());

        auto session = otterbrix::session_id_t();
        constexpr size_t batch_size = 1000;
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (size_t batch_start = 0; batch_start < json_lines.size(); batch_start += batch_size) {
            size_t batch_end = std::min(batch_start + batch_size, json_lines.size());
            std::pmr::vector<components::document::document_ptr> batch_documents(dispatcher->resource());
            
            for (size_t i = batch_start; i < batch_end; ++i) {
                auto doc = components::document::document_t::document_from_json(
                    json_lines[i], 
                    dispatcher->resource()
                );
                batch_documents.push_back(doc);
            }
            
            dispatcher->insert_many(session, database_name, collection_name, batch_documents);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        doc_insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        std::cout << "  ✓ document:       " << doc_insert_time << "ms ("
                  << (json_lines.size() * 1000.0 / doc_insert_time) << " rec/s)" << std::endl;
    }

    // Summary
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║                    INSERT COMPARISON                         ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║ document_table:  " << std::setw(6) << dt_insert_time << " ms  ("
              << std::setw(7) << std::fixed << std::setprecision(1) 
              << (json_lines.size() * 1000.0 / dt_insert_time) << " rec/s)      ║" << std::endl;
    std::cout << "║ document:        " << std::setw(6) << doc_insert_time << " ms  ("
              << std::setw(7) << std::fixed << std::setprecision(1)
              << (json_lines.size() * 1000.0 / doc_insert_time) << " rec/s)      ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    
    if (doc_insert_time < dt_insert_time) {
        double speedup = (double)dt_insert_time / doc_insert_time;
        std::cout << "║ Winner: document (B-tree) - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster          ║" << std::endl;
    } else {
        double speedup = (double)doc_insert_time / dt_insert_time;
        std::cout << "║ Winner: document_table - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster             ║" << std::endl;
    }
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;
}

TEST_CASE("JSONBench 2: SELECT * Performance", "[jsonbench][select]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║         JSONBench: SELECT * Performance Comparison           ║" << std::endl;
    std::cout << "║                  " << json_lines.size() << " records                                    ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;

    long long dt_select_time = 0;
    long long doc_select_time = 0;
    size_t dt_count = 0;
    size_t doc_count = 0;

    // Test 1: document_table
    {
        std::cout << "[1/2] Testing document_table SELECT *..." << std::endl;
        auto config = test_create_config("/tmp/bench_select_dt");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Setup data
        dispatcher->create_database(otterbrix::session_id_t(), database_name);
        dispatcher->execute_sql(
            otterbrix::session_id_t(),
            "CREATE TABLE bench_db.records() WITH (storage='document_table');");
        
        auto session = otterbrix::session_id_t();
        std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
        for (const auto& line : json_lines) {
            docs.push_back(components::document::document_t::document_from_json(line, dispatcher->resource()));
        }
        dispatcher->insert_many(session, database_name, collection_name, docs);

        // Test SELECT *
        auto start = std::chrono::high_resolution_clock::now();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM bench_db.records;");
        auto end = std::chrono::high_resolution_clock::now();
        dt_select_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        REQUIRE(cur->is_success());
        dt_count = cur->size();
        
        std::cout << "  ✓ document_table: " << dt_select_time << "ms (" << dt_count << " records)" << std::endl;
    }

    // Test 2: document (B-tree)
    {
        std::cout << "[2/2] Testing document SELECT *..." << std::endl;
        auto config = test_create_config("/tmp/bench_select_doc");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Setup data
        dispatcher->create_database(otterbrix::session_id_t(), database_name);
        dispatcher->execute_sql(
            otterbrix::session_id_t(),
            "CREATE TABLE bench_db.records() WITH (storage='documents');");
        
        auto session = otterbrix::session_id_t();
        std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
        for (const auto& line : json_lines) {
            docs.push_back(components::document::document_t::document_from_json(line, dispatcher->resource()));
        }
        dispatcher->insert_many(session, database_name, collection_name, docs);

        // Test SELECT *
        auto start = std::chrono::high_resolution_clock::now();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM bench_db.records;");
        auto end = std::chrono::high_resolution_clock::now();
        doc_select_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        REQUIRE(cur->is_success());
        doc_count = cur->size();
        
        std::cout << "  ✓ document:       " << doc_select_time << "ms (" << doc_count << " records)" << std::endl;
    }

    // Summary
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║                  SELECT * COMPARISON                         ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║ document_table:  " << std::setw(6) << dt_select_time << " ms  (" << dt_count << " records)          ║" << std::endl;
    std::cout << "║ document:        " << std::setw(6) << doc_select_time << " ms  (" << doc_count << " records)          ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    
    if (doc_select_time < dt_select_time) {
        double speedup = (double)dt_select_time / doc_select_time;
        std::cout << "║ Winner: document (B-tree) - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster          ║" << std::endl;
    } else {
        double speedup = (double)doc_select_time / dt_select_time;
        std::cout << "║ Winner: document_table - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster             ║" << std::endl;
    }
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;
}

TEST_CASE("JSONBench 3: SELECT with WHERE Performance", "[jsonbench][where]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║      JSONBench: SELECT WHERE Performance Comparison          ║" << std::endl;
    std::cout << "║                  " << json_lines.size() << " records                                    ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;

    long long dt_where_time = 0;
    long long doc_where_time = 0;
    size_t dt_count = 0;
    size_t doc_count = 0;

    // Test 1: document_table
    {
        std::cout << "[1/2] Testing document_table SELECT WHERE..." << std::endl;
        auto config = test_create_config("/tmp/bench_where_dt");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Setup data
        dispatcher->create_database(otterbrix::session_id_t(), database_name);
        dispatcher->execute_sql(
            otterbrix::session_id_t(),
            "CREATE TABLE bench_db.records() WITH (storage='document_table');");
        
        auto session = otterbrix::session_id_t();
        std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
        for (const auto& line : json_lines) {
            docs.push_back(components::document::document_t::document_from_json(line, dispatcher->resource()));
        }
        dispatcher->insert_many(session, database_name, collection_name, docs);

        // Test SELECT WHERE
        auto start = std::chrono::high_resolution_clock::now();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM bench_db.records WHERE kind = 'commit';");
        auto end = std::chrono::high_resolution_clock::now();
        dt_where_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        REQUIRE(cur->is_success());
        dt_count = cur->size();
        
        std::cout << "  ✓ document_table: " << dt_where_time << "ms (" << dt_count << " records found)" << std::endl;
    }

    // Test 2: document (B-tree)
    {
        std::cout << "[2/2] Testing document SELECT WHERE..." << std::endl;
        auto config = test_create_config("/tmp/bench_where_doc");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Setup data
        dispatcher->create_database(otterbrix::session_id_t(), database_name);
        dispatcher->execute_sql(
            otterbrix::session_id_t(),
            "CREATE TABLE bench_db.records() WITH (storage='documents');");
        
        auto session = otterbrix::session_id_t();
        std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
        for (const auto& line : json_lines) {
            docs.push_back(components::document::document_t::document_from_json(line, dispatcher->resource()));
        }
        dispatcher->insert_many(session, database_name, collection_name, docs);

        // Test SELECT WHERE
        auto start = std::chrono::high_resolution_clock::now();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM bench_db.records WHERE kind = 'commit';");
        auto end = std::chrono::high_resolution_clock::now();
        doc_where_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        REQUIRE(cur->is_success());
        doc_count = cur->size();
        
        std::cout << "  ✓ document:       " << doc_where_time << "ms (" << doc_count << " records found)" << std::endl;
    }

    // Summary
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║               SELECT WHERE COMPARISON                        ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║ document_table:  " << std::setw(6) << dt_where_time << " ms  (" << dt_count << " records found)    ║" << std::endl;
    std::cout << "║ document:        " << std::setw(6) << doc_where_time << " ms  (" << doc_count << " records found)    ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    
    if (doc_where_time < dt_where_time) {
        double speedup = (double)dt_where_time / doc_where_time;
        std::cout << "║ Winner: document (B-tree) - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster          ║" << std::endl;
    } else {
        double speedup = (double)doc_where_time / dt_where_time;
        std::cout << "║ Winner: document_table - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster             ║" << std::endl;
    }
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;
}

