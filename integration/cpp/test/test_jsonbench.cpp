#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <set>

static const database_name_t database_name = "bluesky_bench";
static const collection_name_t collection_name = "bluesky";

using components::cursor::cursor_t_ptr;

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
    
    std::cout << "Read " << lines.size() << " lines from " << filepath << std::endl;
    return lines;
}

// Helper to extract DIDs from query results
std::set<std::string> extract_dids_from_cursor(cursor_t_ptr& cur) {
    std::set<std::string> dids;
    for (size_t i = 0; i < cur->size(); ++i) {
        auto doc = cur->get_document(i);
        if (doc && doc->is_exists("/did")) {
            dids.insert(std::string(doc->get_string("/did")));
        }
    }
    return dids;
}

TEST_CASE("JSONBench - Compare document_table vs document results") {
    // Read test data
    std::string data_path = "test_sample.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Testing with " << json_lines.size() << " records" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Test document_table
    cursor_t_ptr doc_table_results;
    {
        std::cout << "\n[1/2] Testing document_table storage..." << std::endl;
        auto config = test_create_config("/tmp/test_jsonbench_dt");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE bluesky_bench.bluesky() WITH (storage='document_table');");
            REQUIRE(cur->is_success());
            std::cout << "✓ Table created" << std::endl;
        }
        
        // Insert data
        {
            auto session = otterbrix::session_id_t();
            std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
            
            for (size_t i = 0; i < json_lines.size(); ++i) {
                try {
                    auto doc = components::document::document_t::document_from_json(
                        json_lines[i], 
                        dispatcher->resource()
                    );
                    documents.push_back(doc);
                } catch (const std::exception& e) {
                    std::cout << "✗ Failed to parse record " << i << ": " << e.what() << std::endl;
                    REQUIRE(false);
                }
            }
            
            auto start = std::chrono::high_resolution_clock::now();
            dispatcher->insert_many(session, database_name, collection_name, documents);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "✓ Inserted " << documents.size() << " records in " 
                      << duration.count() << "ms (" 
                      << (documents.size() * 1000.0 / duration.count()) << " rec/s)" << std::endl;
        }
        
        // Query all records
        {
            auto session = otterbrix::session_id_t();
            auto start = std::chrono::high_resolution_clock::now();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM bluesky_bench.bluesky;");
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            REQUIRE(cur->is_success());
            doc_table_results = cur;
            std::cout << "✓ SELECT * returned " << cur->size() 
                      << " records in " << duration.count() << "ms" << std::endl;
        }
        
        // Query with filter
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM bluesky_bench.bluesky WHERE kind = 'commit' LIMIT 10;");
            REQUIRE(cur->is_success());
            std::cout << "✓ SELECT with WHERE returned " << cur->size() << " records" << std::endl;
        }
    }

    // Test document (B-tree)
    cursor_t_ptr document_results;
    {
        std::cout << "\n[2/2] Testing document (B-tree) storage..." << std::endl;
        auto config = test_create_config("/tmp/test_jsonbench_doc");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE bluesky_bench.bluesky() WITH (storage='documents');");
            REQUIRE(cur->is_success());
            std::cout << "✓ Table created" << std::endl;
        }
        
        // Insert data
        {
            auto session = otterbrix::session_id_t();
            std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
            
            for (size_t i = 0; i < json_lines.size(); ++i) {
                try {
                    auto doc = components::document::document_t::document_from_json(
                        json_lines[i], 
                        dispatcher->resource()
                    );
                    documents.push_back(doc);
                } catch (const std::exception& e) {
                    std::cout << "✗ Failed to parse record " << i << ": " << e.what() << std::endl;
                    REQUIRE(false);
                }
            }
            
            auto start = std::chrono::high_resolution_clock::now();
            dispatcher->insert_many(session, database_name, collection_name, documents);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "✓ Inserted " << documents.size() << " records in " 
                      << duration.count() << "ms (" 
                      << (documents.size() * 1000.0 / duration.count()) << " rec/s)" << std::endl;
        }
        
        // Query all records
        {
            auto session = otterbrix::session_id_t();
            auto start = std::chrono::high_resolution_clock::now();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM bluesky_bench.bluesky;");
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            REQUIRE(cur->is_success());
            document_results = cur;
            std::cout << "✓ SELECT * returned " << cur->size() 
                      << " records in " << duration.count() << "ms" << std::endl;
        }
        
        // Query with filter
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM bluesky_bench.bluesky WHERE kind = 'commit' LIMIT 10;");
            REQUIRE(cur->is_success());
            std::cout << "✓ SELECT with WHERE returned " << cur->size() << " records" << std::endl;
        }
    }
    
    // Compare results
    {
        std::cout << "\n========================================" << std::endl;
        std::cout << "COMPARISON RESULTS" << std::endl;
        std::cout << "========================================" << std::endl;
        
        REQUIRE(doc_table_results);
        REQUIRE(document_results);
        
        std::cout << "document_table returned: " << doc_table_results->size() << " records" << std::endl;
        std::cout << "document returned:       " << document_results->size() << " records" << std::endl;
        
        REQUIRE(doc_table_results->size() == document_results->size());
        std::cout << "✓ Both storages returned same number of records" << std::endl;
        
        // Extract and compare DIDs
        auto dt_dids = extract_dids_from_cursor(doc_table_results);
        auto doc_dids = extract_dids_from_cursor(document_results);
        
        std::cout << "document_table unique DIDs: " << dt_dids.size() << std::endl;
        std::cout << "document unique DIDs:       " << doc_dids.size() << std::endl;
        
        REQUIRE(dt_dids.size() == doc_dids.size());
        std::cout << "✓ Both storages have same number of unique DIDs" << std::endl;
        
        // Check for differences
        std::vector<std::string> only_in_dt;
        std::vector<std::string> only_in_doc;
        
        for (const auto& did : dt_dids) {
            if (doc_dids.find(did) == doc_dids.end()) {
                only_in_dt.push_back(did);
            }
        }
        
        for (const auto& did : doc_dids) {
            if (dt_dids.find(did) == dt_dids.end()) {
                only_in_doc.push_back(did);
            }
        }
        
        if (!only_in_dt.empty()) {
            std::cout << "✗ DIDs only in document_table: " << only_in_dt.size() << std::endl;
            for (size_t i = 0; i < std::min(size_t(5), only_in_dt.size()); ++i) {
                std::cout << "  - " << only_in_dt[i] << std::endl;
            }
        }
        
        if (!only_in_doc.empty()) {
            std::cout << "✗ DIDs only in document: " << only_in_doc.size() << std::endl;
            for (size_t i = 0; i < std::min(size_t(5), only_in_doc.size()); ++i) {
                std::cout << "  - " << only_in_doc[i] << std::endl;
            }
        }
        
        REQUIRE(only_in_dt.empty());
        REQUIRE(only_in_doc.empty());
        std::cout << "✓ Both storages contain exactly the same DIDs" << std::endl;
        
        // Sample a few documents for detailed comparison
        std::cout << "\nSampling 3 documents for detailed comparison:" << std::endl;
        for (size_t i = 0; i < std::min(size_t(3), doc_table_results->size()); ++i) {
            auto dt_doc = doc_table_results->get_document(i);
            auto doc_doc = document_results->get_document(i);
            
            if (dt_doc && doc_doc) {
                std::cout << "\nDocument " << (i+1) << ":" << std::endl;
                std::cout << "  document_table DID: " << dt_doc->get_string("/did") << std::endl;
                std::cout << "  document DID:       " << doc_doc->get_string("/did") << std::endl;
                
                bool did_match = dt_doc->get_string("/did") == doc_doc->get_string("/did");
                std::cout << "  DIDs match: " << (did_match ? "✓" : "✗") << std::endl;
                
                if (dt_doc->is_exists("/kind") && doc_doc->is_exists("/kind")) {
                    bool kind_match = dt_doc->get_string("/kind") == doc_doc->get_string("/kind");
                    std::cout << "  Kind match: " << (kind_match ? "✓" : "✗") << std::endl;
                }
            }
        }
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "✅ ALL CHECKS PASSED" << std::endl;
        std::cout << "========================================\n" << std::endl;
    }
}
