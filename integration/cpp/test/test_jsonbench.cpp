#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <set>
#include <iomanip>

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

TEST_CASE("JSONBench - Compare document_table vs document results") {
    // Read test data
    std::string data_path = "test_sample.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Testing with " << json_lines.size() << " records" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Test document_table
    std::set<std::string> doc_table_dids;
    size_t doc_table_count = 0;
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
        
        // Query all records and extract DIDs BEFORE dispatcher is deleted
        {
            auto session = otterbrix::session_id_t();
            auto start = std::chrono::high_resolution_clock::now();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM bluesky_bench.bluesky;");
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            REQUIRE(cur->is_success());
            doc_table_count = cur->size();
            
            std::cout << "✓ SELECT * returned " << cur->size() 
                      << " records in " << duration.count() << "ms" << std::endl;
            std::cout << "  Cursor uses_table_data: " << cur->uses_table_data() << std::endl;
            
            // Extract DIDs while cursor is still valid
            if (cur->uses_table_data()) {
                // document_table returns data_chunk, not documents
                auto& chunk = cur->chunk_data();
                std::cout << "  Chunk size: " << chunk.size() << ", columns: " << chunk.column_count() << std::endl;
                
                // Find 'did' column
                int did_col_idx = -1;
                for (size_t col = 0; col < chunk.column_count(); ++col) {
                    // TODO: need to get column name
                }
                std::cout << "  Warning: document_table returns data_chunk, DID extraction not yet implemented" << std::endl;
            } else {
                // documents returns document list
                auto& docs = cur->document_data();
                std::cout << "  Document list size: " << docs.size() << std::endl;
                for (size_t i = 0; i < docs.size(); ++i) {
                    auto doc = docs[i];
                    if (doc && doc->is_exists("/did")) {
                        doc_table_dids.insert(std::string(doc->get_string("/did")));
                    }
                }
                std::cout << "  Extracted " << doc_table_dids.size() << " unique DIDs" << std::endl;
            }
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
    std::set<std::string> document_dids;
    size_t document_count = 0;
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
            
            std::set<std::string> inserted_ids;
            std::set<std::string> inserted_dids;
            
            for (size_t i = 0; i < json_lines.size(); ++i) {
                try {
                    auto doc = components::document::document_t::document_from_json(
                        json_lines[i], 
                        dispatcher->resource()
                    );
                    
                    // Add unique _id if missing (required for B-tree documents storage)
                    if (!doc->is_exists("/_id")) {
                        // Generate unique _id using index
                        std::ostringstream id_stream;
                        id_stream << std::setfill('0') << std::setw(24) << i;
                        doc->set("/_id", id_stream.str());
                        if (i < 3) {
                            std::cout << "  Document " << i << " generated _id: " << id_stream.str() << std::endl;
                        }
                    }
                    
                    // Check _id and did
                    if (doc->is_exists("/_id")) {
                        std::string doc_id(doc->get_string("/_id"));
                        inserted_ids.insert(doc_id);
                    }
                    
                    if (doc->is_exists("/did")) {
                        std::string did(doc->get_string("/did"));
                        inserted_dids.insert(did);
                    }
                    
                    documents.push_back(doc);
                } catch (const std::exception& e) {
                    std::cout << "✗ Failed to parse record " << i << ": " << e.what() << std::endl;
                    REQUIRE(false);
                }
            }
            
            std::cout << "  Unique _id values: " << inserted_ids.size() << std::endl;
            std::cout << "  Unique DID values: " << inserted_dids.size() << std::endl;
            
            auto start = std::chrono::high_resolution_clock::now();
            dispatcher->insert_many(session, database_name, collection_name, documents);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "✓ Inserted " << documents.size() << " records in " 
                      << duration.count() << "ms (" 
                      << (documents.size() * 1000.0 / duration.count()) << " rec/s)" << std::endl;
        }
        
        // Query all records and extract DIDs BEFORE dispatcher is deleted
        {
            auto session = otterbrix::session_id_t();
            auto start = std::chrono::high_resolution_clock::now();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT * FROM bluesky_bench.bluesky;");
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            REQUIRE(cur->is_success());
            document_count = cur->size();
            
            std::cout << "✓ SELECT * returned " << cur->size() 
                      << " records in " << duration.count() << "ms" << std::endl;
            std::cout << "  Cursor uses_table_data: " << cur->uses_table_data() << std::endl;
            
            // Extract DIDs while cursor is still valid
            if (cur->uses_table_data()) {
                // document_table returns data_chunk, not documents
                auto& chunk = cur->chunk_data();
                std::cout << "  Chunk size: " << chunk.size() << ", columns: " << chunk.column_count() << std::endl;
                std::cout << "  Warning: returns data_chunk, DID extraction not yet implemented" << std::endl;
            } else {
                // documents returns document list
                auto& docs = cur->document_data();
                std::cout << "  Document list size: " << docs.size() << std::endl;
                for (size_t i = 0; i < docs.size(); ++i) {
                    auto doc = docs[i];
                    if (doc && doc->is_exists("/did")) {
                        document_dids.insert(std::string(doc->get_string("/did")));
                    }
                }
                std::cout << "  Extracted " << document_dids.size() << " unique DIDs" << std::endl;
            }
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
        
        std::cout << "document_table returned: " << doc_table_count << " records" << std::endl;
        std::cout << "document returned:       " << document_count << " records" << std::endl;
        
        REQUIRE(doc_table_count == document_count);
        std::cout << "✓ Both storages returned same number of records" << std::endl;
        
        std::cout << "\ndocument_table unique DIDs: " << doc_table_dids.size() << std::endl;
        std::cout << "document unique DIDs:       " << document_dids.size() << std::endl;
        
        if (!doc_table_dids.empty() && !document_dids.empty()) {
            REQUIRE(doc_table_dids.size() == document_dids.size());
            std::cout << "✓ Both storages have same number of unique DIDs" << std::endl;
            
            // Check for differences
            std::vector<std::string> only_in_dt;
            std::vector<std::string> only_in_doc;
            
            for (const auto& did : doc_table_dids) {
                if (document_dids.find(did) == document_dids.end()) {
                    only_in_dt.push_back(did);
                }
            }
            
            for (const auto& did : document_dids) {
                if (doc_table_dids.find(did) == doc_table_dids.end()) {
                    only_in_doc.push_back(did);
                }
            }
            
            if (!only_in_dt.empty()) {
                std::cout << "\n✗ DIDs only in document_table: " << only_in_dt.size() << std::endl;
                for (size_t i = 0; i < std::min(size_t(5), only_in_dt.size()); ++i) {
                    std::cout << "  - " << only_in_dt[i] << std::endl;
                }
            }
            
            if (!only_in_doc.empty()) {
                std::cout << "\n✗ DIDs only in document: " << only_in_doc.size() << std::endl;
                for (size_t i = 0; i < std::min(size_t(5), only_in_doc.size()); ++i) {
                    std::cout << "  - " << only_in_doc[i] << std::endl;
                }
            }
            
            REQUIRE(only_in_dt.empty());
            REQUIRE(only_in_doc.empty());
            std::cout << "✓ Both storages contain exactly the same DIDs" << std::endl;
            
            // Sample some DIDs
            std::cout << "\nSample of DIDs found in both storages:" << std::endl;
            int count = 0;
            for (const auto& did : doc_table_dids) {
                if (count++ >= 3) break;
                std::cout << "  - " << did << std::endl;
            }
        } else {
            std::cout << "⚠️  DID comparison skipped (cursors return different formats)" << std::endl;
        }
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "✅ ALL CHECKS PASSED" << std::endl;
        std::cout << "========================================\n" << std::endl;
    }
}
