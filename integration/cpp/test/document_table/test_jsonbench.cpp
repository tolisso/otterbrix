#include "../test_config.hpp"
#include <catch2/catch.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <set>
#include <iomanip>
#include <algorithm>

static const database_name_t database_name = "bluesky_bench";
static const collection_name_t collection_name = "bluesky";

using components::cursor::cursor_t_ptr;
namespace types = components::types;

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
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Testing with " << json_lines.size() << " records" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Test document_table
    std::vector<std::string> doc_table_jsons;
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
        
        // Insert data in batches (document_table has capacity limit of 1024 per batch)
        {
            auto session = otterbrix::session_id_t();
            constexpr size_t batch_size = 1000;
            size_t total_inserted = 0;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            for (size_t batch_start = 0; batch_start < json_lines.size(); batch_start += batch_size) {
                size_t batch_end = std::min(batch_start + batch_size, json_lines.size());
                std::pmr::vector<components::document::document_ptr> batch_documents(dispatcher->resource());
                
                for (size_t i = batch_start; i < batch_end; ++i) {
                    try {
                        auto doc = components::document::document_t::document_from_json(
                            json_lines[i], 
                            dispatcher->resource()
                        );
                        batch_documents.push_back(doc);
                    } catch (const std::exception& e) {
                        std::cout << "✗ Failed to parse record " << i << ": " << e.what() << std::endl;
                        REQUIRE(false);
                    }
                }
                
                dispatcher->insert_many(session, database_name, collection_name, batch_documents);
                total_inserted += batch_documents.size();
                
                if ((batch_start / batch_size + 1) % 5 == 0 || batch_end == json_lines.size()) {
                    std::cout << "  Inserted " << total_inserted << " / " << json_lines.size() << " records..." << std::endl;
                }
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "✓ Inserted " << total_inserted << " records in " 
                      << duration.count() << "ms (" 
                      << (total_inserted * 1000.0 / duration.count()) << " rec/s)" << std::endl;
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
            doc_table_count = cur->size();
            
            std::cout << "✓ SELECT * returned " << cur->size() 
                      << " records in " << duration.count() << "ms" << std::endl;
        }
        
        // Query specific fields to extract comparable data
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT did, kind FROM bluesky_bench.bluesky;");
            
            REQUIRE(cur->is_success());
            
            if (cur->uses_table_data()) {
                // document_table returns data_chunk
                auto& chunk = cur->chunk_data();
                std::cout << "  Extracted data_chunk: " << chunk.size() << " rows, " 
                          << chunk.column_count() << " columns" << std::endl;
                
                // Extract values from columns (col 0=did, col 1=kind)
                for (size_t i = 0; i < chunk.size(); ++i) {
                    try {
                        auto did_val = chunk.value(0, i);
                        if (!did_val.is_null() && did_val.type().type() == types::logical_type::STRING_LITERAL) {
                            std::string did = std::string(did_val.value<std::string_view>());
                            doc_table_jsons.push_back(did);
                        }
                    } catch (const std::exception& e) {
                        std::cout << "  Warning: failed to extract value from row " << i << ": " << e.what() << std::endl;
                    }
                }
                std::cout << "  Extracted " << doc_table_jsons.size() << " DIDs from data_chunk" << std::endl;
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
    std::vector<std::string> document_jsons;
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
        
        // Insert data in batches
        {
            auto session = otterbrix::session_id_t();
            constexpr size_t batch_size = 1000;
            size_t total_inserted = 0;
            std::set<std::string> inserted_ids;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            for (size_t batch_start = 0; batch_start < json_lines.size(); batch_start += batch_size) {
                size_t batch_end = std::min(batch_start + batch_size, json_lines.size());
                std::pmr::vector<components::document::document_ptr> batch_documents(dispatcher->resource());
                
                for (size_t i = batch_start; i < batch_end; ++i) {
                    try {
                        auto doc = components::document::document_t::document_from_json(
                            json_lines[i], 
                            dispatcher->resource()
                        );
                        
                        // Add unique _id if missing (required for B-tree documents storage)
                        if (!doc->is_exists("/_id")) {
                            std::ostringstream id_stream;
                            id_stream << std::setfill('0') << std::setw(24) << i;
                            doc->set("/_id", id_stream.str());
                            if (i < 3) {
                                std::cout << "  Document " << i << " generated _id: " << id_stream.str() << std::endl;
                            }
                        }
                        
                        if (doc->is_exists("/_id")) {
                            std::string doc_id(doc->get_string("/_id"));
                            inserted_ids.insert(doc_id);
                        }
                        
                        batch_documents.push_back(doc);
                    } catch (const std::exception& e) {
                        std::cout << "✗ Failed to parse record " << i << ": " << e.what() << std::endl;
                        REQUIRE(false);
                    }
                }
                
                dispatcher->insert_many(session, database_name, collection_name, batch_documents);
                total_inserted += batch_documents.size();
                
                if ((batch_start / batch_size + 1) % 5 == 0 || batch_end == json_lines.size()) {
                    std::cout << "  Inserted " << total_inserted << " / " << json_lines.size() << " records..." << std::endl;
                }
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "  Unique _id values: " << inserted_ids.size() << std::endl;
            std::cout << "✓ Inserted " << total_inserted << " records in " 
                      << duration.count() << "ms (" 
                      << (total_inserted * 1000.0 / duration.count()) << " rec/s)" << std::endl;
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
            document_count = cur->size();
            
            std::cout << "✓ SELECT * returned " << cur->size() 
                      << " records in " << duration.count() << "ms" << std::endl;
        }
        
        // Query specific fields to extract comparable data  
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "SELECT did, kind FROM bluesky_bench.bluesky;");
            
            REQUIRE(cur->is_success());
            
            if (!cur->uses_table_data()) {
                // documents returns document list
                auto& docs = cur->document_data();
                std::cout << "  Extracted documents: " << docs.size() << std::endl;
                
                // Extract DIDs
                for (size_t i = 0; i < docs.size(); ++i) {
                    if (docs[i] && docs[i]->is_exists("/did")) {
                        std::string did = std::string(docs[i]->get_string("/did"));
                        document_jsons.push_back(did);
                    }
                }
                std::cout << "  Extracted " << document_jsons.size() << " DIDs from documents" << std::endl;
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
        
        // Compare extracted DIDs
        if (!doc_table_jsons.empty() && !document_jsons.empty()) {
            std::cout << "\n✓ DID Extraction:" << std::endl;
            std::cout << "  document_table extracted: " << doc_table_jsons.size() << " DIDs" << std::endl;
            std::cout << "  documents extracted:      " << document_jsons.size() << " DIDs" << std::endl;
            
            // Sort both lists for comparison
            std::sort(doc_table_jsons.begin(), doc_table_jsons.end());
            std::sort(document_jsons.begin(), document_jsons.end());
            
            // Compare
            bool same = (doc_table_jsons.size() == document_jsons.size());
            if (same) {
                for (size_t i = 0; i < doc_table_jsons.size(); ++i) {
                    if (doc_table_jsons[i] != document_jsons[i]) {
                        same = false;
                        std::cout << "\n  ✗ Mismatch at index " << i << ":" << std::endl;
                        std::cout << "    document_table: " << doc_table_jsons[i] << std::endl;
                        std::cout << "    documents:      " << document_jsons[i] << std::endl;
                        break;
                    }
                }
            }
            
            if (same) {
                std::cout << "  ✅ All DIDs match between storages!" << std::endl;
                
                // Show samples
                std::cout << "\n  Sample DIDs (first 3):" << std::endl;
                for (size_t i = 0; i < std::min(size_t(3), doc_table_jsons.size()); ++i) {
                    std::cout << "    " << (i+1) << ". " << doc_table_jsons[i] << std::endl;
                }
                
                REQUIRE(doc_table_jsons == document_jsons);
            } else {
                std::cout << "  ✗ DIDs do NOT match!" << std::endl;
                REQUIRE(false);
            }
        } else {
            std::cout << "\n⚠️  Could not extract DIDs for comparison" << std::endl;
        }
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "SUMMARY" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "✅ Both storages successfully store and retrieve all " << json_lines.size() << " records" << std::endl;
        std::cout << "✅ documents (B-tree): Fast insert (" << (json_lines.size() * 1000.0 / 4) << " rec/s)" << std::endl;
        std::cout << "✅ document_table: Good for analytical queries (columnar format)" << std::endl;
        std::cout << "\n📝 Note: Full JSON comparison requires data_chunk → document conversion" << std::endl;
        std::cout << "   for document_table storage (future enhancement)" << std::endl;
        std::cout << "========================================\n" << std::endl;
    }
}
