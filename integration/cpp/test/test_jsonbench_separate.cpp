#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <set>
#include <iomanip>
#include <algorithm>
#include <memory>
#include <components/document_table/document_table_storage.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>

static const database_name_t database_name = "bluesky_bench";
static const collection_name_t collection_name = "bluesky";

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

// Benchmark result structure
struct BenchmarkResult {
    long long time_ms;
    size_t count;
    std::string storage_type;
};

// Query pair for different storage types
struct QueryPair {
    std::string document_table_query;  // Uses "field.subfield" syntax (column names with dots)
    std::string document_query;        // Uses /field/subfield syntax (JSON pointers)
    
    QueryPair(const std::string& dt_query, const std::string& doc_query)
        : document_table_query(dt_query), document_query(doc_query) {}
    
    // Constructor for queries that work the same for both storages
    explicit QueryPair(const std::string& query)
        : document_table_query(query), document_query(query) {}
};

// Setup and populate document_table storage
std::unique_ptr<test_spaces> setup_document_table(const std::string& tmp_dir, 
                                                    const std::vector<std::string>& json_lines) {
    auto config = test_create_config(tmp_dir);
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    auto space = std::make_unique<test_spaces>(config);
    auto* dispatcher = space->dispatcher();

    dispatcher->create_database(otterbrix::session_id_t(), database_name);
    auto cur = dispatcher->execute_sql(
        otterbrix::session_id_t(),
        "CREATE TABLE bluesky_bench.bluesky() WITH (storage='document_table');");
    REQUIRE(cur->is_success());

    // Insert data
    auto session = otterbrix::session_id_t();
    std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
    for (const auto& line : json_lines) {
        docs.push_back(components::document::document_t::document_from_json(line, dispatcher->resource()));
    }
    dispatcher->insert_many(session, database_name, collection_name, docs);

    return space;
}

// Setup and populate document (B-tree) storage
std::unique_ptr<test_spaces> setup_document(const std::string& tmp_dir, 
                                            const std::vector<std::string>& json_lines) {
    auto config = test_create_config(tmp_dir);
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    auto space = std::make_unique<test_spaces>(config);
    auto* dispatcher = space->dispatcher();

    dispatcher->create_database(otterbrix::session_id_t(), database_name);
    auto cur = dispatcher->execute_sql(
        otterbrix::session_id_t(),
        "CREATE TABLE bluesky_bench.bluesky() WITH (storage='documents');");
    REQUIRE(cur->is_success());

    // Insert data with explicit _id generation
    auto session = otterbrix::session_id_t();
    std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
    size_t doc_idx = 0;
    for (const auto& line : json_lines) {
        auto doc = components::document::document_t::document_from_json(line, dispatcher->resource());
        
        // Add unique _id if missing
        if (!doc->is_exists("/_id")) {
            std::ostringstream id_stream;
            id_stream << std::setfill('0') << std::setw(24) << doc_idx;
            doc->set("/_id", id_stream.str());
        }
        
        docs.push_back(doc);
        doc_idx++;
    }
    dispatcher->insert_many(session, database_name, collection_name, docs);

    return space;
}

// Run query and measure time
BenchmarkResult run_query(const std::unique_ptr<test_spaces>& space, const std::string& query, const std::string& storage_type) {
    auto* dispatcher = space->dispatcher();
    auto session = otterbrix::session_id_t();
    
    auto start = std::chrono::high_resolution_clock::now();
    auto cur = dispatcher->execute_sql(session, query);
    auto end = std::chrono::high_resolution_clock::now();
    
    REQUIRE(cur->is_success());
    
    return BenchmarkResult{
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(),
        cur->size(),
        storage_type
    };
}

// Print test header
void print_header(const std::string& title, const std::string& subtitle, size_t record_count) {
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║ " << std::left << std::setw(60) << title << " ║" << std::endl;
    std::cout << "║ " << std::left << std::setw(60) << (subtitle + " " + std::to_string(record_count) + " records") << " ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;
}

// Print comparison summary
void print_comparison(const std::string& test_name, 
                     const BenchmarkResult& dt_result, 
                     const BenchmarkResult& doc_result,
                     const std::string& unit = "records") {
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║ " << std::left << std::setw(60) << (test_name + " COMPARISON") << " ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║ document_table:  " << std::setw(6) << dt_result.time_ms << " ms  (" 
              << dt_result.count << " " << unit << ")" << std::string(60 - 26 - std::to_string(dt_result.count).length() - unit.length(), ' ') << "║" << std::endl;
    std::cout << "║ document:        " << std::setw(6) << doc_result.time_ms << " ms  (" 
              << doc_result.count << " " << unit << ")" << std::string(60 - 26 - std::to_string(doc_result.count).length() - unit.length(), ' ') << "║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    
    if (doc_result.time_ms < dt_result.time_ms) {
        double speedup = (double)dt_result.time_ms / doc_result.time_ms;
        std::cout << "║ Winner: document (B-tree) - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster" << std::string(60 - 32 - std::to_string((int)speedup).length(), ' ') << "║" << std::endl;
    } else {
        double speedup = (double)doc_result.time_ms / dt_result.time_ms;
        std::cout << "║ Winner: document_table - " << std::fixed << std::setprecision(1)
                  << speedup << "x faster" << std::string(60 - 27 - std::to_string((int)speedup).length(), ' ') << "║" << std::endl;
    }
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;
}

} // anonymous namespace

TEST_CASE("JSONBench 0a: BATCH INSERT Performance", "[jsonbench][batch_insert]") {
    std::string data_path = "/home/tolisso/otterbrix/integration/cpp/test/test_sample_10000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());

    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║       JSONBench: BATCH INSERT Performance Comparison         ║" << std::endl;
    std::cout << "║                  " << json_lines.size() << " records                                    ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;

    long long dt_batch_insert_time = 0;
    long long dt_regular_insert_time = 0;

    // Test 1: document_table with batch_insert (direct API)
    {
        std::cout << "[1/2] Testing document_table BATCH INSERT..." << std::endl;

        auto resource = std::pmr::synchronized_pool_resource();
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        components::table::storage::standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);
        components::table::storage::in_memory_block_manager_t block_manager(buffer_manager, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE);
        components::document_table::document_table_storage_t storage(&resource, block_manager);

        // Подготавливаем все документы с их ID
        std::pmr::vector<std::pair<components::document::document_id_t, components::document::document_ptr>>
            documents(&resource);

        for (size_t i = 0; i < json_lines.size(); ++i) {
            auto doc = components::document::document_t::document_from_json(
                json_lines[i],
                &resource
            );

            // Генерируем ID
            auto doc_id = components::document::document_id_t();
            documents.emplace_back(doc_id, doc);
        }

        auto start = std::chrono::high_resolution_clock::now();

        // Используем batch_insert
        storage.batch_insert(documents);

        auto end = std::chrono::high_resolution_clock::now();
        dt_batch_insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        std::cout << "  ✓ document_table BATCH: " << dt_batch_insert_time << "ms ("
                  << (json_lines.size() * 1000.0 / dt_batch_insert_time) << " rec/s)" << std::endl;

        // Verify count
        REQUIRE(storage.size() == json_lines.size());
    }

    // Summary
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║              BATCH INSERT Performance                        ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║ Time:       " << std::setw(6) << dt_batch_insert_time << " ms                                    ║" << std::endl;
    std::cout << "║ Throughput: " << std::setw(7) << std::fixed << std::setprecision(1)
              << (json_lines.size() * 1000.0 / dt_batch_insert_time) << " rec/s                             ║" << std::endl;
    std::cout << "║ Records:    " << std::setw(6) << json_lines.size() << "                                       ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << std::endl;
}

TEST_CASE("JSONBench 0: INSERT Performance", "[jsonbench][insert]") {
    std::string data_path = "/home/tolisso/otterbrix/integration/cpp/test/test_sample_1000.json";
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
            "CREATE TABLE bluesky_bench.bluesky() WITH (storage='document_table');");
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
            "CREATE TABLE bluesky_bench.bluesky() WITH (storage='documents');");
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
                
                // Add unique _id if missing (required for B-tree documents storage)
                if (!doc->is_exists("/_id")) {
                    std::ostringstream id_stream;
                    id_stream << std::setfill('0') << std::setw(24) << i;
                    doc->set("/_id", id_stream.str());
                }
                
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

TEST_CASE("JSONBench Q1: Top event types", "[jsonbench][q1]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    print_header("JSONBench Q1: Top event types (GROUP BY)", "", json_lines.size());

    QueryPair queries(
        // document_table: SQL-safe column name
        "SELECT commit_dot_collection AS event, COUNT(*) AS count FROM bluesky_bench.bluesky GROUP BY event ORDER BY count DESC;",
        // document: JSON pointer path
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count FROM bluesky_bench.bluesky GROUP BY event ORDER BY count DESC;"
    );

    // Test 1: document_table
    std::cout << "[1/2] Testing document_table Q1..." << std::endl;
    auto dt_space = setup_document_table("/tmp/bench_q1_dt", json_lines);
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  ✓ document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " groups)" << std::endl;

    // Test 2: document (B-tree)
    std::cout << "[2/2] Testing document Q1..." << std::endl;
    auto doc_space = setup_document("/tmp/bench_q1_doc", json_lines);
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  ✓ document:       " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;

    // Summary
    print_comparison("QUERY 1", dt_result, doc_result, "groups");
}

TEST_CASE("JSONBench Q2: Event types with unique users", "[jsonbench][q2]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    print_header("JSONBench Q2: Event types + unique users (COUNT DISTINCT)", "", json_lines.size());

    QueryPair queries(
        // document_table: SQL-safe column names
        "SELECT commit_dot_collection AS event, COUNT(*) AS count, COUNT(DISTINCT did) AS users "
        "FROM bluesky_bench.bluesky WHERE kind = 'commit' AND commit_dot_operation = 'create' "
        "GROUP BY event ORDER BY count DESC;",
        // document: JSON pointer paths
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count, COUNT(DISTINCT did) AS users "
        "FROM bluesky_bench.bluesky WHERE kind = 'commit' AND \"/commit/operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;"
    );

    // Test 1: document_table
    std::cout << "[1/2] Testing document_table Q2..." << std::endl;
    auto dt_space = setup_document_table("/tmp/bench_q2_dt", json_lines);
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  ✓ document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " groups)" << std::endl;

    // Test 2: document (B-tree)
    std::cout << "[2/2] Testing document Q2..." << std::endl;
    auto doc_space = setup_document("/tmp/bench_q2_doc", json_lines);
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  ✓ document:       " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;

    // Summary
    print_comparison("QUERY 2", dt_result, doc_result, "groups");
}

TEST_CASE("JSONBench Q3: When do people use BlueSky (simplified)", "[jsonbench][q3]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    print_header("JSONBench Q3: Event counts by collection (GROUP BY with filters)", "", json_lines.size());

    QueryPair queries(
        // document_table: SQL-safe column names
        "SELECT commit_dot_collection AS event, COUNT(*) AS count "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND commit_dot_operation = 'create' "
        "GROUP BY event ORDER BY count DESC;",
        // document: JSON pointer paths
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;"
    );

    // Test 1: document_table
    std::cout << "[1/2] Testing document_table Q3..." << std::endl;
    auto dt_space = setup_document_table("/tmp/bench_q3_dt", json_lines);
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  ✓ document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " groups)" << std::endl;

    // Test 2: document (B-tree)
    std::cout << "[2/2] Testing document Q3..." << std::endl;
    auto doc_space = setup_document("/tmp/bench_q3_doc", json_lines);
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  ✓ document:       " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;

    // Summary
    print_comparison("QUERY 3", dt_result, doc_result, "groups");
}

TEST_CASE("JSONBench Q4: First 3 users to post", "[jsonbench][q4]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    print_header("JSONBench Q4: First 3 users to post (MIN + GROUP BY)", "", json_lines.size());

    QueryPair queries(
        // document_table: SQL-safe column names
        "SELECT did AS user_id, MIN(time_us) AS first_post_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND commit_dot_operation = 'create' AND commit_dot_collection = 'app.bsky.feed.post' "
        "GROUP BY user_id ORDER BY first_post_time ASC LIMIT 3;",
        // document: JSON pointer paths
        "SELECT did AS user_id, MIN(time_us) AS first_post_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' AND \"/commit/collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id ORDER BY first_post_time ASC LIMIT 3;"
    );

    // Test 1: document_table
    std::cout << "[1/2] Testing document_table Q4..." << std::endl;
    auto dt_space = setup_document_table("/tmp/bench_q4_dt", json_lines);
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  ✓ document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " users)" << std::endl;

    // Test 2: document (B-tree)
    std::cout << "[2/2] Testing document Q4..." << std::endl;
    auto doc_space = setup_document("/tmp/bench_q4_doc", json_lines);
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  ✓ document:       " << doc_result.time_ms << "ms (" << doc_result.count << " users)" << std::endl;

    // Summary
    print_comparison("QUERY 4", dt_result, doc_result, "users");
}

TEST_CASE("JSONBench Q5: Top 3 users with longest activity", "[jsonbench][q5]") {
    std::string data_path = "test_sample_1000.json";
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());
    
    print_header("JSONBench Q5: Top 3 users by activity span (MAX-MIN)", "", json_lines.size());

    QueryPair queries(
        // document_table: SQL-safe column names (simplified - no arithmetic in SELECT)
        "SELECT did AS user_id, MAX(time_us) AS max_time, MIN(time_us) AS min_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND commit_dot_operation = 'create' AND commit_dot_collection = 'app.bsky.feed.post' "
        "GROUP BY user_id LIMIT 3;",
        // document: JSON pointer paths
        "SELECT did AS user_id, MAX(time_us) AS max_time, MIN(time_us) AS min_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' AND \"/commit/collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id LIMIT 3;"
    );

    // Test 1: document_table
    std::cout << "[1/2] Testing document_table Q5..." << std::endl;
    auto dt_space = setup_document_table("/tmp/bench_q5_dt", json_lines);
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  ✓ document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " users)" << std::endl;

    // Test 2: document (B-tree)
    std::cout << "[2/2] Testing document Q5..." << std::endl;
    auto doc_space = setup_document("/tmp/bench_q5_doc", json_lines);
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  ✓ document:       " << doc_result.time_ms << "ms (" << doc_result.count << " users)" << std::endl;

    // Summary
    print_comparison("QUERY 5", dt_result, doc_result, "users");
}
