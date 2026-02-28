#include <integration/cpp/test/test_config.hpp>
#include <catch2/catch.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>

static const database_name_t database_name = "bluesky_bench";
static const collection_name_t collection_name = "bluesky";

// ============================================================
// Sanity check: SQL on explicit-schema columnar table
// ============================================================
TEST_CASE("columnar table: SQL on explicit schema", "[columnar_sql]") {
    auto config = test_create_config("/tmp/test_columnar_sql");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    dispatcher->create_database(otterbrix::session_id_t(), database_name_t("testdb"));

    // Create table with explicit schema (did string, kind string)
    {
        auto cur = dispatcher->execute_sql(otterbrix::session_id_t(),
            "CREATE TABLE testdb.testcoll(did string, kind string);");
        REQUIRE(cur->is_success());
    }

    // Insert rows via SQL VALUES
    {
        auto cur = dispatcher->execute_sql(otterbrix::session_id_t(),
            "INSERT INTO testdb.testcoll(did, kind) VALUES "
            "('did1', 'commit'), "
            "('did2', 'commit'), "
            "('did1', 'identity'), "
            "('did3', 'commit');");
        REQUIRE(cur->is_success());
    }

    // SELECT COUNT(*)
    {
        auto cur = dispatcher->execute_sql(otterbrix::session_id_t(),
            "SELECT COUNT(*) AS cnt FROM testdb.testcoll;");
        REQUIRE(cur->is_success());
        auto doc = cur->next_document();
        REQUIRE(doc != nullptr);
        REQUIRE(doc->get_long("cnt") == 4);
    }

    // GROUP BY kind, COUNT(*)
    {
        auto cur = dispatcher->execute_sql(otterbrix::session_id_t(),
            "SELECT kind, COUNT(*) AS cnt FROM testdb.testcoll GROUP BY kind ORDER BY cnt DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto doc1 = cur->next_document();
        REQUIRE(doc1 != nullptr);
        REQUIRE(doc1->get_string("kind") == std::pmr::string("commit"));
        REQUIRE(doc1->get_long("cnt") == 3);
        auto doc2 = cur->next_document();
        REQUIRE(doc2 != nullptr);
        REQUIRE(doc2->get_string("kind") == std::pmr::string("identity"));
        REQUIRE(doc2->get_long("cnt") == 1);
    }

    // WHERE + GROUP BY
    {
        auto cur = dispatcher->execute_sql(otterbrix::session_id_t(),
            "SELECT did, COUNT(*) AS cnt FROM testdb.testcoll "
            "WHERE kind = 'commit' GROUP BY did ORDER BY cnt DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

// Number of documents to use from the dataset (max 100000)
static constexpr size_t DOC_LIMIT = 10000;

// Set to true to also benchmark document (B-tree) storage
static constexpr bool TEST_DOCUMENT_STORAGE = true;

using components::cursor::cursor_t_ptr;
namespace types = components::types;

#ifndef TEST_SAMPLE_FILE
#define TEST_SAMPLE_FILE "test_sample_100_000_filtered.json"
#endif

namespace {

// Get current process memory usage in MB (Linux only)
size_t get_memory_usage_mb() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            size_t kb = 0;
            std::istringstream iss(line.substr(6));
            iss >> kb;
            return kb / 1024;
        }
    }
    return 0;
}

std::vector<std::string> read_ndjson_file(const std::string& filepath, size_t limit = DOC_LIMIT) {
    std::vector<std::string> lines;
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return lines;
    }

    std::string line;
    while (std::getline(file, line) && lines.size() < limit) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }

    return lines;
}

struct BenchmarkResult {
    long long time_ms;
    size_t count;
    std::string storage_type;
};

struct QueryPair {
    std::string table_query;
    std::string document_query;

    QueryPair(const std::string& tbl_query, const std::string& doc_query)
        : table_query(tbl_query), document_query(doc_query) {}

    explicit QueryPair(const std::string& query)
        : table_query(query), document_query(query) {}
};

std::unique_ptr<test_spaces> setup_storage(const std::string& tmp_dir,
                                            const std::vector<std::string>& json_lines,
                                            const std::string& storage_type) {
    auto config = test_create_config(tmp_dir);
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    auto space = std::make_unique<test_spaces>(config);
    auto* dispatcher = space->dispatcher();

    dispatcher->create_database(otterbrix::session_id_t(), database_name);

    std::string create_sql;
    if (storage_type.empty()) {
        create_sql = "CREATE TABLE bluesky_bench.bluesky();";
    } else {
        create_sql = "CREATE TABLE bluesky_bench.bluesky() WITH (storage='" + storage_type + "');";
    }
    auto cur = dispatcher->execute_sql(otterbrix::session_id_t(), create_sql);
    REQUIRE(cur->is_success());

    auto session = otterbrix::session_id_t();
    std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
    size_t doc_idx = 0;
    for (const auto& line : json_lines) {
        auto doc = components::document::document_t::document_from_json(line, dispatcher->resource());

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

BenchmarkResult run_query(const std::unique_ptr<test_spaces>& space,
                          const std::string& query,
                          const std::string& storage_type) {
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

void print_header(const std::string& title, size_t record_count) {
    std::cout << "\n======================================================" << std::endl;
    std::cout << title << " (" << record_count << " records)" << std::endl;
    std::cout << "======================================================\n" << std::endl;
}

void print_comparison(const std::string& test_name,
                     const BenchmarkResult& tbl_result,
                     const BenchmarkResult& doc_result,
                     const std::string& unit = "records") {
    std::cout << "\n--- " << test_name << " ---" << std::endl;
    std::cout << "table:    " << tbl_result.time_ms << " ms (" << tbl_result.count << " " << unit << ")" << std::endl;
    std::cout << "document: " << doc_result.time_ms << " ms (" << doc_result.count << " " << unit << ")" << std::endl;

    if (doc_result.time_ms > 0 && tbl_result.time_ms > 0) {
        if (doc_result.time_ms < tbl_result.time_ms) {
            double speedup = (double)tbl_result.time_ms / doc_result.time_ms;
            std::cout << "Winner: document (B-tree) - " << std::fixed << std::setprecision(1) << speedup << "x faster" << std::endl;
        } else {
            double speedup = (double)doc_result.time_ms / tbl_result.time_ms;
            std::cout << "Winner: table - " << std::fixed << std::setprecision(1) << speedup << "x faster" << std::endl;
        }
    }
}

} // anonymous namespace

TEST_CASE("JSONBench Q1: Top event types", "[jsonbench][q1]") {
    auto json_lines = read_ndjson_file(TEST_SAMPLE_FILE);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q1: Top event types (GROUP BY)", json_lines.size());

    QueryPair queries(
        "SELECT \"commit.collection\" AS event, COUNT(*) AS count FROM bluesky_bench.bluesky GROUP BY event ORDER BY count DESC;",
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count FROM bluesky_bench.bluesky GROUP BY event ORDER BY count DESC;"
    );

    std::cout << "[1] Testing table..." << std::endl;
    auto tbl_space = setup_storage("/tmp/bench_q1_tbl", json_lines, "");
    auto tbl_result = run_query(tbl_space, queries.table_query, "table");
    std::cout << "  table: " << tbl_result.time_ms << "ms (" << tbl_result.count << " groups)" << std::endl;

    if (TEST_DOCUMENT_STORAGE) {
        std::cout << "[2] Testing document..." << std::endl;
        auto doc_space = setup_storage("/tmp/bench_q1_doc", json_lines, "documents");
        auto doc_result = run_query(doc_space, queries.document_query, "document");
        std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;
        print_comparison("QUERY 1", tbl_result, doc_result, "groups");
    }
}

TEST_CASE("JSONBench Q2: Event types with unique users", "[jsonbench][q2]") {
    auto json_lines = read_ndjson_file(TEST_SAMPLE_FILE);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q2: Event types + unique users (COUNT DISTINCT)", json_lines.size());

    QueryPair queries(
        "SELECT \"commit.collection\" AS event, COUNT(*) AS count, COUNT(DISTINCT did) AS users "
        "FROM bluesky_bench.bluesky WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;",
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count, COUNT(DISTINCT did) AS users "
        "FROM bluesky_bench.bluesky WHERE kind = 'commit' AND \"/commit/operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;"
    );

    std::cout << "[1] Testing table..." << std::endl;
    auto tbl_space = setup_storage("/tmp/bench_q2_tbl", json_lines, "");
    auto tbl_result = run_query(tbl_space, queries.table_query, "table");
    std::cout << "  table: " << tbl_result.time_ms << "ms (" << tbl_result.count << " groups)" << std::endl;

    if (TEST_DOCUMENT_STORAGE) {
        std::cout << "[2] Testing document..." << std::endl;
        auto doc_space = setup_storage("/tmp/bench_q2_doc", json_lines, "documents");
        auto doc_result = run_query(doc_space, queries.document_query, "document");
        std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;
        print_comparison("QUERY 2", tbl_result, doc_result, "groups");
    }
}

TEST_CASE("JSONBench Q3: Event counts with filters", "[jsonbench][q3]") {
    auto json_lines = read_ndjson_file(TEST_SAMPLE_FILE);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q3: Event counts (GROUP BY with filters)", json_lines.size());

    QueryPair queries(
        "SELECT \"commit.collection\" AS event, COUNT(*) AS count "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;",
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;"
    );

    std::cout << "[1] Testing table..." << std::endl;
    auto tbl_space = setup_storage("/tmp/bench_q3_tbl", json_lines, "");
    auto tbl_result = run_query(tbl_space, queries.table_query, "table");
    std::cout << "  table: " << tbl_result.time_ms << "ms (" << tbl_result.count << " groups)" << std::endl;

    if (TEST_DOCUMENT_STORAGE) {
        std::cout << "[2] Testing document..." << std::endl;
        auto doc_space = setup_storage("/tmp/bench_q3_doc", json_lines, "documents");
        auto doc_result = run_query(doc_space, queries.document_query, "document");
        std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;
        print_comparison("QUERY 3", tbl_result, doc_result, "groups");
    }
}

TEST_CASE("JSONBench Q4: First 3 users to post", "[jsonbench][q4]") {
    auto json_lines = read_ndjson_file(TEST_SAMPLE_FILE);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q4: First 3 users to post (MIN + GROUP BY)", json_lines.size());

    QueryPair queries(
        "SELECT did AS user_id, MIN(time_us) AS first_post_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"commit.operation\" = 'create' AND \"commit.collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id ORDER BY first_post_time ASC LIMIT 3;",
        "SELECT did AS user_id, MIN(time_us) AS first_post_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' AND \"/commit/collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id ORDER BY first_post_time ASC LIMIT 3;"
    );

    std::cout << "[1] Testing table..." << std::endl;
    auto tbl_space = setup_storage("/tmp/bench_q4_tbl", json_lines, "");
    auto tbl_result = run_query(tbl_space, queries.table_query, "table");
    std::cout << "  table: " << tbl_result.time_ms << "ms (" << tbl_result.count << " users)" << std::endl;

    if (TEST_DOCUMENT_STORAGE) {
        std::cout << "[2] Testing document..." << std::endl;
        auto doc_space = setup_storage("/tmp/bench_q4_doc", json_lines, "documents");
        auto doc_result = run_query(doc_space, queries.document_query, "document");
        std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " users)" << std::endl;
        print_comparison("QUERY 4", tbl_result, doc_result, "users");
    }
}

TEST_CASE("JSONBench Q5: Top 3 users by activity span", "[jsonbench][q5]") {
    auto json_lines = read_ndjson_file(TEST_SAMPLE_FILE);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q5: Top 3 users by activity span (MAX-MIN)", json_lines.size());

    QueryPair queries(
        "SELECT did AS user_id, MAX(time_us) AS max_time, MIN(time_us) AS min_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"commit.operation\" = 'create' AND \"commit.collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id LIMIT 3;",
        "SELECT did AS user_id, MAX(time_us) AS max_time, MIN(time_us) AS min_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' AND \"/commit/collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id LIMIT 3;"
    );

    std::cout << "[1] Testing table..." << std::endl;
    auto tbl_space = setup_storage("/tmp/bench_q5_tbl", json_lines, "");
    auto tbl_result = run_query(tbl_space, queries.table_query, "table");
    std::cout << "  table: " << tbl_result.time_ms << "ms (" << tbl_result.count << " users)" << std::endl;

    if (TEST_DOCUMENT_STORAGE) {
        std::cout << "[2] Testing document..." << std::endl;
        auto doc_space = setup_storage("/tmp/bench_q5_doc", json_lines, "documents");
        auto doc_result = run_query(doc_space, queries.document_query, "document");
        std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " users)" << std::endl;
        print_comparison("QUERY 5", tbl_result, doc_result, "users");
    }
}

TEST_CASE("JSONBench Memory: Insert only", "[jsonbench][memory]") {
    std::cout << "\n======================================================" << std::endl;
    std::cout << "JSONBench MEMORY TEST: Insert " << DOC_LIMIT << " documents" << std::endl;
    std::cout << "======================================================\n" << std::endl;

    auto json_lines = read_ndjson_file(TEST_SAMPLE_FILE);
    REQUIRE(!json_lines.empty());
    std::cout << "Loaded " << json_lines.size() << " JSON lines from file" << std::endl;

    size_t mem_before = get_memory_usage_mb();
    std::cout << "\nMemory before setup: " << mem_before << " MB" << std::endl;

    auto config = test_create_config("/tmp/bench_memory");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    size_t mem_after_init = get_memory_usage_mb();
    std::cout << "Memory after spaces init: " << mem_after_init << " MB (+" << (mem_after_init - mem_before) << " MB)" << std::endl;

    dispatcher->create_database(otterbrix::session_id_t(), database_name);
    auto cur = dispatcher->execute_sql(
        otterbrix::session_id_t(),
        "CREATE TABLE bluesky_bench.bluesky();");
    REQUIRE(cur->is_success());

    size_t mem_after_create = get_memory_usage_mb();
    std::cout << "Memory after create table: " << mem_after_create << " MB (+" << (mem_after_create - mem_after_init) << " MB)" << std::endl;

    // Prepare documents
    auto session = otterbrix::session_id_t();
    std::pmr::vector<components::document::document_ptr> docs(dispatcher->resource());
    size_t doc_idx = 0;
    for (const auto& line : json_lines) {
        auto doc = components::document::document_t::document_from_json(line, dispatcher->resource());
        if (!doc->is_exists("/_id")) {
            std::ostringstream id_stream;
            id_stream << std::setfill('0') << std::setw(24) << doc_idx;
            doc->set("/_id", id_stream.str());
        }
        docs.push_back(doc);
        doc_idx++;
    }

    size_t mem_after_parse = get_memory_usage_mb();
    std::cout << "Memory after parsing docs: " << mem_after_parse << " MB (+" << (mem_after_parse - mem_after_create) << " MB)" << std::endl;

    // Insert
    auto start = std::chrono::high_resolution_clock::now();
    dispatcher->insert_many(session, database_name, collection_name, docs);
    auto end = std::chrono::high_resolution_clock::now();
    auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    size_t mem_after_insert = get_memory_usage_mb();
    std::cout << "Memory after insert: " << mem_after_insert << " MB (+" << (mem_after_insert - mem_after_parse) << " MB)" << std::endl;

    std::cout << "\n--- SUMMARY ---" << std::endl;
    std::cout << "Documents:     " << json_lines.size() << std::endl;
    std::cout << "Insert time:   " << insert_ms << " ms" << std::endl;
    std::cout << "Total memory:  " << mem_after_insert << " MB" << std::endl;
    std::cout << "Memory delta:  " << (mem_after_insert - mem_before) << " MB" << std::endl;
    std::cout << "MB per 1000 docs: " << std::fixed << std::setprecision(1)
              << ((mem_after_insert - mem_before) * 1000.0 / json_lines.size()) << " MB" << std::endl;
}
