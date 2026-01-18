#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>

static const database_name_t database_name = "bluesky_bench";
static const collection_name_t collection_name = "bluesky";

using components::cursor::cursor_t_ptr;
namespace types = components::types;

namespace {

std::string data_path = "/home/tolisso/otterbrix/integration/cpp/test/test_sample_100_000.json";

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

struct BenchmarkResult {
    long long time_ms;
    size_t count;
    std::string storage_type;
};

struct QueryPair {
    std::string document_table_query;
    std::string document_query;

    QueryPair(const std::string& dt_query, const std::string& doc_query)
        : document_table_query(dt_query), document_query(doc_query) {}

    explicit QueryPair(const std::string& query)
        : document_table_query(query), document_query(query) {}
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

    std::string create_sql = "CREATE TABLE bluesky_bench.bluesky() WITH (storage='" + storage_type + "');";
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
                     const BenchmarkResult& dt_result,
                     const BenchmarkResult& doc_result,
                     const std::string& unit = "records") {
    std::cout << "\n--- " << test_name << " ---" << std::endl;
    std::cout << "document_table: " << dt_result.time_ms << " ms (" << dt_result.count << " " << unit << ")" << std::endl;
    std::cout << "document:       " << doc_result.time_ms << " ms (" << doc_result.count << " " << unit << ")" << std::endl;

    if (doc_result.time_ms > 0 && dt_result.time_ms > 0) {
        if (doc_result.time_ms < dt_result.time_ms) {
            double speedup = (double)dt_result.time_ms / doc_result.time_ms;
            std::cout << "Winner: document (B-tree) - " << std::fixed << std::setprecision(1) << speedup << "x faster" << std::endl;
        } else {
            double speedup = (double)doc_result.time_ms / dt_result.time_ms;
            std::cout << "Winner: document_table - " << std::fixed << std::setprecision(1) << speedup << "x faster" << std::endl;
        }
    }
}

} // anonymous namespace

TEST_CASE("JSONBench Q1: Top event types", "[jsonbench][q1]") {
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q1: Top event types (GROUP BY)", json_lines.size());

    QueryPair queries(
        "SELECT commit_dot_collection AS event, COUNT(*) AS count FROM bluesky_bench.bluesky GROUP BY event ORDER BY count DESC;",
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count FROM bluesky_bench.bluesky GROUP BY event ORDER BY count DESC;"
    );

    std::cout << "[1/2] Testing document_table..." << std::endl;
    auto dt_space = setup_storage("/tmp/bench_q1_dt", json_lines, "document_table");
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " groups)" << std::endl;

    std::cout << "[2/2] Testing document..." << std::endl;
    auto doc_space = setup_storage("/tmp/bench_q1_doc", json_lines, "documents");
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;

    print_comparison("QUERY 1", dt_result, doc_result, "groups");
}

TEST_CASE("JSONBench Q2: Event types with unique users", "[jsonbench][q2]") {
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q2: Event types + unique users (COUNT DISTINCT)", json_lines.size());

    QueryPair queries(
        "SELECT commit_dot_collection AS event, COUNT(*) AS count, COUNT(DISTINCT did) AS users "
        "FROM bluesky_bench.bluesky WHERE kind = 'commit' AND commit_dot_operation = 'create' "
        "GROUP BY event ORDER BY count DESC;",
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count, COUNT(DISTINCT did) AS users "
        "FROM bluesky_bench.bluesky WHERE kind = 'commit' AND \"/commit/operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;"
    );

    std::cout << "[1/2] Testing document_table..." << std::endl;
    auto dt_space = setup_storage("/tmp/bench_q2_dt", json_lines, "document_table");
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " groups)" << std::endl;

    std::cout << "[2/2] Testing document..." << std::endl;
    auto doc_space = setup_storage("/tmp/bench_q2_doc", json_lines, "documents");
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;

    print_comparison("QUERY 2", dt_result, doc_result, "groups");
}

TEST_CASE("JSONBench Q3: Event counts with filters", "[jsonbench][q3]") {
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q3: Event counts (GROUP BY with filters)", json_lines.size());

    QueryPair queries(
        "SELECT commit_dot_collection AS event, COUNT(*) AS count "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND commit_dot_operation = 'create' "
        "GROUP BY event ORDER BY count DESC;",
        "SELECT \"/commit/collection\" AS event, COUNT(*) AS count "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' "
        "GROUP BY event ORDER BY count DESC;"
    );

    std::cout << "[1/2] Testing document_table..." << std::endl;
    auto dt_space = setup_storage("/tmp/bench_q3_dt", json_lines, "document_table");
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " groups)" << std::endl;

    std::cout << "[2/2] Testing document..." << std::endl;
    auto doc_space = setup_storage("/tmp/bench_q3_doc", json_lines, "documents");
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " groups)" << std::endl;

    print_comparison("QUERY 3", dt_result, doc_result, "groups");
}

TEST_CASE("JSONBench Q4: First 3 users to post", "[jsonbench][q4]") {
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q4: First 3 users to post (MIN + GROUP BY)", json_lines.size());

    QueryPair queries(
        "SELECT did AS user_id, MIN(time_us) AS first_post_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND commit_dot_operation = 'create' AND commit_dot_collection = 'app.bsky.feed.post' "
        "GROUP BY user_id ORDER BY first_post_time ASC LIMIT 3;",
        "SELECT did AS user_id, MIN(time_us) AS first_post_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' AND \"/commit/collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id ORDER BY first_post_time ASC LIMIT 3;"
    );

    std::cout << "[1/2] Testing document_table..." << std::endl;
    auto dt_space = setup_storage("/tmp/bench_q4_dt", json_lines, "document_table");
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " users)" << std::endl;

    std::cout << "[2/2] Testing document..." << std::endl;
    auto doc_space = setup_storage("/tmp/bench_q4_doc", json_lines, "documents");
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " users)" << std::endl;

    print_comparison("QUERY 4", dt_result, doc_result, "users");
}

TEST_CASE("JSONBench Q5: Top 3 users by activity span", "[jsonbench][q5]") {
    auto json_lines = read_ndjson_file(data_path);
    REQUIRE(!json_lines.empty());

    print_header("JSONBench Q5: Top 3 users by activity span (MAX-MIN)", json_lines.size());

    QueryPair queries(
        "SELECT did AS user_id, MAX(time_us) AS max_time, MIN(time_us) AS min_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND commit_dot_operation = 'create' AND commit_dot_collection = 'app.bsky.feed.post' "
        "GROUP BY user_id LIMIT 3;",
        "SELECT did AS user_id, MAX(time_us) AS max_time, MIN(time_us) AS min_time "
        "FROM bluesky_bench.bluesky "
        "WHERE kind = 'commit' AND \"/commit/operation\" = 'create' AND \"/commit/collection\" = 'app.bsky.feed.post' "
        "GROUP BY user_id LIMIT 3;"
    );

    std::cout << "[1/2] Testing document_table..." << std::endl;
    auto dt_space = setup_storage("/tmp/bench_q5_dt", json_lines, "document_table");
    auto dt_result = run_query(dt_space, queries.document_table_query, "document_table");
    std::cout << "  document_table: " << dt_result.time_ms << "ms (" << dt_result.count << " users)" << std::endl;

    std::cout << "[2/2] Testing document..." << std::endl;
    auto doc_space = setup_storage("/tmp/bench_q5_doc", json_lines, "documents");
    auto doc_result = run_query(doc_space, queries.document_query, "document");
    std::cout << "  document: " << doc_result.time_ms << "ms (" << doc_result.count << " users)" << std::endl;

    print_comparison("QUERY 5", dt_result, doc_result, "users");
}
