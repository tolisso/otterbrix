#include <catch2/catch.hpp>
#include <components/document_table/document_table_storage.hpp>
#include <components/physical_plan/document_table/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan/document_table/operators/operator_insert.hpp>
#include <components/physical_plan/base/operators/operator_raw_data.hpp>
#include <components/tests/generaty.hpp>
#include <services/collection/collection.hpp>
#include <core/pmr.hpp>

using namespace components;

namespace {

// Helper to create test context
std::unique_ptr<services::collection::context_collection_t>
create_test_context(std::pmr::memory_resource* resource, const std::string& name) {
    collection_full_name_t collection_name{"test_db", name};
    actor_zeta::address_t dummy_address = actor_zeta::address_t::empty_address();
    log_t log;

    return std::make_unique<services::collection::context_collection_t>(
        resource,
        collection_name,
        true, // dynamic_schema
        dummy_address,
        log
    );
}

// Helper to create simple test document
document::document_ptr create_test_doc(std::pmr::memory_resource* resource,
                                       const std::string& id_hex,
                                       const std::string& name,
                                       int age) {
    auto doc = document::make_document(resource);
    document::document_id_t doc_id(id_hex);
    doc->set("/_id", doc_id.to_string());
    doc->set("/name", name);
    doc->set("/age", static_cast<int64_t>(age));
    return doc;
}

} // anonymous namespace

TEST_CASE("primary_key_scan: single document by _id", "[document_table][primary_key_scan]") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "pk_scan_single");

    // Insert test document
    std::pmr::vector<document::document_ptr> docs(&resource);
    auto doc = create_test_doc(&resource, "507f1f77bcf86cd799439011", "Alice", 30);
    document::document_id_t search_id("507f1f77bcf86cd799439011");
    docs.push_back(doc);

    operator_insert insert_op(context.get());
    insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
    insert_op.on_execute(nullptr);

    REQUIRE(insert_op.modified() != nullptr);
    REQUIRE(insert_op.modified()->size() == 1);

    // Test primary_key_scan
    SECTION("find by _id using append") {
        primary_key_scan scan_op(context.get(), nullptr);
        scan_op.append(search_id);
        scan_op.on_execute(nullptr);

        REQUIRE(scan_op.output() != nullptr);
        auto& chunk = scan_op.output()->data_chunk();
        REQUIRE(chunk.size() == 1);

        // Verify the document data (column 1 is name)
        auto name_val = chunk.value(1, 0);
        REQUIRE(std::string(name_val.value<std::string_view>()) == "Alice");

        // Verify age (column 2)
        auto age_val = chunk.value(2, 0);
        REQUIRE(age_val.value<int64_t>() == 30);
    }
}

TEST_CASE("primary_key_scan: multiple documents", "[document_table][primary_key_scan]") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "pk_scan_multiple");

    // Insert multiple test documents
    std::pmr::vector<document::document_ptr> docs(&resource);
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439011", "Alice", 30));
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439012", "Bob", 25));
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439013", "Charlie", 35));
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439014", "David", 28));
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439015", "Eve", 32));

    operator_insert insert_op(context.get());
    insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
    insert_op.on_execute(nullptr);

    REQUIRE(insert_op.modified()->size() == 5);

    SECTION("find multiple documents by _id") {
        primary_key_scan scan_op(context.get(), nullptr);
        
        // Search for Bob and David
        scan_op.append(document::document_id_t("507f1f77bcf86cd799439012"));
        scan_op.append(document::document_id_t("507f1f77bcf86cd799439014"));
        
        scan_op.on_execute(nullptr);

        REQUIRE(scan_op.output() != nullptr);
        auto& chunk = scan_op.output()->data_chunk();
        REQUIRE(chunk.size() == 2);

        // Verify we got the right documents
        std::set<std::string> names;
        for (size_t i = 0; i < chunk.size(); ++i) {
            auto name_val = chunk.value(1, i);
            names.insert(std::string(name_val.value<std::string_view>()));
        }
        
        REQUIRE(names.count("Bob") == 1);
        REQUIRE(names.count("David") == 1);
    }
}

TEST_CASE("primary_key_scan: non-existent document", "[document_table][primary_key_scan]") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "pk_scan_not_found");

    // Insert one document
    std::pmr::vector<document::document_ptr> docs(&resource);
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439011", "Alice", 30));

    operator_insert insert_op(context.get());
    insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
    insert_op.on_execute(nullptr);

    SECTION("search for non-existent _id") {
        primary_key_scan scan_op(context.get(), nullptr);
        
        // Search for document that doesn't exist
        scan_op.append(document::document_id_t("507f1f77bcf86cd799999999"));
        
        scan_op.on_execute(nullptr);

        REQUIRE(scan_op.output() != nullptr);
        auto& chunk = scan_op.output()->data_chunk();
        REQUIRE(chunk.size() == 0); // Should return empty result
    }

    SECTION("search for mix of existing and non-existing _ids") {
        primary_key_scan scan_op(context.get(), nullptr);
        
        // Search for existing and non-existing
        scan_op.append(document::document_id_t("507f1f77bcf86cd799439011")); // exists
        scan_op.append(document::document_id_t("507f1f77bcf86cd799999999")); // doesn't exist
        
        scan_op.on_execute(nullptr);

        REQUIRE(scan_op.output() != nullptr);
        auto& chunk = scan_op.output()->data_chunk();
        REQUIRE(chunk.size() == 1); // Should return only the existing one

        auto name_val = chunk.value(1, 0);
        REQUIRE(std::string(name_val.value<std::string_view>()) == "Alice");
    }
}

TEST_CASE("primary_key_scan: empty search", "[document_table][primary_key_scan]") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "pk_scan_empty");

    // Insert documents
    std::pmr::vector<document::document_ptr> docs(&resource);
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439011", "Alice", 30));

    operator_insert insert_op(context.get());
    insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
    insert_op.on_execute(nullptr);

    SECTION("scan without any _id") {
        primary_key_scan scan_op(context.get(), nullptr);
        // Don't append any IDs
        
        scan_op.on_execute(nullptr);

        REQUIRE(scan_op.output() != nullptr);
        auto& chunk = scan_op.output()->data_chunk();
        REQUIRE(chunk.size() == 0); // Should return empty
    }
}

TEST_CASE("primary_key_scan: comparison with full_scan", "[document_table][primary_key_scan]") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "pk_scan_vs_full");

    // Insert test documents
    std::pmr::vector<document::document_ptr> docs(&resource);
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439011", "Alice", 30));
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439012", "Bob", 25));
    docs.push_back(create_test_doc(&resource, "507f1f77bcf86cd799439013", "Charlie", 35));

    operator_insert insert_op(context.get());
    insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
    insert_op.on_execute(nullptr);

    SECTION("primary_key_scan should give same result as full_scan for one document") {
        document::document_id_t search_id("507f1f77bcf86cd799439012");

        // Get result via primary_key_scan
        primary_key_scan pk_scan(context.get(), nullptr);
        pk_scan.append(search_id);
        pk_scan.on_execute(nullptr);

        auto& pk_chunk = pk_scan.output()->data_chunk();
        REQUIRE(pk_chunk.size() == 1);
        auto pk_name_val = pk_chunk.value(1, 0);
        auto pk_age_val = pk_chunk.value(2, 0);
        std::string pk_name = std::string(pk_name_val.value<std::string_view>());
        int64_t pk_age = pk_age_val.value<int64_t>();

        // Get result via full_scan (for comparison)
        full_scan full(context.get(), nullptr, logical_plan::limit_t::unlimit());
        full.on_execute(nullptr);

        auto& full_chunk = full.output()->data_chunk();
        REQUIRE(full_chunk.size() == 3); // All documents

        // Find Bob in full scan results
        bool found_bob = false;
        for (size_t i = 0; i < full_chunk.size(); ++i) {
            auto name_val = full_chunk.value(1, i);
            std::string name = std::string(name_val.value<std::string_view>());
            if (name == "Bob") {
                found_bob = true;
                auto age_val = full_chunk.value(2, i);
                int64_t age = age_val.value<int64_t>();
                REQUIRE(pk_name == "Bob");
                REQUIRE(pk_age == age);
                break;
            }
        }
        REQUIRE(found_bob);
    }
}

TEST_CASE("primary_key_scan: performance on large dataset", "[document_table][primary_key_scan][.performance]") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "pk_scan_performance");

    // Insert many documents
    const size_t NUM_DOCS = 10000;
    std::pmr::vector<document::document_ptr> docs(&resource);
    std::vector<std::string> ids;
    
    for (size_t i = 0; i < NUM_DOCS; ++i) {
        // Generate unique hex ID
        char id_hex[25];
        snprintf(id_hex, sizeof(id_hex), "507f1f77bcf86cd7994%05zu", i);
        ids.push_back(id_hex);
        
        docs.push_back(create_test_doc(&resource, id_hex, "User" + std::to_string(i), 20 + (i % 50)));
    }

    operator_insert insert_op(context.get());
    insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
    insert_op.on_execute(nullptr);

    REQUIRE(insert_op.modified()->size() == NUM_DOCS);

    SECTION("primary_key_scan should be fast O(1)") {
        // Search for a document in the middle
        document::document_id_t search_id(ids[NUM_DOCS / 2]);

        auto start = std::chrono::high_resolution_clock::now();
        
        primary_key_scan scan_op(context.get(), nullptr);
        scan_op.append(search_id);
        scan_op.on_execute(nullptr);
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        REQUIRE(scan_op.output() != nullptr);
        auto& chunk = scan_op.output()->data_chunk();
        REQUIRE(chunk.size() == 1);

        // Primary key scan should be very fast (< 1ms even for 10K docs)
        INFO("Primary key scan took: " << duration.count() << " microseconds");
        REQUIRE(duration.count() < 1000); // Should be under 1ms
    }

    SECTION("compare with full_scan performance") {
        document::document_id_t search_id(ids[NUM_DOCS / 2]);

        // Time primary_key_scan
        auto pk_start = std::chrono::high_resolution_clock::now();
        primary_key_scan pk_scan(context.get(), nullptr);
        pk_scan.append(search_id);
        pk_scan.on_execute(nullptr);
        auto pk_end = std::chrono::high_resolution_clock::now();
        auto pk_duration = std::chrono::duration_cast<std::chrono::microseconds>(pk_end - pk_start);

        // Time full_scan
        auto full_start = std::chrono::high_resolution_clock::now();
        full_scan full(context.get(), nullptr, logical_plan::limit_t::unlimit());
        full.on_execute(nullptr);
        auto full_end = std::chrono::high_resolution_clock::now();
        auto full_duration = std::chrono::duration_cast<std::chrono::microseconds>(full_end - full_start);

        INFO("Primary key scan: " << pk_duration.count() << " μs");
        INFO("Full scan: " << full_duration.count() << " μs");
        INFO("Speedup: " << static_cast<double>(full_duration.count()) / pk_duration.count() << "x");

        // Primary key scan should be significantly faster
        REQUIRE(pk_duration.count() < full_duration.count());
        
        // Should be at least 10x faster on 10K documents
        REQUIRE(full_duration.count() > pk_duration.count() * 10);
    }
}

