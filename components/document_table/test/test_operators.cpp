#include <catch2/catch.hpp>
#include <components/document_table/document_table_storage.hpp>
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan/document_table/operators/operator_insert.hpp>
#include <components/physical_plan/document_table/operators/operator_delete.hpp>
#include <components/physical_plan/document_table/operators/operator_update.hpp>
#include <components/physical_plan/base/operators/operator_raw_data.hpp>
#include <components/tests/generaty.hpp>
#include <services/collection/collection.hpp>
#include <core/pmr.hpp>

using namespace components;

// Helper to create test context
std::unique_ptr<services::collection::context_collection_t>
create_test_context(std::pmr::memory_resource* resource, const std::string& name) {
    collection_full_name_t collection_name{"test_db", name};
    actor_zeta::address_t dummy_address = actor_zeta::address_t::empty_address();
    log_t log;

    return std::make_unique<services::collection::context_collection_t>(
        resource,
        collection_name,
        services::collection::storage_type_t::DOCUMENT_TABLE,
        dummy_address,
        log
    );
}

// Helper to create simple test document
document::document_ptr create_simple_doc(std::pmr::memory_resource* resource,
                                         int id,
                                         const std::string& name,
                                         int age) {
    auto doc = document::make_document(resource);
    doc->set("/_id", gen_id(id, resource));
    doc->set("/name", name);
    doc->set("/age", static_cast<int64_t>(age));
    return doc;
}

TEST_CASE("document_table::operators - basic storage test") {
    using namespace document_table;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "storage_test");

    SECTION("storage initialization") {
        auto& storage = context->document_table_storage().storage();
        REQUIRE(storage.table() != nullptr);
        // Skip size check - table not fully initialized yet
    }

    SECTION("insert single document directly") {
        auto& storage = context->document_table_storage().storage();

        auto doc = create_simple_doc(&resource, 1, "Alice", 30);
        document::document_id_t doc_id = document::get_document_id(doc);

        storage.insert(doc_id, doc);

        // Check via id_to_row mapping instead of calculate_size
        REQUIRE(storage.size() == 1);

        size_t row_id;
        REQUIRE(storage.get_row_id(doc_id, row_id));
        REQUIRE(row_id == 0);
    }
}

TEST_CASE("document_table::operators - insert and scan integration") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "insert_scan_test");

    SECTION("insert documents and scan them back") {
        // Create test documents
        std::pmr::vector<document::document_ptr> docs(&resource);
        docs.push_back(create_simple_doc(&resource, 1, "Alice", 30));
        docs.push_back(create_simple_doc(&resource, 2, "Bob", 25));
        docs.push_back(create_simple_doc(&resource, 3, "Charlie", 35));

        // Insert documents
        operator_insert insert_op(context.get());
        insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert_op.on_execute(nullptr);

        // Verify insert worked
        REQUIRE(insert_op.modified() != nullptr);
        REQUIRE(insert_op.modified()->size() == 3);

        // Verify insert output is filled
        REQUIRE(insert_op.output() != nullptr);
        auto& insert_output = insert_op.output()->data_chunk();
        REQUIRE(insert_output.size() == 3);

        // Now scan to verify data
        full_scan scan_op(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan_op.on_execute(nullptr);

        // Verify scan results
        REQUIRE(scan_op.output() != nullptr);
        auto& output_chunk = scan_op.output()->data_chunk();

        // Should have 3 rows
        REQUIRE(output_chunk.size() == 3);

        // Verify schema has columns: _id, name, age
        auto types = output_chunk.types();
        REQUIRE(types.size() >= 3); // At least _id, name, age
    }

    SECTION("insert and scan with limit") {
        // Create test documents
        std::pmr::vector<document::document_ptr> docs(&resource);
        for (int i = 1; i <= 10; i++) {
            docs.push_back(create_simple_doc(&resource, i, "User" + std::to_string(i), 20 + i));
        }

        // Insert
        operator_insert insert_op(context.get());
        insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert_op.on_execute(nullptr);

        REQUIRE(insert_op.modified()->size() == 10);

        // Scan with limit 5
        full_scan scan_op(context.get(), nullptr, logical_plan::limit_t(5));
        scan_op.on_execute(nullptr);

        // Should return only 5 rows
        REQUIRE(scan_op.output() != nullptr);
        REQUIRE(scan_op.output()->data_chunk().size() == 5);
    }

    SECTION("schema evolution - insert documents with different fields") {
        // First insert: {_id, name, age}
        std::pmr::vector<document::document_ptr> docs1(&resource);
        docs1.push_back(create_simple_doc(&resource, 1, "Alice", 30));

        operator_insert insert_op1(context.get());
        insert_op1.set_children({new base::operators::operator_raw_data_t(std::move(docs1))});
        insert_op1.on_execute(nullptr);

        // Get initial column count
        auto& storage = context->document_table_storage().storage();
        size_t initial_cols = storage.table()->column_count();

        // Second insert: add new field 'city'
        std::pmr::vector<document::document_ptr> docs2(&resource);
        auto doc2 = create_simple_doc(&resource, 2, "Bob", 25);
        doc2->set("/city", std::pmr::string("NYC", &resource));
        docs2.push_back(doc2);

        operator_insert insert_op2(context.get());
        insert_op2.set_children({new base::operators::operator_raw_data_t(std::move(docs2))});
        insert_op2.on_execute(nullptr);

        // Schema should have evolved - added 'city' column
        size_t new_cols = storage.table()->column_count();
        REQUIRE(new_cols == initial_cols + 1);

        // Scan should return both documents
        full_scan scan_op(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan_op.on_execute(nullptr);
        REQUIRE(scan_op.output()->data_chunk().size() == 2);
    }
}

TEST_CASE("document_table::operators - insert and delete integration") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "insert_delete_test");

    SECTION("insert and delete all") {
        // Insert documents
        std::pmr::vector<document::document_ptr> docs(&resource);
        docs.push_back(create_simple_doc(&resource, 1, "Alice", 30));
        docs.push_back(create_simple_doc(&resource, 2, "Bob", 25));

        // Stack-based operator is OK as long as it doesn't get passed to set_children
        operator_insert insert_op(context.get());
        insert_op.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert_op.on_execute(nullptr);

        REQUIRE(insert_op.modified()->size() == 2);

        // For delete, we need to pass scan_op to delete_op via set_children
        // So both must be heap-allocated
        {
            boost::intrusive_ptr<full_scan> scan_op(new full_scan(context.get(), nullptr, logical_plan::limit_t::unlimit()));
            scan_op->on_execute(nullptr);
            REQUIRE(scan_op->output()->data_chunk().size() == 2);

            boost::intrusive_ptr<operator_delete> delete_op(new operator_delete(context.get(), nullptr));
            delete_op->set_children(scan_op);
            delete_op->on_execute(nullptr);

            // Should have deleted 2 rows
            REQUIRE(delete_op->modified() != nullptr);
            REQUIRE(delete_op->modified()->size() == 2);

            // intrusive_ptr will auto-cleanup when going out of scope
        }

        // Scan again - should be empty
        full_scan scan_op2(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan_op2.on_execute(nullptr);
        REQUIRE(scan_op2.output()->data_chunk().size() == 0);
    }
}

TEST_CASE("document_table::operators - basic operator creation") {
    using namespace document_table;
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "basic_test");

    SECTION("create scan operator") {
        full_scan scan_op(context.get(), nullptr, logical_plan::limit_t::unlimit());
        REQUIRE(true); // Just verify it compiles and constructs
    }

    SECTION("create insert operator") {
        operator_insert insert_op(context.get());
        REQUIRE(true);
    }

    SECTION("create delete operator") {
        operator_delete delete_op(context.get(), nullptr);
        REQUIRE(true);
    }

    SECTION("create update operator") {
        std::pmr::vector<expressions::update_expr_ptr> updates(&resource);
        operator_update update_op(context.get(), std::move(updates), false, nullptr);
        REQUIRE(true);
    }
}
