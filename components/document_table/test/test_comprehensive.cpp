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
create_context(std::pmr::memory_resource* resource, const std::string& name) {
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

// Helper to create document
document::document_ptr make_doc(std::pmr::memory_resource* resource,
                                int id,
                                const std::string& name,
                                int age,
                                const std::string& city = "") {
    auto doc = document::make_document(resource);
    doc->set("/_id", gen_id(id, resource));
    doc->set("/name", name);
    doc->set("/age", static_cast<int64_t>(age));
    if (!city.empty()) {
        doc->set("/city", city);
    }
    return doc;
}

TEST_CASE("document_table - comprehensive CRUD tests") {
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_context(&resource, "crud_test");

    SECTION("INSERT multiple batches and verify count") {
        // First batch
        std::pmr::vector<document::document_ptr> batch1(&resource);
        batch1.push_back(make_doc(&resource, 1, "Alice", 30, "NYC"));
        batch1.push_back(make_doc(&resource, 2, "Bob", 25, "LA"));

        operator_insert insert1(context.get());
        insert1.set_children({new base::operators::operator_raw_data_t(std::move(batch1))});
        insert1.on_execute(nullptr);

        REQUIRE(insert1.modified()->size() == 2);
        REQUIRE(insert1.output()->data_chunk().size() == 2);

        // Second batch - different schema (no city)
        std::pmr::vector<document::document_ptr> batch2(&resource);
        batch2.push_back(make_doc(&resource, 3, "Charlie", 35));
        batch2.push_back(make_doc(&resource, 4, "Diana", 28));

        operator_insert insert2(context.get());
        insert2.set_children({new base::operators::operator_raw_data_t(std::move(batch2))});
        insert2.on_execute(nullptr);

        REQUIRE(insert2.modified()->size() == 2);

        // Scan all - should have 4 documents
        full_scan scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);

        REQUIRE(scan.output() != nullptr);
        REQUIRE(scan.output()->data_chunk().size() == 4);
    }

    SECTION("UPDATE documents") {
        // Insert initial data
        std::pmr::vector<document::document_ptr> docs(&resource);
        docs.push_back(make_doc(&resource, 1, "Alice", 30));
        docs.push_back(make_doc(&resource, 2, "Bob", 25));
        docs.push_back(make_doc(&resource, 3, "Charlie", 30));

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        // TODO: Test UPDATE when we have update expressions working
        // For now just verify insert worked
        REQUIRE(insert.modified()->size() == 3);
    }

    SECTION("DELETE with full scan") {
        // Insert data
        std::pmr::vector<document::document_ptr> docs(&resource);
        docs.push_back(make_doc(&resource, 1, "Alice", 30));
        docs.push_back(make_doc(&resource, 2, "Bob", 25));
        docs.push_back(make_doc(&resource, 3, "Charlie", 35));

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        REQUIRE(insert.modified()->size() == 3);

        // Delete using intrusive_ptr pattern
        {
            boost::intrusive_ptr<full_scan> scan_op(
                new full_scan(context.get(), nullptr, logical_plan::limit_t::unlimit()));
            scan_op->on_execute(nullptr);
            REQUIRE(scan_op->output()->data_chunk().size() == 3);

            boost::intrusive_ptr<operator_delete> delete_op(
                new operator_delete(context.get(), nullptr));
            delete_op->set_children(scan_op);
            delete_op->on_execute(nullptr);

            REQUIRE(delete_op->modified() != nullptr);
            REQUIRE(delete_op->modified()->size() == 3);
        }

        // Verify all deleted
        full_scan verify_scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        verify_scan.on_execute(nullptr);
        REQUIRE(verify_scan.output()->data_chunk().size() == 0);
    }

    SECTION("SELECT with LIMIT") {
        // Insert 10 documents
        std::pmr::vector<document::document_ptr> docs(&resource);
        for (int i = 1; i <= 10; ++i) {
            docs.push_back(make_doc(&resource, i, "User" + std::to_string(i), 20 + i));
        }

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        REQUIRE(insert.modified()->size() == 10);

        // Scan with LIMIT 5
        full_scan scan(context.get(), nullptr, logical_plan::limit_t(5));
        scan.on_execute(nullptr);

        REQUIRE(scan.output()->data_chunk().size() == 5);
    }
}

TEST_CASE("document_table - schema evolution tests") {
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_context(&resource, "schema_test");

    SECTION("adding new fields progressively") {
        // Start with minimal schema
        std::pmr::vector<document::document_ptr> batch1(&resource);
        auto doc1 = document::make_document(&resource);
        doc1->set("/_id", gen_id(1, &resource));
        doc1->set("/name", std::string("Alice"));
        batch1.push_back(std::move(doc1));

        operator_insert insert1(context.get());
        insert1.set_children({new base::operators::operator_raw_data_t(std::move(batch1))});
        insert1.on_execute(nullptr);

        auto& storage = context->document_table_storage().storage();
        auto schema1 = storage.to_column_definitions();
        REQUIRE(schema1.size() == 2); // _id, name

        // Add age field
        std::pmr::vector<document::document_ptr> batch2(&resource);
        auto doc2 = document::make_document(&resource);
        doc2->set("/_id", gen_id(2, &resource));
        doc2->set("/name", std::string("Bob"));
        doc2->set("/age", static_cast<int64_t>(25));
        batch2.push_back(std::move(doc2));

        operator_insert insert2(context.get());
        insert2.set_children({new base::operators::operator_raw_data_t(std::move(batch2))});
        insert2.on_execute(nullptr);

        auto schema2 = storage.to_column_definitions();
        REQUIRE(schema2.size() == 3); // _id, name, age

        // Add city field
        std::pmr::vector<document::document_ptr> batch3(&resource);
        auto doc3 = document::make_document(&resource);
        doc3->set("/_id", gen_id(3, &resource));
        doc3->set("/name", std::string("Charlie"));
        doc3->set("/age", static_cast<int64_t>(30));
        doc3->set("/city", std::string("NYC"));
        batch3.push_back(std::move(doc3));

        operator_insert insert3(context.get());
        insert3.set_children({new base::operators::operator_raw_data_t(std::move(batch3))});
        insert3.on_execute(nullptr);

        auto schema3 = storage.to_column_definitions();
        REQUIRE(schema3.size() == 4); // _id, name, age, city

        // Verify all 3 documents are still accessible
        full_scan scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->data_chunk().size() == 3);
    }

    SECTION("mixed documents with different fields") {
        std::pmr::vector<document::document_ptr> docs(&resource);

        // Document with fields: _id, name, age
        auto doc1 = document::make_document(&resource);
        doc1->set("/_id", gen_id(1, &resource));
        doc1->set("/name", std::string("Alice"));
        doc1->set("/age", static_cast<int64_t>(30));
        docs.push_back(std::move(doc1));

        // Document with fields: _id, name, city
        auto doc2 = document::make_document(&resource);
        doc2->set("/_id", gen_id(2, &resource));
        doc2->set("/name", std::string("Bob"));
        doc2->set("/city", std::string("LA"));
        docs.push_back(std::move(doc2));

        // Document with fields: _id, email
        auto doc3 = document::make_document(&resource);
        doc3->set("/_id", gen_id(3, &resource));
        doc3->set("/email", std::string("charlie@example.com"));
        docs.push_back(std::move(doc3));

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        REQUIRE(insert.modified()->size() == 3);

        auto& storage = context->document_table_storage().storage();
        auto schema = storage.to_column_definitions();

        // Should have: _id, name, age, city, email
        REQUIRE(schema.size() == 5);

        // Verify all documents scanned
        full_scan scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->data_chunk().size() == 3);
    }
}

TEST_CASE("document_table - large dataset tests") {
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_context(&resource, "large_test");

    SECTION("insert 1000 documents") {
        std::pmr::vector<document::document_ptr> docs(&resource);
        for (int i = 1; i <= 1000; ++i) {
            docs.push_back(make_doc(&resource, i, "User" + std::to_string(i), 20 + (i % 50)));
        }

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        REQUIRE(insert.modified()->size() == 1000);

        // Verify scan
        full_scan scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->data_chunk().size() == 1000);
    }

    SECTION("batch inserts - 10 batches of 100") {
        for (int batch = 0; batch < 10; ++batch) {
            std::pmr::vector<document::document_ptr> docs(&resource);
            for (int i = 1; i <= 100; ++i) {
                int id = batch * 100 + i;
                docs.push_back(make_doc(&resource, id, "User" + std::to_string(id), 20 + (id % 50)));
            }

            operator_insert insert(context.get());
            insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
            insert.on_execute(nullptr);

            REQUIRE(insert.modified()->size() == 100);
        }

        // Verify total count
        full_scan scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->data_chunk().size() == 1000);
    }
}

TEST_CASE("document_table - edge cases") {
    using namespace document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_context(&resource, "edge_test");

    SECTION("empty insert") {
        std::pmr::vector<document::document_ptr> docs(&resource);

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        REQUIRE(insert.modified()->size() == 0);
    }

    SECTION("scan empty collection") {
        full_scan scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);

        REQUIRE(scan.output() != nullptr);
        REQUIRE(scan.output()->data_chunk().size() == 0);
    }

    SECTION("delete from empty collection") {
        boost::intrusive_ptr<full_scan> scan_op(
            new full_scan(context.get(), nullptr, logical_plan::limit_t::unlimit()));
        scan_op->on_execute(nullptr);

        boost::intrusive_ptr<operator_delete> delete_op(
            new operator_delete(context.get(), nullptr));
        delete_op->set_children(scan_op);
        delete_op->on_execute(nullptr);

        REQUIRE(delete_op->modified()->size() == 0);
    }

    SECTION("LIMIT 0") {
        // Insert data
        std::pmr::vector<document::document_ptr> docs(&resource);
        docs.push_back(make_doc(&resource, 1, "Alice", 30));

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        // Scan with LIMIT 0
        full_scan scan(context.get(), nullptr, logical_plan::limit_t(0));
        scan.on_execute(nullptr);

        REQUIRE(scan.output()->data_chunk().size() == 0);
    }

    SECTION("document with only _id") {
        std::pmr::vector<document::document_ptr> docs(&resource);
        auto doc = document::make_document(&resource);
        doc->set("/_id", gen_id(1, &resource));
        docs.push_back(std::move(doc));

        operator_insert insert(context.get());
        insert.set_children({new base::operators::operator_raw_data_t(std::move(docs))});
        insert.on_execute(nullptr);

        REQUIRE(insert.modified()->size() == 1);

        full_scan scan(context.get(), nullptr, logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->data_chunk().size() == 1);
    }
}
