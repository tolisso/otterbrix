#include <catch2/catch.hpp>
#include <components/document_table/document_table_storage.hpp>
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan/document_table/operators/operator_insert.hpp>
#include <components/physical_plan/document_table/operators/operator_delete.hpp>
#include <components/physical_plan/document_table/operators/operator_update.hpp>
#include <components/physical_plan/base/operators/operator_raw_data.hpp>
#include <services/collection/collection.hpp>
#include <core/pmr.hpp>

TEST_CASE("document_table::operators::full_scan") {
    using namespace components;
    using namespace components::document_table;
    using namespace components::document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();

    // Create a simple context with document_table storage
    collection_full_name_t collection_name{"test_db", "test_collection"};
    actor_zeta::address_t dummy_address = actor_zeta::address_t::empty_address();
    log_t log;

    auto context = std::make_unique<services::collection::context_collection_t>(
        &resource,
        collection_name,
        services::collection::storage_type_t::DOCUMENT_TABLE,
        dummy_address,
        log
    );

    // Get document_table_storage
    auto& storage = context->document_table_storage().storage();

    SECTION("create scan operator") {
        // Create full_scan operator with no filter
        expressions::compare_expression_ptr no_filter = nullptr;
        logical_plan::limit_t no_limit = logical_plan::limit_t::unlimit();

        auto scan_op = std::make_unique<full_scan>(
            context.get(),
            no_filter,
            no_limit
        );

        // Just verify operator was created successfully
        REQUIRE(scan_op != nullptr);

        // TODO: Execute scan after implementing operator_insert
        // scan_op->on_execute(nullptr);
        // REQUIRE(scan_op->output() != nullptr);
    }

    SECTION("create scan operator with limit") {
        expressions::compare_expression_ptr no_filter = nullptr;
        logical_plan::limit_t limit_5(5); // limit 5 rows

        auto scan_op = std::make_unique<full_scan>(
            context.get(),
            no_filter,
            limit_5
        );

        REQUIRE(scan_op != nullptr);

        // TODO: Test actual scan with limit after implementing operator_insert
        // scan_op->on_execute(nullptr);
        // REQUIRE(scan_op->output() != nullptr);
        // REQUIRE(scan_op->output()->data_chunk().size() <= 5);
    }
}

TEST_CASE("document_table::operators::operator_insert") {
    using namespace components;
    using namespace components::document_table;
    using namespace components::document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();

    // Create context with document_table storage
    collection_full_name_t collection_name{"test_db", "test_insert"};
    actor_zeta::address_t dummy_address = actor_zeta::address_t::empty_address();
    log_t log;

    auto context = std::make_unique<services::collection::context_collection_t>(
        &resource,
        collection_name,
        services::collection::storage_type_t::DOCUMENT_TABLE,
        dummy_address,
        log
    );

    auto& storage = context->document_table_storage().storage();

    SECTION("create insert operator") {
        // Just verify operator can be created
        auto insert_op = std::make_unique<operator_insert>(context.get());
        REQUIRE(insert_op != nullptr);

        // TODO: Test actual insertion after resolving document API
        // Need to understand proper way to create and populate documents
        // For now, just verify basic construction works
    }
}

TEST_CASE("document_table::operators::operator_delete") {
    using namespace components;
    using namespace components::document_table;
    using namespace components::document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();

    collection_full_name_t collection_name{"test_db", "test_delete"};
    actor_zeta::address_t dummy_address = actor_zeta::address_t::empty_address();
    log_t log;

    auto context = std::make_unique<services::collection::context_collection_t>(
        &resource,
        collection_name,
        services::collection::storage_type_t::DOCUMENT_TABLE,
        dummy_address,
        log
    );

    SECTION("create delete operator") {
        expressions::compare_expression_ptr no_filter = nullptr;
        auto delete_op = std::make_unique<operator_delete>(context.get(), no_filter);
        REQUIRE(delete_op != nullptr);
    }

    SECTION("create delete operator with filter") {
        // TODO: Create a proper compare expression
        expressions::compare_expression_ptr filter = nullptr;
        auto delete_op = std::make_unique<operator_delete>(context.get(), filter);
        REQUIRE(delete_op != nullptr);
    }
}

TEST_CASE("document_table::operators::operator_update") {
    using namespace components;
    using namespace components::document_table;
    using namespace components::document_table::operators;

    auto resource = std::pmr::synchronized_pool_resource();

    collection_full_name_t collection_name{"test_db", "test_update"};
    actor_zeta::address_t dummy_address = actor_zeta::address_t::empty_address();
    log_t log;

    auto context = std::make_unique<services::collection::context_collection_t>(
        &resource,
        collection_name,
        services::collection::storage_type_t::DOCUMENT_TABLE,
        dummy_address,
        log
    );

    SECTION("create update operator") {
        std::pmr::vector<expressions::update_expr_ptr> updates(&resource);
        bool upsert = false;
        expressions::compare_expression_ptr filter = nullptr;

        auto update_op = std::make_unique<operator_update>(
            context.get(),
            std::move(updates),
            upsert,
            filter
        );
        REQUIRE(update_op != nullptr);
    }

    SECTION("create upsert operator") {
        std::pmr::vector<expressions::update_expr_ptr> updates(&resource);
        bool upsert = true;
        expressions::compare_expression_ptr filter = nullptr;

        auto update_op = std::make_unique<operator_update>(
            context.get(),
            std::move(updates),
            upsert,
            filter
        );
        REQUIRE(update_op != nullptr);
    }
}

