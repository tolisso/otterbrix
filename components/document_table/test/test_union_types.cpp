#include <catch2/catch.hpp>
#include <components/document_table/document_table_storage.hpp>
#include <components/physical_plan/document_table/operators/operator_insert.hpp>
#include <components/physical_plan/base/operators/operator_raw_data.hpp>
#include <components/tests/generaty.hpp>
#include <services/collection/collection.hpp>
#include <core/pmr.hpp>

using namespace components;
using namespace document_table;

namespace {
    // Helper to create test context with document_table storage
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

} // anonymous namespace

TEST_CASE("Union Types - Basic union creation on type conflict") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_basic");

    // Вставляем документ с целым числом
    std::pmr::vector<document::document_ptr> batch1(&resource);
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/age", static_cast<int64_t>(30));
    batch1.push_back(std::move(doc1));

    operator_insert insert1(context.get());
    insert1.set_children({new operator_raw_data_t(std::move(batch1))});
    insert1.on_execute(nullptr);

    auto& storage = context->document_table_storage().storage();
    
    // Проверяем, что колонка age существует и имеет тип BIGINT
    auto* col = storage.schema().get_column_info("age");
    REQUIRE(col != nullptr);
    CHECK(col->type.type() == types::logical_type::BIGINT);
    CHECK_FALSE(col->is_union);

    // Вставляем документ со строкой - должен создаться union!
    std::pmr::vector<document::document_ptr> batch2(&resource);
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/age", std::string("thirty"));
    batch2.push_back(std::move(doc2));

    operator_insert insert2(context.get());
    insert2.set_children({new operator_raw_data_t(std::move(batch2))});
    REQUIRE_NOTHROW(insert2.on_execute(nullptr));

    // Проверяем, что колонка стала union (тип остается BIGINT, но is_union=true)
    col = storage.schema().get_column_info("age");
    REQUIRE(col != nullptr);
    CHECK(col->type.type() == types::logical_type::BIGINT); // Остается первый тип
    CHECK(col->is_union); // Но помечена как union
    REQUIRE(col->union_types.size() == 2);
    CHECK(col->union_types[0] == types::logical_type::BIGINT);
    CHECK(col->union_types[1] == types::logical_type::STRING_LITERAL);
}

TEST_CASE("Union Types - Extend union with third type") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_extend");

    // Документ 1: int
    std::pmr::vector<document::document_ptr> batch1(&resource);
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/value", static_cast<int64_t>(42));
    batch1.push_back(std::move(doc1));

    operator_insert insert1(context.get());
    insert1.set_children({new operator_raw_data_t(std::move(batch1))});
    insert1.on_execute(nullptr);

    // Документ 2: string
    std::pmr::vector<document::document_ptr> batch2(&resource);
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/value", std::string("text"));
    batch2.push_back(std::move(doc2));

    operator_insert insert2(context.get());
    insert2.set_children({new operator_raw_data_t(std::move(batch2))});
    insert2.on_execute(nullptr);

    auto& storage = context->document_table_storage().storage();
    auto* col = storage.schema().get_column_info("value");
    REQUIRE(col->is_union);
    CHECK(col->union_types.size() == 2);

    // Документ 3: для теста используем boolean вместо double (так как document API не поддерживает double через set)
    std::pmr::vector<document::document_ptr> batch3(&resource);
    auto doc3 = document::make_document(&resource);
    doc3->set("/_id", gen_id(3, &resource));
    doc3->set("/value", true); // boolean вместо double для теста
    batch3.push_back(std::move(doc3));

    operator_insert insert3(context.get());
    insert3.set_children({new operator_raw_data_t(std::move(batch3))});
    REQUIRE_NOTHROW(insert3.on_execute(nullptr));

    col = storage.schema().get_column_info("value");
    REQUIRE(col->is_union);
    CHECK(col->type.type() == types::logical_type::BIGINT); // Первый тип
    CHECK(col->union_types.size() == 3);
    CHECK(col->union_types[0] == types::logical_type::BIGINT);
    CHECK(col->union_types[1] == types::logical_type::STRING_LITERAL);
    CHECK(col->union_types[2] == types::logical_type::BOOLEAN); // boolean вместо double
}

TEST_CASE("Union Types - get_union_tag returns correct indices") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_tags");

    // Создаем union с тремя типами
    std::pmr::vector<document::document_ptr> batch1(&resource);
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/x", static_cast<int64_t>(1));
    batch1.push_back(std::move(doc1));

    operator_insert insert1(context.get());
    insert1.set_children({new operator_raw_data_t(std::move(batch1))});
    insert1.on_execute(nullptr);

    std::pmr::vector<document::document_ptr> batch2(&resource);
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/x", std::string("two"));
    batch2.push_back(std::move(doc2));

    operator_insert insert2(context.get());
    insert2.set_children({new operator_raw_data_t(std::move(batch2))});
    insert2.on_execute(nullptr);

    std::pmr::vector<document::document_ptr> batch3(&resource);
    auto doc3 = document::make_document(&resource);
    doc3->set("/_id", gen_id(3, &resource));
    doc3->set("/x", true); // boolean вместо double
    batch3.push_back(std::move(doc3));

    operator_insert insert3(context.get());
    insert3.set_children({new operator_raw_data_t(std::move(batch3))});
    insert3.on_execute(nullptr);

    auto& storage = context->document_table_storage().storage();
    auto* col = storage.schema().get_column_info("x");
    REQUIRE(col->is_union);
    CHECK(col->type.type() == types::logical_type::BIGINT); // Первый тип

    // Проверяем tag для каждого типа
    CHECK(storage.schema().get_union_tag(col, types::logical_type::BIGINT) == 0);
    CHECK(storage.schema().get_union_tag(col, types::logical_type::STRING_LITERAL) == 1);
    CHECK(storage.schema().get_union_tag(col, types::logical_type::BOOLEAN) == 2); // boolean вместо double
}

TEST_CASE("Union Types - Multiple columns with different unions") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_multi");

    // Документ 1: a=int, b=string
    std::pmr::vector<document::document_ptr> batch1(&resource);
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/a", static_cast<int64_t>(1));
    doc1->set("/b", std::string("text"));
    batch1.push_back(std::move(doc1));

    operator_insert insert1(context.get());
    insert1.set_children({new operator_raw_data_t(std::move(batch1))});
    insert1.on_execute(nullptr);

    // Документ 2: a=string, b=int
    std::pmr::vector<document::document_ptr> batch2(&resource);
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/a", std::string("one"));
    doc2->set("/b", static_cast<int64_t>(2));
    batch2.push_back(std::move(doc2));

    operator_insert insert2(context.get());
    insert2.set_children({new operator_raw_data_t(std::move(batch2))});
    insert2.on_execute(nullptr);

    auto& storage = context->document_table_storage().storage();
    
    // Проверяем, что обе колонки стали union
    auto* col_a = storage.schema().get_column_info("a");
    auto* col_b = storage.schema().get_column_info("b");

    REQUIRE(col_a->is_union);
    REQUIRE(col_b->is_union);

    // Проверяем типы в каждом union
    CHECK(col_a->type.type() == types::logical_type::BIGINT); // Первый тип
    CHECK(col_a->union_types[0] == types::logical_type::BIGINT);
    CHECK(col_a->union_types[1] == types::logical_type::STRING_LITERAL);

    CHECK(col_b->type.type() == types::logical_type::STRING_LITERAL); // Первый тип
    CHECK(col_b->union_types[0] == types::logical_type::STRING_LITERAL);
    CHECK(col_b->union_types[1] == types::logical_type::BIGINT);
}

TEST_CASE("Union Types - NULL values handled correctly") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_nulls");

    // Создаем union из int и string
    std::pmr::vector<document::document_ptr> batch1(&resource);
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/field", static_cast<int64_t>(100));
    batch1.push_back(std::move(doc1));

    operator_insert insert1(context.get());
    insert1.set_children({new operator_raw_data_t(std::move(batch1))});
    insert1.on_execute(nullptr);

    std::pmr::vector<document::document_ptr> batch2(&resource);
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/field", std::string("text"));
    batch2.push_back(std::move(doc2));

    operator_insert insert2(context.get());
    insert2.set_children({new operator_raw_data_t(std::move(batch2))});
    insert2.on_execute(nullptr);

    // Вставляем документ без поля field
    std::pmr::vector<document::document_ptr> batch3(&resource);
    auto doc3 = document::make_document(&resource);
    doc3->set("/_id", gen_id(3, &resource));
    doc3->set("/other", std::string("data"));
    batch3.push_back(std::move(doc3));

    operator_insert insert3(context.get());
    insert3.set_children({new operator_raw_data_t(std::move(batch3))});
    REQUIRE_NOTHROW(insert3.on_execute(nullptr));

    auto& storage = context->document_table_storage().storage();
    auto* col = storage.schema().get_column_info("field");
    REQUIRE(col->is_union);
    CHECK(col->union_types.size() == 2);
}
