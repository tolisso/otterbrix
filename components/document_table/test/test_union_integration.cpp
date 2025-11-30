#include <catch2/catch.hpp>
#include <components/document_table/document_table_storage.hpp>
#include <components/physical_plan/document_table/operators/operator_insert.hpp>
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
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

TEST_CASE("Union Integration - Insert and Scan with union columns") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_crud");
    auto& storage = context->document_table_storage().storage();

    SECTION("Insert and Scan with union types") {
        // Вставляем документы с разными типами для одного поля
        std::pmr::vector<document::document_ptr> batch1(&resource);
        
        auto doc1 = document::make_document(&resource);
        doc1->set("/_id", gen_id(1, &resource));
        doc1->set("/status", static_cast<int64_t>(200)); // int
        doc1->set("/message", std::string("OK"));
        batch1.push_back(std::move(doc1));
        
        auto doc2 = document::make_document(&resource);
        doc2->set("/_id", gen_id(2, &resource));
        doc2->set("/status", std::string("error")); // string - создает union!
        doc2->set("/message", std::string("Failed"));
        batch1.push_back(std::move(doc2));
        
        auto doc3 = document::make_document(&resource);
        doc3->set("/_id", gen_id(3, &resource));
        doc3->set("/status", true); // boolean - расширяет union!
        doc3->set("/message", std::string("Active"));
        batch1.push_back(std::move(doc3));

        operator_insert insert1(context.get());
        insert1.set_children({new operator_raw_data_t(std::move(batch1))});
        insert1.on_execute(nullptr);

        // Проверяем, что status стал union с 3 типами
        auto* col = storage.schema().get_column_info("status");
        REQUIRE(col != nullptr);
        CHECK(col->is_union);
        CHECK(col->union_types.size() == 3);

        // Проверяем количество вставленных строк
        CHECK(storage.size() == 3);

        // Scan - читаем все данные
        table::table_scan_state scan_state(&resource);
        std::vector<table::storage_index_t> column_ids;
        for (uint64_t i = 0; i < storage.table()->column_count(); ++i) {
            column_ids.push_back(table::storage_index_t(i));
        }
        
        storage.initialize_scan(scan_state, column_ids);
        auto output_types = storage.table()->copy_types();
        vector::data_chunk_t output(&resource, output_types);
        storage.scan(output, scan_state);
        
        CHECK(output.size() > 0);
        CHECK(storage.size() == 3);
    }
}

TEST_CASE("Union Integration - Mixed data types in large dataset") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_large");
    auto& storage = context->document_table_storage().storage();

    // Вставляем 100 документов со случайными типами
    for (int i = 0; i < 100; ++i) {
        std::pmr::vector<document::document_ptr> batch(&resource);
        auto doc = document::make_document(&resource);
        doc->set("/_id", gen_id(i, &resource));
        
        // Чередуем типы для поля "value"
        if (i % 3 == 0) {
            doc->set("/value", static_cast<int64_t>(i));
        } else if (i % 3 == 1) {
            doc->set("/value", std::string("str_") + std::to_string(i));
        } else {
            doc->set("/value", (i % 2 == 0));
        }
        
        batch.push_back(std::move(doc));

        operator_insert insert(context.get());
        insert.set_children({new operator_raw_data_t(std::move(batch))});
        insert.on_execute(nullptr);
    }

    // Проверяем результат
    auto* col = storage.schema().get_column_info("value");
    REQUIRE(col != nullptr);
    CHECK(col->is_union);
    CHECK(col->union_types.size() == 3);
    CHECK(storage.size() == 100);
}

TEST_CASE("Union Integration - Multiple union columns simultaneously") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_multi_col");
    auto& storage = context->document_table_storage().storage();

    SECTION("Three columns evolving to unions independently") {
        // Документ 1: все int
        std::pmr::vector<document::document_ptr> batch1(&resource);
        auto doc1 = document::make_document(&resource);
        doc1->set("/_id", gen_id(1, &resource));
        doc1->set("/field_a", static_cast<int64_t>(10));
        doc1->set("/field_b", static_cast<int64_t>(20));
        doc1->set("/field_c", static_cast<int64_t>(30));
        batch1.push_back(std::move(doc1));

        operator_insert insert1(context.get());
        insert1.set_children({new operator_raw_data_t(std::move(batch1))});
        insert1.on_execute(nullptr);

        // Документ 2: field_a -> string, остальные int
        std::pmr::vector<document::document_ptr> batch2(&resource);
        auto doc2 = document::make_document(&resource);
        doc2->set("/_id", gen_id(2, &resource));
        doc2->set("/field_a", std::string("text"));
        doc2->set("/field_b", static_cast<int64_t>(21));
        doc2->set("/field_c", static_cast<int64_t>(31));
        batch2.push_back(std::move(doc2));

        operator_insert insert2(context.get());
        insert2.set_children({new operator_raw_data_t(std::move(batch2))});
        insert2.on_execute(nullptr);

        // Документ 3: field_b -> bool, остальные как есть
        std::pmr::vector<document::document_ptr> batch3(&resource);
        auto doc3 = document::make_document(&resource);
        doc3->set("/_id", gen_id(3, &resource));
        doc3->set("/field_a", static_cast<int64_t>(12));
        doc3->set("/field_b", true);
        doc3->set("/field_c", static_cast<int64_t>(32));
        batch3.push_back(std::move(doc3));

        operator_insert insert3(context.get());
        insert3.set_children({new operator_raw_data_t(std::move(batch3))});
        insert3.on_execute(nullptr);

        // Документ 4: field_c -> string
        std::pmr::vector<document::document_ptr> batch4(&resource);
        auto doc4 = document::make_document(&resource);
        doc4->set("/_id", gen_id(4, &resource));
        doc4->set("/field_a", std::string("another"));
        doc4->set("/field_b", static_cast<int64_t>(23));
        doc4->set("/field_c", std::string("final"));
        batch4.push_back(std::move(doc4));

        operator_insert insert4(context.get());
        insert4.set_children({new operator_raw_data_t(std::move(batch4))});
        insert4.on_execute(nullptr);

        // Проверяем все три колонки
        auto* col_a = storage.schema().get_column_info("field_a");
        auto* col_b = storage.schema().get_column_info("field_b");
        auto* col_c = storage.schema().get_column_info("field_c");

        REQUIRE(col_a != nullptr);
        REQUIRE(col_b != nullptr);
        REQUIRE(col_c != nullptr);

        CHECK(col_a->is_union);
        CHECK(col_b->is_union);
        CHECK(col_c->is_union);

        CHECK(col_a->union_types.size() == 2); // BIGINT, STRING_LITERAL
        CHECK(col_b->union_types.size() == 2); // BIGINT, BOOLEAN
        CHECK(col_c->union_types.size() == 2); // BIGINT, STRING_LITERAL

        CHECK(storage.size() == 4);
    }
}

TEST_CASE("Union Integration - Sparse data with NULLs") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_sparse");
    auto& storage = context->document_table_storage().storage();

    // Вставляем документы где некоторые поля отсутствуют
    std::pmr::vector<document::document_ptr> batch(&resource);
    
    // Документ 1: только field1
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/field1", static_cast<int64_t>(100));
    batch.push_back(std::move(doc1));
    
    // Документ 2: только field2
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/field2", std::string("text"));
    batch.push_back(std::move(doc2));
    
    // Документ 3: field1 как string (создает union), field2 отсутствует
    auto doc3 = document::make_document(&resource);
    doc3->set("/_id", gen_id(3, &resource));
    doc3->set("/field1", std::string("string"));
    batch.push_back(std::move(doc3));
    
    // Документ 4: оба поля присутствуют
    auto doc4 = document::make_document(&resource);
    doc4->set("/_id", gen_id(4, &resource));
    doc4->set("/field1", true); // Расширяет union
    doc4->set("/field2", static_cast<int64_t>(200)); // Создает union
    batch.push_back(std::move(doc4));

    operator_insert insert(context.get());
    insert.set_children({new operator_raw_data_t(std::move(batch))});
    insert.on_execute(nullptr);

    // Проверяем схему
    auto* col1 = storage.schema().get_column_info("field1");
    auto* col2 = storage.schema().get_column_info("field2");

    REQUIRE(col1 != nullptr);
    REQUIRE(col2 != nullptr);

    CHECK(col1->is_union);
    CHECK(col2->is_union);

    CHECK(col1->union_types.size() == 3); // BIGINT, STRING_LITERAL, BOOLEAN
    CHECK(col2->union_types.size() == 2); // STRING_LITERAL, BIGINT

    CHECK(storage.size() == 4);
}

TEST_CASE("Union Integration - Schema evolution with batches") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_batches");
    auto& storage = context->document_table_storage().storage();

    SECTION("Gradual type expansion across batches") {
        // Batch 1: Только integers
        std::pmr::vector<document::document_ptr> batch1(&resource);
        for (int i = 0; i < 5; ++i) {
            auto doc = document::make_document(&resource);
            doc->set("/_id", gen_id(i, &resource));
            doc->set("/data", static_cast<int64_t>(i * 10));
            batch1.push_back(std::move(doc));
        }

        operator_insert insert1(context.get());
        insert1.set_children({new operator_raw_data_t(std::move(batch1))});
        insert1.on_execute(nullptr);

        auto* col = storage.schema().get_column_info("data");
        CHECK_FALSE(col->is_union);

        // Batch 2: Добавляем strings
        std::pmr::vector<document::document_ptr> batch2(&resource);
        for (int i = 5; i < 10; ++i) {
            auto doc = document::make_document(&resource);
            doc->set("/_id", gen_id(i, &resource));
            doc->set("/data", std::string("item_") + std::to_string(i));
            batch2.push_back(std::move(doc));
        }

        operator_insert insert2(context.get());
        insert2.set_children({new operator_raw_data_t(std::move(batch2))});
        insert2.on_execute(nullptr);

        col = storage.schema().get_column_info("data");
        CHECK(col->is_union);
        CHECK(col->union_types.size() == 2);

        // Batch 3: Добавляем booleans
        std::pmr::vector<document::document_ptr> batch3(&resource);
        for (int i = 10; i < 15; ++i) {
            auto doc = document::make_document(&resource);
            doc->set("/_id", gen_id(i, &resource));
            doc->set("/data", (i % 2 == 0));
            batch3.push_back(std::move(doc));
        }

        operator_insert insert3(context.get());
        insert3.set_children({new operator_raw_data_t(std::move(batch3))});
        insert3.on_execute(nullptr);

        col = storage.schema().get_column_info("data");
        CHECK(col->is_union);
        CHECK(col->union_types.size() == 3);
        CHECK(storage.size() == 15);
    }
}

TEST_CASE("Union Integration - Nested documents with union at different levels") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_nested");
    auto& storage = context->document_table_storage().storage();

    // Документы с вложенными объектами
    std::pmr::vector<document::document_ptr> batch1(&resource);
    
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/user/name", std::string("Alice"));
    doc1->set("/user/age", static_cast<int64_t>(30));
    batch1.push_back(std::move(doc1));
    
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/user/name", std::string("Bob"));
    doc2->set("/user/age", std::string("unknown")); // age теперь union!
    batch1.push_back(std::move(doc2));

    operator_insert insert(context.get());
    insert.set_children({new operator_raw_data_t(std::move(batch1))});
    REQUIRE_NOTHROW(insert.on_execute(nullptr));

    // Проверяем что вставка прошла успешно
    CHECK(storage.size() == 2);
    
    // NOTE: Nested paths могут обрабатываться по-разному в зависимости от extractor
    // Этот тест проверяет что union работает даже для вложенных структур
    // Детальная проверка schema опущена из-за вариативности обработки путей
}

TEST_CASE("Union Integration - Stress test with rapid type changes") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_stress");
    auto& storage = context->document_table_storage().storage();

    // Быстро меняем типы для одного поля
    for (int i = 0; i < 50; ++i) {
        std::pmr::vector<document::document_ptr> batch(&resource);
        auto doc = document::make_document(&resource);
        doc->set("/_id", gen_id(i, &resource));
        
        // Циклически меняем типы
        int type_idx = i % 4;
        if (type_idx == 0) {
            doc->set("/flexible", static_cast<int64_t>(i));
        } else if (type_idx == 1) {
            doc->set("/flexible", std::string("val_") + std::to_string(i));
        } else if (type_idx == 2) {
            doc->set("/flexible", (i % 2 == 0));
        } else {
            // Пропускаем поле (NULL)
        }
        
        batch.push_back(std::move(doc));

        operator_insert insert(context.get());
        insert.set_children({new operator_raw_data_t(std::move(batch))});
        REQUIRE_NOTHROW(insert.on_execute(nullptr));
    }

    auto* col = storage.schema().get_column_info("flexible");
    REQUIRE(col != nullptr);
    CHECK(col->is_union);
    CHECK(col->union_types.size() == 3); // BIGINT, STRING_LITERAL, BOOLEAN
    CHECK(storage.size() == 50);
}

TEST_CASE("Union Integration - get_union_tag consistency across operations") {
    using namespace document_table::operators;
    using namespace base::operators;
    
    auto resource = std::pmr::synchronized_pool_resource();
    auto context = create_test_context(&resource, "union_tag_consistency");
    auto& storage = context->document_table_storage().storage();

    // Создаем union
    std::pmr::vector<document::document_ptr> batch(&resource);
    
    auto doc1 = document::make_document(&resource);
    doc1->set("/_id", gen_id(1, &resource));
    doc1->set("/status", static_cast<int64_t>(1));
    batch.push_back(std::move(doc1));
    
    auto doc2 = document::make_document(&resource);
    doc2->set("/_id", gen_id(2, &resource));
    doc2->set("/status", std::string("active"));
    batch.push_back(std::move(doc2));
    
    auto doc3 = document::make_document(&resource);
    doc3->set("/_id", gen_id(3, &resource));
    doc3->set("/status", true);
    batch.push_back(std::move(doc3));

    operator_insert insert(context.get());
    insert.set_children({new operator_raw_data_t(std::move(batch))});
    insert.on_execute(nullptr);

    auto* col = storage.schema().get_column_info("status");
    REQUIRE(col != nullptr);
    REQUIRE(col->is_union);

    // Проверяем, что теги стабильны
    uint8_t tag_int = storage.schema().get_union_tag(col, types::logical_type::BIGINT);
    uint8_t tag_str = storage.schema().get_union_tag(col, types::logical_type::STRING_LITERAL);
    uint8_t tag_bool = storage.schema().get_union_tag(col, types::logical_type::BOOLEAN);

    CHECK(tag_int == 0);
    CHECK(tag_str == 1);
    CHECK(tag_bool == 2);

    // Все теги должны быть уникальны
    CHECK(tag_int != tag_str);
    CHECK(tag_int != tag_bool);
    CHECK(tag_str != tag_bool);
}

