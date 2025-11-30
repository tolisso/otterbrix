#include <gtest/gtest.h>
#include <components/document_table/document_table_storage.hpp>
#include <components/document_table/dynamic_schema.hpp>
#include <components/document/document.hpp>
#include <components/document/impl/base_document.hpp>
#include <components/table/storage/block_manager.hpp>
#include <memory_resource>

using namespace components::document_table;
using namespace components::document;
using namespace components::types;

namespace {

    // Вспомогательная функция для создания документа из JSON строки
    document_ptr create_test_document(const std::string& json_str) {
        return impl::create_document(json_str);
    }

    // Вспомогательная функция для создания document_id
    document_id_t create_test_id(const std::string& id_str) {
        document_id_t id;
        std::memcpy(id.data(), id_str.c_str(), std::min(id_str.size(), id.size));
        return id;
    }

} // anonymous namespace

class UnionTypesTest : public ::testing::Test {
protected:
    void SetUp() override {
        resource_ = std::pmr::new_delete_resource();
        block_manager_ = std::make_unique<table::storage::block_manager_t>();
        storage_ = std::make_unique<document_table_storage_t>(resource_, *block_manager_);
    }

    void TearDown() override {
        storage_.reset();
        block_manager_.reset();
    }

    std::pmr::memory_resource* resource_;
    std::unique_ptr<table::storage::block_manager_t> block_manager_;
    std::unique_ptr<document_table_storage_t> storage_;
};

// Тест 1: Базовый тест создания union при конфликте типов
TEST_F(UnionTypesTest, BasicUnionCreation) {
    // Вставляем документ с целым числом
    auto doc1 = create_test_document(R"({"age": 30})");
    auto id1 = create_test_id("doc1");
    
    ASSERT_NO_THROW(storage_->insert(id1, doc1));

    // Проверяем, что колонка age существует и имеет тип INTEGER
    auto* col = storage_->schema().get_column_info("age");
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->type.type(), logical_type::INTEGER);
    EXPECT_FALSE(col->is_union);

    // Вставляем документ со строкой - должен создаться union!
    auto doc2 = create_test_document(R"({"age": "thirty"})");
    auto id2 = create_test_id("doc2");
    
    ASSERT_NO_THROW(storage_->insert(id2, doc2));

    // Проверяем, что колонка стала union
    col = storage_->schema().get_column_info("age");
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->type.type(), logical_type::UNION);
    EXPECT_TRUE(col->is_union);
    EXPECT_EQ(col->union_types.size(), 2);
    EXPECT_EQ(col->union_types[0], logical_type::INTEGER);
    EXPECT_EQ(col->union_types[1], logical_type::STRING_LITERAL);
}

// Тест 2: Расширение union третьим типом
TEST_F(UnionTypesTest, ExtendUnionWithThirdType) {
    // Вставляем документы с разными типами
    auto doc1 = create_test_document(R"({"value": 42})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"value": "text"})");
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    // После двух типов должен быть union
    auto* col = storage_->schema().get_column_info("value");
    ASSERT_TRUE(col->is_union);
    EXPECT_EQ(col->union_types.size(), 2);

    // Добавляем третий тип - float
    auto doc3 = create_test_document(R"({"value": 3.14})");
    auto id3 = create_test_id("doc3");
    ASSERT_NO_THROW(storage_->insert(id3, doc3));

    // Проверяем, что union расширился
    col = storage_->schema().get_column_info("value");
    ASSERT_TRUE(col->is_union);
    EXPECT_EQ(col->union_types.size(), 3);
    EXPECT_EQ(col->union_types[0], logical_type::INTEGER);
    EXPECT_EQ(col->union_types[1], logical_type::STRING_LITERAL);
    EXPECT_EQ(col->union_types[2], logical_type::DOUBLE);
}

// Тест 3: Вставка того же типа не расширяет union
TEST_F(UnionTypesTest, SameTypeDoesNotExtendUnion) {
    // Создаем union
    auto doc1 = create_test_document(R"({"field": 100})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"field": "string"})");
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    auto* col = storage_->schema().get_column_info("field");
    EXPECT_EQ(col->union_types.size(), 2);

    // Вставляем еще один документ с INTEGER (уже есть в union)
    auto doc3 = create_test_document(R"({"field": 200})");
    auto id3 = create_test_id("doc3");
    ASSERT_NO_THROW(storage_->insert(id3, doc3));

    // Union не должен расшириться
    col = storage_->schema().get_column_info("field");
    EXPECT_EQ(col->union_types.size(), 2);
}

// Тест 4: get_union_tag возвращает правильные индексы
TEST_F(UnionTypesTest, UnionTagCorrectness) {
    // Создаем union с несколькими типами
    auto doc1 = create_test_document(R"({"x": 1})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"x": "two"})");
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    auto doc3 = create_test_document(R"({"x": 3.0})");
    auto id3 = create_test_id("doc3");
    storage_->insert(id3, doc3);

    auto* col = storage_->schema().get_column_info("x");
    ASSERT_TRUE(col->is_union);

    // Проверяем tag для каждого типа
    EXPECT_EQ(storage_->schema().get_union_tag(col, logical_type::INTEGER), 0);
    EXPECT_EQ(storage_->schema().get_union_tag(col, logical_type::STRING_LITERAL), 1);
    EXPECT_EQ(storage_->schema().get_union_tag(col, logical_type::DOUBLE), 2);
}

// Тест 5: Исключение при запросе несуществующего типа в union
TEST_F(UnionTypesTest, UnionTagThrowsForNonexistentType) {
    // Создаем union с двумя типами
    auto doc1 = create_test_document(R"({"data": 123})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"data": "abc"})");
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    auto* col = storage_->schema().get_column_info("data");
    ASSERT_TRUE(col->is_union);

    // Запрашиваем tag для типа, которого нет в union
    EXPECT_THROW(storage_->schema().get_union_tag(col, logical_type::BOOLEAN), std::runtime_error);
}

// Тест 6: Множественные колонки с разными union
TEST_F(UnionTypesTest, MultipleColumnsWithDifferentUnions) {
    // Создаем документы с конфликтами в разных колонках
    auto doc1 = create_test_document(R"({"a": 1, "b": "text"})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"a": "one", "b": 2})");
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    // Проверяем, что обе колонки стали union
    auto* col_a = storage_->schema().get_column_info("a");
    auto* col_b = storage_->schema().get_column_info("b");

    ASSERT_TRUE(col_a->is_union);
    ASSERT_TRUE(col_b->is_union);

    // Проверяем типы в каждом union
    EXPECT_EQ(col_a->union_types[0], logical_type::INTEGER);
    EXPECT_EQ(col_a->union_types[1], logical_type::STRING_LITERAL);

    EXPECT_EQ(col_b->union_types[0], logical_type::STRING_LITERAL);
    EXPECT_EQ(col_b->union_types[1], logical_type::INTEGER);
}

// Тест 7: NULL значения в union колонках
TEST_F(UnionTypesTest, NullValuesInUnionColumns) {
    // Создаем union
    auto doc1 = create_test_document(R"({"field": 100})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"field": "text"})");
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    // Вставляем документ без поля field
    auto doc3 = create_test_document(R"({"other": "data"})");
    auto id3 = create_test_id("doc3");
    ASSERT_NO_THROW(storage_->insert(id3, doc3));

    // Union должен остаться таким же
    auto* col = storage_->schema().get_column_info("field");
    ASSERT_TRUE(col->is_union);
    EXPECT_EQ(col->union_types.size(), 2);
}

// Тест 8: Типы boolean и string
TEST_F(UnionTypesTest, BooleanStringUnion) {
    auto doc1 = create_test_document(R"({"flag": true})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"flag": "yes"})");
    auto id2 = create_test_id("doc2");
    ASSERT_NO_THROW(storage_->insert(id2, doc2));

    auto* col = storage_->schema().get_column_info("flag");
    ASSERT_TRUE(col->is_union);
    EXPECT_EQ(col->union_types[0], logical_type::BOOLEAN);
    EXPECT_EQ(col->union_types[1], logical_type::STRING_LITERAL);
}

// Тест 9: Numeric types (int, long, double, float)
TEST_F(UnionTypesTest, MultipleNumericTypesUnion) {
    auto doc1 = create_test_document(R"({"num": 42})");          // INTEGER
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    auto doc2 = create_test_document(R"({"num": 9223372036854775807})"); // BIGINT
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    auto doc3 = create_test_document(R"({"num": 3.14159})");     // DOUBLE
    auto id3 = create_test_id("doc3");
    storage_->insert(id3, doc3);

    auto* col = storage_->schema().get_column_info("num");
    ASSERT_TRUE(col->is_union);
    EXPECT_GE(col->union_types.size(), 3);
}

// Тест 10: Проверка размера схемы после создания union
TEST_F(UnionTypesTest, SchemaColumnCountUnchanged) {
    auto doc1 = create_test_document(R"({"field1": 1, "field2": "a"})");
    auto id1 = create_test_id("doc1");
    storage_->insert(id1, doc1);

    size_t initial_column_count = storage_->schema().column_count();

    // Создаем union - количество колонок не должно измениться
    auto doc2 = create_test_document(R"({"field1": "one", "field2": 2})");
    auto id2 = create_test_id("doc2");
    storage_->insert(id2, doc2);

    EXPECT_EQ(storage_->schema().column_count(), initial_column_count);
}

