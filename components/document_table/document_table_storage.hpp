#pragma once

#include "dynamic_schema.hpp"
#include <functional>
#include <components/document/document.hpp>
#include <components/document/document_id.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <memory>
#include <memory_resource>
#include <unordered_map>

namespace components::document_table {

    // Хэш-функция для document_id_t
    struct document_id_hash_t {
        std::size_t operator()(const document::document_id_t& id) const {
            // Используем hash для string_view на основе данных
            return std::hash<std::string_view>{}(std::string_view(reinterpret_cast<const char*>(id.data()), id.size));
        }
    };

    class document_table_storage_t {
    public:
        explicit document_table_storage_t(std::pmr::memory_resource* resource,
                                          table::storage::block_manager_t& block_manager);

        // Вставка документа с автоматической эволюцией схемы
        void insert(const document::document_id_t& id, const document::document_ptr& doc);

        // Получение документа по ID
        document::document_ptr get(const document::document_id_t& id);

        // Удаление документа
        void remove(const document::document_id_t& id);

        // Проверка существования
        bool contains(const document::document_id_t& id) const;

        // Сканирование таблицы
        void scan(vector::data_chunk_t& output, table::table_scan_state& state);

        // Инициализация сканирования
        void initialize_scan(table::table_scan_state& state,
                             const std::vector<table::storage_index_t>& column_ids,
                             const table::table_filter_t* filter = nullptr);

        // Доступ к схеме
        dynamic_schema_t& schema() { return *schema_; }
        const dynamic_schema_t& schema() const { return *schema_; }

        // Доступ к таблице
        table::data_table_t* table() { return table_.get(); }
        const table::data_table_t* table() const { return table_.get(); }

        // Количество документов
        size_t size() const { return id_to_row_.size(); }

        // Получение row_id по document_id
        bool get_row_id(const document::document_id_t& id, size_t& row_id) const;

    private:
        // Проверка, нужна ли эволюция схемы
        bool needs_evolution(const document::document_ptr& doc) const;

        // Эволюция схемы и пересоздание таблицы
        void evolve_schema(const std::pmr::vector<column_info_t>& new_columns);

        // Миграция данных при расширении схемы
        void migrate_data(table::data_table_t* old_table,
                          table::data_table_t* new_table,
                          const std::pmr::vector<column_info_t>& new_columns);

        // Конвертация документа в row согласно текущей схеме
        vector::data_chunk_t document_to_row(const document::document_ptr& doc);

        // Конвертация row обратно в документ
        document::document_ptr row_to_document(const vector::data_chunk_t& row, size_t row_idx);

        // Вспомогательные методы для работы с union типами
        types::logical_value_t extract_value_from_document(const document::document_ptr& doc,
                                                           const std::string& json_path,
                                                           types::logical_type expected_type);
        
        types::logical_type detect_value_type_in_document(const document::document_ptr& doc,
                                                          const std::string& json_path);

        std::pmr::memory_resource* resource_;
        table::storage::block_manager_t& block_manager_;

        // Динамическая схема
        std::unique_ptr<dynamic_schema_t> schema_;

        // Текущая таблица
        std::unique_ptr<table::data_table_t> table_;

        // Маппинг document_id -> row_id
        std::pmr::unordered_map<document::document_id_t,
                                size_t,
                                document_id_hash_t,
                                std::equal_to<document::document_id_t>>
            id_to_row_;

        // Следующий row_id
        size_t next_row_id_;
    };

} // namespace components::document_table
