#pragma once

#include <memory>
#include <unordered_map>

#include <core/btree/btree.hpp>
#include <core/pmr.hpp>

#include <components/context/context.hpp>
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/session/session.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include "json_path_extractor.hpp"

#include <utility>

#include "forward.hpp"
#include "route.hpp"
#include "session/session.hpp"

namespace services {
    class memory_storage_t;
}

namespace services::collection {

    using document_id_t = components::document::document_id_t;
    using document_ptr = components::document::document_ptr;
    using document_storage_t = core::pmr::btree::btree_t<document_id_t, document_ptr>;
    using cursor_storage_t = std::pmr::unordered_map<session_id_t, components::cursor::cursor_t>;

    // Column info for dynamic schema tables
    struct column_info_t {
        std::string json_path;
        components::types::complex_logical_type type;
        size_t column_index = 0;
        bool is_array_element = false;
        size_t array_index = 0;
    };

    class table_storage_t {
    public:
        // Fixed schema (empty)
        explicit table_storage_t(std::pmr::memory_resource* resource)
            : resource_(resource)
            , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , table_(std::make_unique<components::table::data_table_t>(
                  resource,
                  block_manager_,
                  std::vector<components::table::column_definition_t>{}))
            , columns_(resource)
            , path_to_index_(resource) {}

        // Fixed schema (with columns)
        explicit table_storage_t(std::pmr::memory_resource* resource,
                                 std::vector<components::table::column_definition_t> columns)
            : resource_(resource)
            , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , table_(std::make_unique<components::table::data_table_t>(
                  resource,
                  block_manager_,
                  std::move(columns)))
            , columns_(resource)
            , path_to_index_(resource) {}

        // Dynamic schema (schema evolves on insert)
        explicit table_storage_t(std::pmr::memory_resource* resource, bool /*dynamic_schema*/)
            : resource_(resource)
            , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , table_(std::make_unique<components::table::data_table_t>(
                  resource,
                  block_manager_,
                  std::vector<components::table::column_definition_t>{}))
            , columns_(resource)
            , path_to_index_(resource)
            , has_dynamic_schema_(true)
            , extractor_(std::make_unique<json_path_extractor_t>(resource)) {}

        components::table::data_table_t& table() { return *table_; }
        const components::table::data_table_t& table() const { return *table_; }

        bool has_dynamic_schema() const noexcept { return has_dynamic_schema_; }

        // Dynamic schema column accessors
        bool has_column(const std::string& json_path) const;
        const column_info_t* get_column_info(const std::string& json_path) const;
        const column_info_t* get_column_by_index(size_t index) const;
        const std::pmr::vector<column_info_t>& columns() const { return columns_; }
        size_t column_count() const { return columns_.size(); }
        std::vector<components::table::column_definition_t> to_column_definitions() const;

        // Evolve schema from SQL INSERT VALUES types
        void evolve_schema_from_types(const std::pmr::vector<components::types::complex_logical_type>& types);

        // Convert documents to data_chunk with schema evolution (API insert path)
        components::vector::data_chunk_t prepare_insert(
            const std::pmr::vector<std::pair<document_id_t, document_ptr>>& documents);

        size_t size() const { return table_ ? table_->row_group()->total_rows() : 0; }

    private:
        void add_column(const std::string& json_path,
                        const components::types::complex_logical_type& type,
                        bool is_array_element = false,
                        size_t array_index = 0);

        std::pmr::vector<column_info_t> evolve_from_document(const document_ptr& doc);
        void evolve_schema(const std::pmr::vector<column_info_t>& new_columns);
        components::vector::data_chunk_t document_to_row(const document_ptr& doc);

        components::types::logical_value_t extract_value_from_document(const document_ptr& doc,
                                                                        const std::string& json_path,
                                                                        components::types::logical_type expected_type);

        components::types::logical_type detect_value_type_in_document(const document_ptr& doc,
                                                                       const std::string& json_path);

        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        components::table::storage::in_memory_block_manager_t block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;

        // Dynamic schema fields (only used when has_dynamic_schema_ = true)
        bool has_dynamic_schema_ = false;
        std::pmr::vector<column_info_t> columns_;
        std::pmr::unordered_map<std::string, size_t> path_to_index_;
        std::unique_ptr<json_path_extractor_t> extractor_;
    };

    enum class storage_type_t : uint8_t
    {
        DOCUMENT_BTREE = 0,
        TABLE_COLUMNS = 1,
    };

    namespace executor {
        class executor_t;
    }

    class context_collection_t final {
    public:
        // Constructor for DOCUMENT_BTREE (B-tree storage)
        explicit context_collection_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name,
                                      const actor_zeta::address_t& mdisk,
                                      const log_t& log)
            : resource_(resource)
            , document_storage_(resource_)
            , table_storage_(resource_)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , name_(name)
            , mdisk_(mdisk)
            , log_(log)
            , storage_type_(storage_type_t::DOCUMENT_BTREE)
            , uses_datatable_(false) {
            assert(resource != nullptr);
        }

        // Constructor for TABLE_COLUMNS (fixed schema columnar storage)
        explicit context_collection_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name,
                                      std::vector<components::table::column_definition_t> columns,
                                      const actor_zeta::address_t& mdisk,
                                      const log_t& log)
            : resource_(resource)
            , document_storage_(resource_)
            , table_storage_(resource_, std::move(columns))
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , name_(name)
            , mdisk_(mdisk)
            , log_(log)
            , storage_type_(storage_type_t::TABLE_COLUMNS)
            , uses_datatable_(true) {
            assert(resource != nullptr);
        }

        // Constructor for TABLE_COLUMNS with dynamic schema
        explicit context_collection_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name,
                                      bool dynamic_schema,
                                      const actor_zeta::address_t& mdisk,
                                      const log_t& log)
            : resource_(resource)
            , document_storage_(resource_)
            , table_storage_(resource_, dynamic_schema)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , name_(name)
            , mdisk_(mdisk)
            , log_(log)
            , storage_type_(storage_type_t::TABLE_COLUMNS)
            , uses_datatable_(true) {
            assert(resource != nullptr);
            assert(dynamic_schema);
        }

        // Storage accessors
        document_storage_t& document_storage() noexcept { return document_storage_; }
        table_storage_t& table_storage() noexcept { return table_storage_; }

        storage_type_t storage_type() const noexcept { return storage_type_; }

        components::table::data_table_t& data_table() { return table_storage_.table(); }

        bool has_dynamic_schema() const noexcept { return table_storage_.has_dynamic_schema(); }

        components::index::index_engine_ptr& index_engine() noexcept { return index_engine_; }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        log_t& log() noexcept { return log_; }

        const collection_full_name_t& name() const noexcept { return name_; }

        sessions::sessions_storage_t& sessions() noexcept { return sessions_; }

        bool drop() noexcept {
            if (dropped_) {
                return false;
            }
            dropped_ = true;
            return true;
        }

        bool dropped() const noexcept { return dropped_; }

        bool uses_datatable() const noexcept { return uses_datatable_; }

        actor_zeta::address_t disk() noexcept { return mdisk_; }

    private:
        std::pmr::memory_resource* resource_;
        document_storage_t document_storage_;
        table_storage_t table_storage_;
        components::index::index_engine_ptr index_engine_;

        collection_full_name_t name_;
        sessions::sessions_storage_t sessions_;
        actor_zeta::address_t mdisk_;
        log_t log_;

        storage_type_t storage_type_;
        bool uses_datatable_;
        bool dropped_{false};
    };

} // namespace services::collection
