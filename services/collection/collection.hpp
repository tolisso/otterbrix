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
#include <components/document_table/document_table_storage.hpp>

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

    class table_storage_t {
    public:
        explicit table_storage_t(std::pmr::memory_resource* resource)
            : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , table_(std::make_unique<components::table::data_table_t>(
                  resource,
                  block_manager_,
                  std::vector<components::table::column_definition_t>{})) {}

        explicit table_storage_t(std::pmr::memory_resource* resource,
                                 std::vector<components::table::column_definition_t> columns)
            : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , table_(std::make_unique<components::table::data_table_t>(resource, block_manager_, std::move(columns))) {}

        components::table::data_table_t& table() { return *table_; }

    private:
        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        components::table::storage::in_memory_block_manager_t block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;
    };

    class document_table_storage_wrapper_t {
    public:
        explicit document_table_storage_wrapper_t(std::pmr::memory_resource* resource)
            : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , storage_(std::make_unique<components::document_table::document_table_storage_t>(resource, block_manager_)) {}

        components::document_table::document_table_storage_t& storage() { return *storage_; }

    private:
        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        components::table::storage::in_memory_block_manager_t block_manager_;
        std::unique_ptr<components::document_table::document_table_storage_t> storage_;
    };

    enum class storage_type_t : uint8_t
    {
        DOCUMENT_BTREE = 0,  // document_storage_ (B-tree)
        TABLE_COLUMNS = 1,   // table_storage_ (columnar)
        DOCUMENT_TABLE = 2   // document_table_storage_ (hybrid)
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
            , document_table_storage_(resource_)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , name_(name)
            , mdisk_(mdisk)
            , log_(log)
            , storage_type_(storage_type_t::DOCUMENT_BTREE)
            , uses_datatable_(false) {
            assert(resource != nullptr);
        }

        // Constructor for TABLE_COLUMNS (columnar storage)
        explicit context_collection_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name,
                                      std::vector<components::table::column_definition_t> columns,
                                      const actor_zeta::address_t& mdisk,
                                      const log_t& log)
            : resource_(resource)
            , document_storage_(resource_)
            , table_storage_(resource_, std::move(columns))
            , document_table_storage_(resource_)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , name_(name)
            , mdisk_(mdisk)
            , log_(log)
            , storage_type_(storage_type_t::TABLE_COLUMNS)
            , uses_datatable_(true) {
            assert(resource != nullptr);
        }

        // Constructor for DOCUMENT_TABLE (hybrid storage)
        explicit context_collection_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name,
                                      storage_type_t storage_type,
                                      const actor_zeta::address_t& mdisk,
                                      const log_t& log)
            : resource_(resource)
            , document_storage_(resource_)
            , table_storage_(resource_)
            , document_table_storage_(resource_)
            , index_engine_(core::pmr::make_unique<components::index::index_engine_t>(resource_))
            , name_(name)
            , mdisk_(mdisk)
            , log_(log)
            , storage_type_(storage_type)
            , uses_datatable_(storage_type == storage_type_t::DOCUMENT_TABLE) {
            assert(resource != nullptr);
            assert(storage_type == storage_type_t::DOCUMENT_TABLE);
        }

        // Accessors for different storage types
        document_storage_t& document_storage() noexcept { return document_storage_; }
        table_storage_t& table_storage() noexcept { return table_storage_; }
        document_table_storage_wrapper_t& document_table_storage() noexcept { return document_table_storage_; }

        storage_type_t storage_type() const noexcept { return storage_type_; }

        // Unified data_table access for both TABLE_COLUMNS and DOCUMENT_TABLE
        components::table::data_table_t& data_table() {
            if (storage_type_ == storage_type_t::DOCUMENT_TABLE)
                return *document_table_storage_.storage().table();
            return table_storage_.table();
        }

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
        document_table_storage_wrapper_t document_table_storage_;
        components::index::index_engine_ptr index_engine_;

        collection_full_name_t name_;
        /**
         * @brief Index create/drop context
         */
        sessions::sessions_storage_t sessions_;
        actor_zeta::address_t mdisk_;
        log_t log_;

        storage_type_t storage_type_;
        bool uses_datatable_;
        bool dropped_{false};
    };

} // namespace services::collection