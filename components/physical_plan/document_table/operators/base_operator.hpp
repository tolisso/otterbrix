#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/document_table/document_table_storage.hpp>
#include <services/collection/collection.hpp>

namespace components::document_table::operators {

    /**
     * @brief Base helper class for document_table operators
     *
     * Provides convenient access to document_table_storage and its underlying data_table.
     * Most operations can delegate directly to the internal table.
     */
    class base_helper_t {
    public:
        explicit base_helper_t(services::collection::context_collection_t* context)
            : context_(context) {}

        // Access to document_table storage
        document_table::document_table_storage_t& storage() {
            return context_->document_table_storage().storage();
        }

        // Access to underlying data_table (for direct delegation)
        table::data_table_t& table() {
            return *storage().table();
        }

        // Get column types from dynamic schema
        std::pmr::vector<types::complex_logical_type> get_column_types() {
            auto column_defs = storage().schema().to_column_definitions();
            std::pmr::vector<types::complex_logical_type> types(context_->resource());
            types.reserve(column_defs.size());
            for (const auto& col_def : column_defs) {
                types.push_back(col_def.type());
            }
            return types;
        }

    private:
        services::collection::context_collection_t* context_;
    };

} // namespace components::document_table::operators
