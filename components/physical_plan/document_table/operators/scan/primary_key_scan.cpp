#include "primary_key_scan.hpp"

#include <components/table/column_state.hpp>
#include <services/collection/collection.hpp>

namespace components::document_table::operators {

    primary_key_scan::primary_key_scan(services::collection::context_collection_t* context,
                                       const expressions::compare_expression_ptr& expression)
        : base::operators::read_only_operator_t(context, base::operators::operator_type::match)
        , expression_(expression)
        , document_ids_(context->resource()) {}

    void primary_key_scan::append(const document::document_id_t& id) {
        document_ids_.push_back(id);
    }

    void primary_key_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Get storage
        auto& storage = context_->document_table_storage().storage();

        // Get column types from schema
        auto column_defs = storage.schema().to_column_definitions();
        std::pmr::vector<types::complex_logical_type> types(context_->resource());
        types.reserve(column_defs.size());
        for (const auto& col_def : column_defs) {
            types.push_back(col_def.type());
        }

        // Create output chunk
        output_ = base::operators::make_operator_data(context_->resource(), types);

        // If we have expression, extract _id value from it
        if (expression_ && pipeline_context) {
            // Get the value from parameters
            auto& params = pipeline_context->parameters;
            auto it = params.parameters.find(expression_->value());
            if (it != params.parameters.end()) {
                auto value = it->second.as_logical_value();
                
                // Convert value to document_id_t
                if (value.type().type() == types::logical_type::STRING_LITERAL) {
                    // Extract string from logical_value_t
                    std::string_view id_str = value.value<std::string_view>();
                    // Create document_id_t from string (hex format)
                    document::document_id_t doc_id(id_str);
                    if (!doc_id.is_null()) {
                        document_ids_.push_back(doc_id);
                    }
                }
            }
        }

        // Early return if no IDs to search
        if (document_ids_.empty()) {
            return;
        }

        // Convert document_id -> row_id using id_to_row_ map
        vector::vector_t row_ids(context_->resource(), types::logical_type::BIGINT);
        size_t row_count = 0;
        for (const auto& doc_id : document_ids_) {
            size_t row_id;
            // Use get_row_id method from document_table_storage
            if (storage.get_row_id(doc_id, row_id)) {
                row_ids.set_value(row_count++, types::logical_value_t(static_cast<int64_t>(row_id)));
            }
            // If document doesn't exist, just skip it (don't add to row_ids)
        }

        // Fetch rows from table by row_ids
        if (row_count > 0) {
            // Prepare column indices - fetch all columns
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(storage.table()->column_count());
            for (size_t i = 0; i < storage.table()->column_count(); ++i) {
                column_indices.emplace_back(i);
            }

            // Use table fetch to get rows directly by row_id (O(1) for each)
            table::column_fetch_state state;
            storage.table()->fetch(
                output_->data_chunk(),
                column_indices,
                row_ids,
                row_count,
                state
            );
        }
    }

} // namespace components::document_table::operators

