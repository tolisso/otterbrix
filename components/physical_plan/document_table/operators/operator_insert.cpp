#include "operator_insert.hpp"
#include <services/collection/collection.hpp>
#include <components/document/document.hpp>

namespace components::document_table::operators {

    operator_insert::operator_insert(services::collection::context_collection_t* context)
        : base::operators::read_write_operator_t(context, base::operators::operator_type::insert) {}

    void operator_insert::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !left_->output()) {
            return;
        }

        auto& storage = context_->document_table_storage().storage();

        // Get input documents
        const auto& input_docs = left_->output()->documents();

        modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());

        // Insert each document
        // document_table_storage will handle schema evolution automatically
        for (const auto& doc : input_docs) {
            if (!doc || !doc->is_valid()) {
                continue;
            }

            // Extract document ID from the document
            document::document_id_t doc_id = document::get_document_id(doc);

            // Insert document - storage handles schema evolution
            storage.insert(doc_id, doc);

            // Track the row_id in modified
            size_t row_id;
            if (storage.get_row_id(doc_id, row_id)) {
                modified_->append(row_id);

                // TODO: Index insertion needs proper chunk data
                // For now we skip indexing in document_table
                // if (pipeline_context) {
                //     context_->index_engine()->insert_row(chunk, row_id, pipeline_context);
                // }
            }
        }

        // Create output with the current schema
        auto column_defs = storage.schema().to_column_definitions();
        std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
        output_types.reserve(column_defs.size());
        for (const auto& col_def : column_defs) {
            output_types.push_back(col_def.type());
        }

        output_ = base::operators::make_operator_data(context_->resource(), output_types);

        // If we inserted any rows, scan them back to fill output
        if (modified_ && modified_->size() > 0) {
            // Scan all columns of the inserted rows
            std::vector<table::storage_index_t> column_indices;
            for (size_t i = 0; i < storage.table()->column_count(); ++i) {
                column_indices.emplace_back(i);
            }

            // Use modified row_ids to create a filter
            // For now, just scan everything since we don't have a row_id filter
            // TODO: Implement row_id-based filtering for better performance
            table::table_scan_state state(context_->resource());
            storage.initialize_scan(state, column_indices, nullptr);

            // Scan and get the data
            vector::data_chunk_t temp_chunk(context_->resource(), output_types);
            storage.scan(temp_chunk, state);

            // Copy only the rows we just inserted
            // Since we just inserted them, they should be the last N rows
            if (temp_chunk.size() >= modified_->size()) {
                size_t start_idx = temp_chunk.size() - modified_->size();
                output_->data_chunk().set_cardinality(modified_->size());

                for (size_t col = 0; col < temp_chunk.data.size(); ++col) {
                    for (size_t row = 0; row < modified_->size(); ++row) {
                        auto val = temp_chunk.data[col].value(start_idx + row);
                        output_->data_chunk().data[col].set_value(row, val);
                    }
                }
            }
        }
    }

} // namespace components::document_table::operators
