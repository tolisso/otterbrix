#include "operator_insert.hpp"
#include <services/collection/collection.hpp>

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

            // Generate document ID if not present
            document::document_id_t doc_id;
            if (doc->is_exists("_id")) {
                // TODO: Extract existing _id from document
                // For now, generate a new ID
                doc_id = document::document_id_t();
            } else {
                doc_id = document::document_id_t();
            }

            // Insert document - storage handles schema evolution
            storage.insert(doc_id, doc);

            // Track the row_id in modified
            size_t row_id;
            if (storage.get_row_id(doc_id, row_id)) {
                modified_->append(row_id);

                // Update index if pipeline context exists
                if (pipeline_context) {
                    // TODO: Index insertion needs proper chunk data
                    // For now we skip indexing in document_table
                    // context_->index_engine()->insert_row(chunk, row_id, pipeline_context);
                }
            }
        }

        // Create output with the current schema
        auto types = storage.schema().to_column_definitions();
        std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
        output_types.reserve(types.size());
        for (const auto& col_def : types) {
            output_types.push_back(col_def.type());
        }

        output_ = base::operators::make_operator_data(context_->resource(), output_types);

        // TODO: Fill output chunk with inserted data
        // For now, output will be empty but with correct schema
    }

} // namespace components::document_table::operators
