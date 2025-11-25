#include "operator_insert.hpp"
#include <services/collection/collection.hpp>
#include <components/document/document.hpp>
#include <iostream>
#include <cstdlib>

namespace components::document_table::operators {

    operator_insert::operator_insert(services::collection::context_collection_t* context)
        : base::operators::read_write_operator_t(context, base::operators::operator_type::insert) {}

    void operator_insert::on_execute_impl(pipeline::context_t* pipeline_context) {
        // DEBUG logging
        std::cerr << "[DEBUG operator_insert] on_execute_impl called" << std::endl;
        std::cerr << "[DEBUG operator_insert] left_=" << (left_ ? "not null" : "null") << std::endl;
        std::cerr << "[DEBUG operator_insert] left_->output()=" << (left_ && left_->output() ? "not null" : "null") << std::endl;
        std::cerr.flush();
        
        if (!left_ || !left_->output()) {
            std::cerr << "[DEBUG operator_insert] No left or output, creating empty output" << std::endl;
            std::cerr.flush();
            // Create empty output
            auto& storage = context_->document_table_storage().storage();
            auto column_defs = storage.schema().to_column_definitions();
            std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
            output_types.reserve(column_defs.size());
            for (const auto& col_def : column_defs) {
                output_types.push_back(col_def.type());
            }
            output_ = base::operators::make_operator_data(context_->resource(), output_types);
            std::cerr << "[DEBUG operator_insert] Created empty output, size=" << output_->size() << std::endl;
            std::cerr.flush();
            return;
        }

        auto& storage = context_->document_table_storage().storage();
        std::cerr << "[DEBUG operator_insert] left_->output()->uses_documents()=" << left_->output()->uses_documents() << std::endl;
        std::cerr << "[DEBUG operator_insert] left_->output()->uses_data_chunk()=" << left_->output()->uses_data_chunk() << std::endl;
        std::cerr.flush();

        // Check if output uses documents (required for document_table INSERT)
        if (!left_->output()->uses_documents()) {
            std::cerr << "[DEBUG operator_insert] Output does not use documents, creating empty output" << std::endl;
            std::cerr.flush();
            // This should not happen for document_table, but handle gracefully
            auto column_defs = storage.schema().to_column_definitions();
            std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
            output_types.reserve(column_defs.size());
            for (const auto& col_def : column_defs) {
                output_types.push_back(col_def.type());
            }
            output_ = base::operators::make_operator_data(context_->resource(), output_types);
            return;
        }

        // Get input documents
        const auto& input_docs = left_->output()->documents();
        std::cerr << "[DEBUG operator_insert] input_docs size=" << input_docs.size() << std::endl;
        std::cerr.flush();

        modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());

        // Insert each document
        // document_table_storage will handle schema evolution automatically
        size_t inserted_count = 0;
        for (const auto& doc : input_docs) {
            if (!doc || !doc->is_valid()) {
                std::cerr << "[DEBUG operator_insert] Skipping invalid document" << std::endl;
                std::cerr.flush();
                continue;
            }

            try {
                // Extract document ID from the document
                document::document_id_t doc_id = document::get_document_id(doc);
                std::cerr << "[DEBUG operator_insert] Inserting document with id=" << doc_id.to_string() << std::endl;
                std::cerr.flush();

                // Insert document - storage handles schema evolution
                storage.insert(doc_id, doc);

                // Track the row_id in modified
                size_t row_id;
                if (storage.get_row_id(doc_id, row_id)) {
                    modified_->append(row_id);
                    inserted_count++;
                    std::cerr << "[DEBUG operator_insert] Document inserted, row_id=" << row_id << std::endl;
                    std::cerr.flush();

                    // TODO: Index insertion needs proper chunk data
                    // For now we skip indexing in document_table
                    // if (pipeline_context) {
                    //     context_->index_engine()->insert_row(chunk, row_id, pipeline_context);
                    // }
                }
            } catch (const std::runtime_error& e) {
                // Re-throw to propagate error up to executor
                throw;
            }
        }

        std::cerr << "[DEBUG operator_insert] Inserted " << inserted_count << " documents, modified_->size()=" << (modified_ ? modified_->size() : 0) << std::endl;
        std::cerr.flush();
        
        // Create output with the current schema
        auto column_defs = storage.schema().to_column_definitions();
        std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
        output_types.reserve(column_defs.size());
        for (const auto& col_def : column_defs) {
            output_types.push_back(col_def.type());
        }

        // Create output with proper capacity based on number of inserted rows
        size_t output_capacity = modified_ ? modified_->size() : 0;
        std::cerr << "[DEBUG operator_insert] Creating output with capacity=" << output_capacity << ", column_count=" << column_defs.size() << std::endl;
        output_ = base::operators::make_operator_data(context_->resource(), output_types, output_capacity);
        std::cerr << "[DEBUG operator_insert] Output created, size=" << output_->size() << ", uses_data_chunk=" << output_->uses_data_chunk() << std::endl;
        std::cerr.flush();

        // If we inserted any rows, scan them back to fill output
        if (modified_ && modified_->size() > 0) {
            std::cerr << "[DEBUG operator_insert] Scanning inserted rows, modified_->size()=" << modified_->size() << std::endl;
            std::cerr.flush();
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
            std::cerr << "[DEBUG operator_insert] temp_chunk.size()=" << temp_chunk.size() << ", modified_->size()=" << modified_->size() << std::endl;
            std::cerr.flush();
            if (temp_chunk.size() >= modified_->size()) {
                size_t start_idx = temp_chunk.size() - modified_->size();
                output_->data_chunk().set_cardinality(modified_->size());
                std::cerr << "[DEBUG operator_insert] Copying rows from index " << start_idx << std::endl;
                std::cerr.flush();

                for (size_t col = 0; col < temp_chunk.data.size(); ++col) {
                    for (size_t row = 0; row < modified_->size(); ++row) {
                        auto val = temp_chunk.data[col].value(start_idx + row);
                        output_->data_chunk().data[col].set_value(row, val);
                    }
                }
                std::cerr << "[DEBUG operator_insert] Output filled, final size=" << output_->data_chunk().size() << std::endl;
            } else {
                std::cerr << "[DEBUG operator_insert] WARNING: temp_chunk.size() < modified_->size(), not copying" << std::endl;
            }
        } else {
            std::cerr << "[DEBUG operator_insert] No rows inserted, output remains empty" << std::endl;
        }
        std::cerr << "[DEBUG operator_insert] on_execute_impl completed, output size=" << (output_ ? output_->size() : 0) << std::endl;
        std::cerr.flush();
    }

} // namespace components::document_table::operators
