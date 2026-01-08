#include "operator_insert.hpp"
#include <services/collection/collection.hpp>
#include <components/document/document.hpp>

namespace components::document_table::operators {

    operator_insert::operator_insert(services::collection::context_collection_t* context)
        : base::operators::read_write_operator_t(context, base::operators::operator_type::insert) {}

    void operator_insert::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !left_->output()) {
            // Create empty output
            auto& storage = context_->document_table_storage().storage();
            auto column_defs = storage.to_column_definitions();
            std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
            output_types.reserve(column_defs.size());
            for (const auto& col_def : column_defs) {
                output_types.push_back(col_def.type());
            }
            output_ = base::operators::make_operator_data(context_->resource(), output_types);
            return;
        }

        auto& storage = context_->document_table_storage().storage();

        // Check if input uses data_chunk (from SQL INSERT) or documents (from API)
        if (left_->output()->uses_data_chunk()) {
            // Handle data_chunk input (from SQL INSERT)
            const auto& input_chunk = left_->output()->data_chunk();

            // Convert data_chunk to documents for batch_insert
            std::pmr::vector<std::pair<document::document_id_t, document::document_ptr>> documents(context_->resource());
            documents.reserve(input_chunk.size());

            // Cache types - types() returns a copy, so we must store it
            auto chunk_types = input_chunk.types();

            // Find _id column index
            int id_col_idx = -1;
            for (size_t i = 0; i < chunk_types.size(); ++i) {
                if (chunk_types[i].has_alias() && chunk_types[i].alias() == "_id") {
                    id_col_idx = static_cast<int>(i);
                    break;
                }
            }

            // Create documents from data_chunk rows
            for (size_t row = 0; row < input_chunk.size(); ++row) {
                auto doc = document::make_document(context_->resource());

                // Extract _id
                document::document_id_t doc_id;
                if (id_col_idx >= 0) {
                    auto id_val = input_chunk.data[id_col_idx].value(row);
                    if (id_val.type().type() == types::logical_type::STRING_LITERAL) {
                        doc_id = document::document_id_t(std::string(id_val.value<std::string_view>()));
                    } else {
                        doc_id = document::document_id_t(std::to_string(row));
                    }
                } else {
                    doc_id = document::document_id_t(std::to_string(row));
                }

                // Add all columns to document
                for (size_t col = 0; col < chunk_types.size(); ++col) {
                    const auto& col_type = chunk_types[col];
                    if (!col_type.has_alias()) {
                        continue; // Skip columns without alias
                    }
                    std::string path = "/" + col_type.alias();

                    auto val = input_chunk.data[col].value(row);

                    switch (val.type().type()) {
                        case types::logical_type::STRING_LITERAL:
                            doc->set(path, std::string(val.value<std::string_view>()));
                            break;
                        case types::logical_type::BIGINT:
                            doc->set(path, val.value<int64_t>());
                            break;
                        case types::logical_type::INTEGER:
                            doc->set(path, static_cast<int64_t>(val.value<int32_t>()));
                            break;
                        case types::logical_type::DOUBLE:
                            doc->set(path, val.value<double>());
                            break;
                        case types::logical_type::FLOAT:
                            doc->set(path, static_cast<double>(val.value<float>()));
                            break;
                        case types::logical_type::BOOLEAN:
                            doc->set(path, val.value<bool>());
                            break;
                        default:
                            // Skip unsupported types
                            break;
                    }
                }

                documents.emplace_back(doc_id, doc);
            }

            // Store starting row_id before batch insert
            size_t start_row_id = storage.size();

            // Execute batch insert
            storage.batch_insert(documents);

            // Calculate inserted count
            size_t end_row_id = storage.size();
            size_t inserted_count = end_row_id - start_row_id;

            // Fill modified_ with row IDs
            modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
            for (size_t i = 0; i < documents.size(); ++i) {
                size_t row_id;
                if (storage.get_row_id(documents[i].first, row_id)) {
                    modified_->append(row_id);
                }
            }

            // Create output with current schema
            auto column_defs = storage.to_column_definitions();
            std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
            output_types.reserve(column_defs.size());
            for (const auto& col_def : column_defs) {
                output_types.push_back(col_def.type());
            }

            size_t output_capacity = modified_ ? modified_->size() : 0;
            output_ = base::operators::make_operator_data(context_->resource(), output_types, output_capacity);

            // Copy inserted data to output
            if (modified_ && modified_->size() > 0) {
                std::vector<table::storage_index_t> column_indices;
                for (size_t i = 0; i < storage.table()->column_count(); ++i) {
                    column_indices.emplace_back(i);
                }

                table::table_scan_state state(context_->resource());
                storage.initialize_scan(state, column_indices, nullptr);

                vector::data_chunk_t temp_chunk(context_->resource(), output_types);
                storage.scan(temp_chunk, state);

                if (temp_chunk.size() >= modified_->size()) {
                    size_t start_idx = temp_chunk.size() - modified_->size();
                    output_->data_chunk().set_cardinality(modified_->size());

                    for (size_t col = 0; col < temp_chunk.data.size(); ++col) {
                        for (size_t r = 0; r < modified_->size(); ++r) {
                            auto val = temp_chunk.data[col].value(start_idx + r);
                            output_->data_chunk().data[col].set_value(r, val);
                        }
                    }
                }
            }
            return;
        }

        // Handle documents input (from API)
        if (!left_->output()->uses_documents()) {
            // No valid input, create empty output
            auto column_defs = storage.to_column_definitions();
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

        // Prepare batch insert data: collect all (id, doc) pairs
        std::pmr::vector<std::pair<document::document_id_t, document::document_ptr>> documents(context_->resource());
        documents.reserve(input_docs.size());

        for (const auto& doc : input_docs) {
            if (!doc || !doc->is_valid()) {
                continue;
            }

            // Extract document ID from the document
            document::document_id_t doc_id = document::get_document_id(doc);
            documents.emplace_back(doc_id, doc);
        }

        // Store the starting row_id before batch insert
        size_t start_row_id = storage.size();

        // Execute batch insert - storage handles schema evolution and batching
        storage.batch_insert(documents);

        // Calculate the ending row_id after batch insert
        size_t end_row_id = storage.size();
        size_t inserted_count = end_row_id - start_row_id;

        // Fill modified_ with the row IDs of inserted documents
        modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
        for (size_t i = 0; i < documents.size(); ++i) {
            size_t row_id;
            if (storage.get_row_id(documents[i].first, row_id)) {
                modified_->append(row_id);

                // TODO: Index insertion needs proper chunk data
                // For now we skip indexing in document_table
                // if (pipeline_context) {
                //     context_->index_engine()->insert_row(chunk, row_id, pipeline_context);
                // }
            }
        }

        // Create output with the current schema
        auto column_defs = storage.to_column_definitions();
        std::pmr::vector<types::complex_logical_type> output_types(context_->resource());
        output_types.reserve(column_defs.size());
        for (const auto& col_def : column_defs) {
            output_types.push_back(col_def.type());
        }

        // Create output with proper capacity based on number of inserted rows
        size_t output_capacity = modified_ ? modified_->size() : 0;
        output_ = base::operators::make_operator_data(context_->resource(), output_types, output_capacity);

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
