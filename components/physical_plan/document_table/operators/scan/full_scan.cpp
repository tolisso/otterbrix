#include "full_scan.hpp"

#include <components/physical_plan/table/operators/transformation.hpp>
#include <components/table/column_state.hpp>
#include <services/collection/collection.hpp>
#include <chrono>
#include <iostream>

namespace components::document_table::operators {

    // Helper function to transform compare expression to table filter
    // Based on table::operators::transform_predicate
    namespace {
        std::unique_ptr<table::table_filter_t>
        transform_predicate(const expressions::compare_expression_ptr& expression,
                            const std::pmr::vector<types::complex_logical_type>& types,
                            const logical_plan::storage_parameters* parameters);

        std::unique_ptr<table::table_filter_t>
        transform_predicate(const expressions::compare_expression_ptr& expression,
                            const std::pmr::vector<types::complex_logical_type>& types,
                            const logical_plan::storage_parameters* parameters) {
            if (!expression || expression->type() == expressions::compare_type::all_true) {
                return nullptr;
            }
            switch (expression->type()) {
                case expressions::compare_type::union_and: {
                    auto filter = std::make_unique<table::conjunction_and_filter_t>();
                    filter->child_filters.reserve(expression->children().size());
                    for (const auto& child : expression->children()) {
                        filter->child_filters.emplace_back(
                            transform_predicate(reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                                types,
                                                parameters));
                    }
                    return filter;
                }
                case expressions::compare_type::union_or: {
                    auto filter = std::make_unique<table::conjunction_or_filter_t>();
                    filter->child_filters.reserve(expression->children().size());
                    for (const auto& child : expression->children()) {
                        filter->child_filters.emplace_back(
                            transform_predicate(reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                                types,
                                                parameters));
                    }
                    return filter;
                }
                case expressions::compare_type::invalid:
                    throw std::runtime_error("unsupported compare_type in expression to filter conversion");
                default: {
                    auto it = std::find_if(types.begin(), types.end(), [&](const types::complex_logical_type& type) {
                        return type.alias() == expression->primary_key().as_string();
                    });
                    assert(it != types.end());
                    return std::make_unique<table::constant_filter_t>(
                        expression->type(),
                        parameters->parameters.at(expression->value()),  // Already logical_value_t
                        it - types.begin());
                }
            }
        }
    } // anonymous namespace

    full_scan::full_scan(services::collection::context_collection_t* context,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::limit_t limit)
        : base::operators::read_only_operator_t(context, base::operators::operator_type::match)
        , expression_(expression)
        , limit_(limit) {}

    void full_scan::set_projection(std::vector<std::string> columns) {
        projection_columns_ = std::move(columns);
    }

    void full_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
        auto t_start = std::chrono::high_resolution_clock::now();

        // Get storage and schema
        auto& storage = context_->document_table_storage().storage();

        // Get column definitions from schema
        auto column_defs = storage.schema().to_column_definitions();

        // Build full types for filter (filter needs original column indices)
        std::pmr::vector<types::complex_logical_type> all_types(context_->resource());
        all_types.reserve(column_defs.size());
        for (const auto& col_def : column_defs) {
            all_types.push_back(col_def.type());
        }

        // Determine which columns to scan (for output)
        std::vector<table::storage_index_t> column_indices;
        std::pmr::vector<types::complex_logical_type> output_types(context_->resource());

        if (projection_columns_.empty()) {
            // No projection - scan all columns
            column_indices.reserve(column_defs.size());
            output_types.reserve(column_defs.size());
            for (size_t i = 0; i < column_defs.size(); ++i) {
                column_indices.emplace_back(i);
                output_types.push_back(column_defs[i].type());
            }
        } else {
            // Projection specified - scan only requested columns
            column_indices.reserve(projection_columns_.size());
            output_types.reserve(projection_columns_.size());
            for (const auto& col_name : projection_columns_) {
                // Find column index by name
                for (size_t i = 0; i < column_defs.size(); ++i) {
                    if (column_defs[i].type().alias() == col_name) {
                        column_indices.emplace_back(i);
                        output_types.push_back(column_defs[i].type());
                        break;
                    }
                }
            }
            std::cout << "[DEBUG full_scan] projection: requested " << projection_columns_.size()
                      << " columns, found " << column_indices.size() << std::endl;
        }

        auto t_types = std::chrono::high_resolution_clock::now();

        // Create output chunk with projected types
        output_ = base::operators::make_operator_data(context_->resource(), output_types);

        auto t_output = std::chrono::high_resolution_clock::now();

        // Early return if limit is 0
        int count = 0;
        if (!limit_.check(count)) {
            return; // limit = 0 - output is empty but valid
        }

        // Early return if collection is empty (no documents inserted yet)
        if (storage.size() == 0) {
            return;
        }

        // Transform expression to table filter using FULL types (for correct column indices)
        auto filter = transform_predicate(
            expression_,
            all_types,
            pipeline_context ? &pipeline_context->parameters : nullptr
        );

        auto t_filter = std::chrono::high_resolution_clock::now();

        // Initialize and execute scan
        table::table_scan_state state(context_->resource());
        storage.initialize_scan(state, column_indices, filter.get());

        auto t_init = std::chrono::high_resolution_clock::now();

        storage.scan(output_->data_chunk(), state);

        auto t_scan = std::chrono::high_resolution_clock::now();

        // Apply limit
        if (limit_.limit() >= 0) {
            output_->data_chunk().set_cardinality(
                std::min<size_t>(output_->data_chunk().size(), static_cast<size_t>(limit_.limit()))
            );
        }

        auto t_end = std::chrono::high_resolution_clock::now();

        std::cout << "[TIMING full_scan] columns=" << output_types.size()
                  << ", rows=" << output_->data_chunk().size()
                  << " | types: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_types - t_start).count() << "ms"
                  << ", output_alloc: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_output - t_types).count() << "ms"
                  << ", filter: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_filter - t_output).count() << "ms"
                  << ", init: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_init - t_filter).count() << "ms"
                  << ", scan: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_scan - t_init).count() << "ms"
                  << ", total: " << std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count() << "ms"
                  << std::endl;
    }

} // namespace components::document_table::operators
