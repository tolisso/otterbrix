#include "full_scan.hpp"

#include <components/physical_plan/table/operators/transformation.hpp>
#include <components/table/column_state.hpp>
#include <services/collection/collection.hpp>

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
                        return type.alias() == expression->key_left().as_string();
                    });
                    assert(it != types.end());
                    return std::make_unique<table::constant_filter_t>(
                        expression->type(),
                        parameters->parameters.at(expression->value()).as_logical_value(),
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

    void full_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Skip logging for now - test contexts don't have valid loggers
        // TODO: Add proper logging when context has valid logger
        // trace(context_->log(), "document_table::full_scan");

        // Get storage and schema
        auto& storage = context_->document_table_storage().storage();

        // Get column types from schema - convert to complex_logical_type
        auto column_defs = storage.schema().to_column_definitions();
        std::pmr::vector<types::complex_logical_type> types(context_->resource());
        types.reserve(column_defs.size());
        for (const auto& col_def : column_defs) {
            types.push_back(col_def.type());
        }

        // Create output chunk (must be created even if limit=0 or empty collection)
        output_ = base::operators::make_operator_data(context_->resource(), types);

        // Early return if limit is 0
        int count = 0;
        if (!limit_.check(count)) {
            return; // limit = 0 - output is empty but valid
        }

        // Early return if collection is empty (no documents inserted yet)
        if (storage.size() == 0) {
            return;
        }

        // Prepare column indices - scan all columns
        std::vector<table::storage_index_t> column_indices;
        column_indices.reserve(storage.table()->column_count());
        for (size_t i = 0; i < storage.table()->column_count(); ++i) {
            column_indices.emplace_back(i);
        }

        // Transform expression to table filter
        auto filter = transform_predicate(
            expression_,
            types,
            pipeline_context ? &pipeline_context->parameters : nullptr
        );

        // Initialize and execute scan
        table::table_scan_state state(context_->resource());
        storage.initialize_scan(state, column_indices, filter.get());
        storage.scan(output_->data_chunk(), state);

        // Apply limit
        if (limit_.limit() >= 0) {
            output_->data_chunk().set_cardinality(
                std::min<size_t>(output_->data_chunk().size(), static_cast<size_t>(limit_.limit()))
            );
        }
    }

} // namespace components::document_table::operators
