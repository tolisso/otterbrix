#include "full_scan.hpp"

#include <components/physical_plan/table/operators/transformation.hpp>
#include <services/collection/collection.hpp>

namespace components::table::operators {

    std::unique_ptr<table::table_filter_t>
    transform_predicate(const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type> types,
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
                return std::make_unique<table::constant_filter_t>(expression->type(),
                                                                  parameters->parameters.at(expression->value()),
                                                                  it - types.begin());
            }
        }
    }

    full_scan::full_scan(services::collection::context_collection_t* context,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , expression_(expression)
        , limit_(limit) {}

    void full_scan::set_projection(std::vector<std::string> columns) {
        projection_columns_ = std::move(columns);
    }

    void full_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(), "full_scan");
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }

        auto all_types = context_->data_table().copy_types();

        // Determine which columns to scan
        std::vector<table::storage_index_t> column_indices;
        std::pmr::vector<types::complex_logical_type> output_types(context_->resource());

        if (projection_columns_.empty()) {
            // No projection - scan all columns
            column_indices.reserve(context_->data_table().column_count());
            output_types = all_types;
            for (int64_t i = 0; i < context_->data_table().column_count(); i++) {
                column_indices.emplace_back(i);
            }
        } else {
            // Projection specified - scan only requested columns
            column_indices.reserve(projection_columns_.size());
            output_types.reserve(projection_columns_.size());
            for (const auto& col_name : projection_columns_) {
                std::string alt_name = "/" + col_name;
                for (size_t i = 0; i < all_types.size(); ++i) {
                    const auto& alias = all_types[i].alias();
                    if (alias == col_name || alias == alt_name) {
                        column_indices.emplace_back(i);
                        output_types.push_back(all_types[i]);
                        break;
                    }
                }
            }
        }

        output_ = base::operators::make_operator_data(context_->resource(), output_types);
        table::table_scan_state state(context_->resource());
        auto filter =
            transform_predicate(expression_, all_types, pipeline_context ? &pipeline_context->parameters : nullptr);
        context_->data_table().initialize_scan(state, column_indices, filter.get());
        // TODO: check limit inside scan
        context_->data_table().scan(output_->data_chunk(), state);
        if (limit_.limit() >= 0) {
            output_->data_chunk().set_cardinality(std::min<size_t>(output_->data_chunk().size(), limit_.limit()));
        }
    }

} // namespace components::table::operators
