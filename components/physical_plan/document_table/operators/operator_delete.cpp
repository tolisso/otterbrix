#include "operator_delete.hpp"
#include <components/physical_plan/table/operators/predicates/predicate.hpp>
#include <services/collection/collection.hpp>

namespace components::document_table::operators {

    operator_delete::operator_delete(services::collection::context_collection_t* context,
                                     expressions::compare_expression_ptr expr)
        : base::operators::read_write_operator_t(context, base::operators::operator_type::remove)
        , base_helper_t(context)
        , compare_expression_(std::move(expr)) {}

    void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
        // This implementation is almost identical to table::operators::operator_delete
        // The only difference is we use table() helper instead of context_->table_storage().table()

        // TODO: worth to create separate update_join operator or mutable_join with callback
        if (left_ && left_->output() && right_ && right_->output()) {
            modified_ = base::operators::make_operator_write_data<size_t>(base::operators::operator_t::context_->resource());
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            auto types_left = chunk_left.types();
            auto types_right = chunk_right.types();

            auto ids_capacity = vector::DEFAULT_VECTOR_CAPACITY;
            vector::vector_t ids(left_->output()->resource(), types::logical_type::BIGINT, ids_capacity);
            auto predicate = compare_expression_
                                 ? table::operators::predicates::create_predicate(compare_expression_,
                                                                                  types_left,
                                                                                  types_right,
                                                                                  &pipeline_context->parameters)
                                 : table::operators::predicates::create_all_true_predicate(base::operators::operator_t::context_->resource());

            size_t index = 0;
            for (size_t i = 0; i < chunk_left.size(); i++) {
                for (size_t j = 0; j < chunk_right.size(); j++) {
                    if (predicate->check(chunk_left, chunk_right, i, j)) {
                        ids.data<int64_t>()[index++] = i;
                        if (index >= ids_capacity) {
                            ids.resize(ids_capacity, ids_capacity * 2);
                            ids_capacity *= 2;
                        }
                    }
                }
            }

            // Use helper method to access table()
            auto state = table().initialize_delete({});
            table().delete_rows(*state, ids, index);

            for (size_t i = 0; i < index; i++) {
                size_t id = ids.data<int64_t>()[i];
                modified_->append(id);
                base::operators::operator_t::context_->index_engine()->delete_row(chunk_left, id, pipeline_context);
            }
        } else if (left_ && left_->output()) {
            modified_ = base::operators::make_operator_write_data<size_t>(base::operators::operator_t::context_->resource());
            auto& chunk = left_->output()->data_chunk();
            auto types = chunk.types();

            vector::vector_t ids(left_->output()->resource(), types::logical_type::BIGINT, chunk.size());
            auto predicate = compare_expression_
                                 ? table::operators::predicates::create_predicate(compare_expression_,
                                                                                  types,
                                                                                  types,
                                                                                  &pipeline_context->parameters)
                                 : table::operators::predicates::create_all_true_predicate(left_->output()->resource());

            size_t index = 0;
            for (size_t i = 0; i < chunk.size(); i++) {
                if (predicate->check(chunk, i)) {
                    if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                        ids.data<int64_t>()[index++] = chunk.data.front().indexing().get_index(i);
                    } else {
                        ids.data<int64_t>()[index++] = chunk.row_ids.data<int64_t>()[i];
                    }
                }
            }
            ids.resize(chunk.size(), index);

            // Use helper method to access table()
            auto state = table().initialize_delete({});
            table().delete_rows(*state, ids, index);

            for (size_t i = 0; i < index; i++) {
                size_t id = ids.data<int64_t>()[i];
                modified_->append(id);
                base::operators::operator_t::context_->index_engine()->delete_row(chunk, i, pipeline_context);
            }
        }
    }

} // namespace components::document_table::operators
