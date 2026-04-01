#include "operator_match.hpp"

#include "predicates/predicate.hpp"
#include <components/expressions/function_expression.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    operator_match_t::operator_match_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::match)
        , expression_(std::move(expression))
        , limit_(limit) {}

    void operator_match_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!limit_.check(0)) {
            return; // limit = 0
        }
        if (!left_ || !left_->output()) {
            return;
        }

        const auto& input_chunks = left_->output()->chunks();
        if (input_chunks.empty()) {
            return;
        }

        auto types = left_->output()->types();
        output_ = make_operator_data(left_->output()->resource(), types);

        int matched_total = 0;

        for (const auto& chunk : input_chunks) {
            if (!limit_.check(matched_total)) {
                break;
            }

            auto predicate = expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        chunk.types(),
                                                                        chunk.types(),
                                                                        &pipeline_context->parameters)
                                         : predicates::create_all_true_predicate(left_->output()->resource());

            std::pmr::vector<uint64_t> matched(left_->output()->resource());
            matched.reserve(chunk.size());
            for (size_t i = 0; i < chunk.size(); i++) {
                if (predicate->check(chunk, i)) {
                    matched.push_back(static_cast<uint64_t>(i));
                    if (!limit_.check(matched_total + static_cast<int>(matched.size()))) {
                        break;
                    }
                }
            }

            auto count = matched.size();
            if (count > 0) {
                vector::data_chunk_t out_chunk(left_->output()->resource(), chunk.types(), count);
                vector::indexing_vector_t indexing(left_->output()->resource(), matched.data());
                chunk.copy(out_chunk, indexing, static_cast<uint64_t>(count), 0);
                vector::vector_ops::copy(chunk.row_ids, out_chunk.row_ids, indexing, count, 0, 0);
                out_chunk.set_cardinality(count);
                output_->add_chunk(std::move(out_chunk));
                matched_total += static_cast<int>(count);
            }
        }
    }

} // namespace components::operators
