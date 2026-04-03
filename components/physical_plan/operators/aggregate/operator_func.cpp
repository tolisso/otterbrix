#include "operator_func.hpp"

#include <unordered_set>
#include <components/compute/function.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/arithmetic_eval.hpp>

namespace components::operators::aggregate {

    namespace {

        // Build a chunk containing only the columns needed for the aggregate.
        // Key columns are referenced (no copy); arithmetic results are moved in.
        vector::data_chunk_t
        build_arg_chunk(std::pmr::memory_resource* res,
                        const vector::data_chunk_t& chunk,
                        const std::pmr::vector<expressions::param_storage>& args,
                        const std::pmr::vector<types::complex_logical_type>& col_types,
                        std::vector<vector::vector_t>&& computed_vecs,
                        const logical_plan::storage_parameters& params) {
            vector::data_chunk_t c(res, col_types, chunk.size());
            c.set_cardinality(chunk.size());
            size_t computed_idx = 0;
            for (size_t i = 0; i < args.size(); i++) {
                const auto& arg = args[i];
                if (std::holds_alternative<expressions::key_t>(arg)) {
                    const auto& key = std::get<expressions::key_t>(arg);
                    c.data[i].reference(chunk.data[key.path().front()]);
                } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                    const auto& id = std::get<core::parameter_id_t>(arg);
                    c.data[i].reference(params.parameters.at(id));
                    c.data[i].flatten(vector::indexing_vector_t(res, chunk.size()), chunk.size());
                } else if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                    if (computed_idx < computed_vecs.size()) {
                        c.data[i] = std::move(computed_vecs[computed_idx++]);
                    }
                }
            }
            return c;
        }

        void apply_distinct(std::pmr::memory_resource* res,
                             vector::data_chunk_t& c,
                             const std::pmr::vector<types::complex_logical_type>& col_types) {
            if (c.size() == 0 || c.column_count() == 0) return;
            struct lv_hash {
                size_t operator()(const types::logical_value_t& v) const noexcept { return v.hash(); }
            };
            struct lv_eq {
                bool operator()(const types::logical_value_t& a,
                                const types::logical_value_t& b) const { return a == b; }
            };
            std::unordered_set<types::logical_value_t, lv_hash, lv_eq> seen;
            seen.reserve(c.size());
            std::pmr::vector<uint64_t> unique_indices(res);
            unique_indices.reserve(c.size());
            for (uint64_t row = 0; row < c.size(); row++) {
                if (seen.insert(c.data[0].value(row)).second) {
                    unique_indices.push_back(row);
                }
            }
            vector::indexing_vector_t indexing(res, unique_indices.data());
            vector::data_chunk_t unique_c(res, col_types, unique_indices.size());
            c.copy(unique_c, indexing, unique_indices.size(), 0);
            c = std::move(unique_c);
        }

    } // namespace

    operator_func_t::operator_func_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     compute::function* func,
                                     std::pmr::vector<expressions::param_storage> args,
                                     bool distinct)
        : operator_aggregate_t(resource, std::move(log))
        , args_(std::move(args))
        , func_(func)
        , distinct_(distinct) {
        assert(func);
    }

    types::logical_value_t operator_func_t::aggregate_impl(pipeline::context_t* pipeline_context) {
        auto result = types::logical_value_t(std::pmr::null_memory_resource(), types::logical_type::NA);
        if (!left_ || !left_->output()) {
            result.set_alias(func_->name());
            return result;
        }

        auto& chunks = left_->output()->chunks();
        if (chunks.empty()) {
            result.set_alias(func_->name());
            return result;
        }

        auto* res = left_->output()->resource();

        // Compute arithmetic expression vectors for one chunk.
        // Returns empty vector and calls set_error on failure.
        auto make_computed = [&](vector::data_chunk_t& chunk) -> std::vector<vector::vector_t> {
            std::vector<vector::vector_t> computed_vecs;
            for (const auto& arg : args_) {
                if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                    auto& expr = std::get<expressions::expression_ptr>(arg);
                    if (expr->group() == expressions::expression_group::scalar) {
                        auto* scalar_expr = static_cast<const expressions::scalar_expression_t*>(expr.get());
                        auto [computed, arith_error] = operators::evaluate_arithmetic(res,
                                                                                      scalar_expr->type(),
                                                                                      scalar_expr->params(),
                                                                                      chunk,
                                                                                      pipeline_context->parameters);
                        if (!arith_error.empty()) {
                            set_error(std::move(arith_error));
                            return {};
                        }
                        computed_vecs.emplace_back(std::move(computed));
                    }
                }
            }
            return computed_vecs;
        };

        // Determine output column types from the first chunk
        auto& first_chunk = chunks[0];
        std::pmr::vector<types::complex_logical_type> col_types(res);
        col_types.reserve(args_.size());
        {
            auto computed_vecs = make_computed(first_chunk);
            if (!error_message().empty()) { result.set_alias(func_->name()); return result; }
            size_t computed_idx = 0;
            for (const auto& arg : args_) {
                if (std::holds_alternative<expressions::key_t>(arg)) {
                    const auto& key = std::get<expressions::key_t>(arg);
                    assert(!key.path().empty());
                    assert(key.path().front() < first_chunk.data.size());
                    col_types.push_back(first_chunk.data[key.path().front()].type());
                } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                    const auto& id = std::get<core::parameter_id_t>(arg);
                    col_types.push_back(pipeline_context->parameters.parameters.at(id).type());
                } else if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                    if (computed_idx < computed_vecs.size()) {
                        col_types.push_back(computed_vecs[computed_idx++].type());
                    }
                }
            }
        }

        if (col_types.size() != args_.size()) {
            result.set_alias(func_->name());
            return result;
        }

        // Fast path: single chunk — reference columns directly, zero data copy
        if (chunks.size() == 1) {
            auto computed_vecs = make_computed(first_chunk);
            if (!error_message().empty()) { result.set_alias(func_->name()); return result; }
            auto c = build_arg_chunk(res, first_chunk, args_, col_types,
                                     std::move(computed_vecs), pipeline_context->parameters);
            if (distinct_) { apply_distinct(res, c, col_types); }
            auto res_val = func_->execute(c, c.size());
            if (res_val.status() == compute::compute_status::ok()) {
                result = std::get<std::pmr::vector<types::logical_value_t>>(res_val.value())[0];
            }
            result.set_alias(func_->name());
            return result;
        }

        // Multi-chunk: build combined chunk with only the needed columns (not all source columns).
        uint64_t total_rows = static_cast<uint64_t>(left_->output()->size());
        vector::data_chunk_t combined(res, col_types, total_rows > 0 ? total_rows : 1);
        combined.set_cardinality(0);

        for (auto& chunk : chunks) {
            if (chunk.size() == 0) continue;
            auto computed_vecs = make_computed(chunk);
            if (!error_message().empty()) { result.set_alias(func_->name()); return result; }
            auto mini = build_arg_chunk(res, chunk, args_, col_types,
                                        std::move(computed_vecs), pipeline_context->parameters);
            // append resolves any references and copies only the needed column data
            combined.append(mini, true);
        }

        if (distinct_) { apply_distinct(res, combined, col_types); }

        auto res_val = func_->execute(combined, combined.size());
        if (res_val.status() == compute::compute_status::ok()) {
            result = std::get<std::pmr::vector<types::logical_value_t>>(res_val.value())[0];
        }
        result.set_alias(func_->name());
        return result;
    }

    std::string operator_func_t::key_impl() const { return func_->name(); }

} // namespace components::operators::aggregate
