#include "operator_join.hpp"
#include "predicates/predicate.hpp"

#include <algorithm>
#include <components/expressions/compare_expression.hpp>
#include <components/vector/vector_operations.hpp>
#include <unordered_map>

namespace components::operators {

    namespace {
        // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
        // They must be skipped when copying — vector_ops::copy would dereference a null data_.
        bool is_placeholder(const vector::vector_t& v) noexcept {
            return v.data() == nullptr && v.auxiliary() == nullptr;
        }

        // Streams join output into a std::vector<data_chunk_t> where every chunk is
        // ≤ DEFAULT_VECTOR_CAPACITY (1024) rows. Emits rows one at a time via
        // vector_ops::copy and flushes on each full chunk.
        class join_builder {
        public:
            join_builder(std::pmr::memory_resource* resource,
                         const std::pmr::vector<types::complex_logical_type>& out_types,
                         const std::vector<size_t>& indices_left,
                         const std::vector<size_t>& indices_right,
                         chunks_vector_t& out_chunks)
                : resource_(resource)
                , out_types_(out_types)
                , indices_left_(indices_left)
                , indices_right_(indices_right)
                , out_chunks_(out_chunks)
                , cur_(resource, out_types, vector::DEFAULT_VECTOR_CAPACITY) {}

            void flush() {
                if (filled_ == 0) {
                    return;
                }
                cur_.set_cardinality(filled_);
                out_chunks_.emplace_back(std::move(cur_));
                cur_ = vector::data_chunk_t(resource_, out_types_, vector::DEFAULT_VECTOR_CAPACITY);
                filled_ = 0;
            }

            void emit_matched(const vector::data_chunk_t& L,
                              uint64_t li,
                              const vector::data_chunk_t& R,
                              uint64_t rj) {
                ensure_space();
                copy_left_row(L, li);
                copy_right_row(R, rj);
                ++filled_;
            }

            // L row with NULLs on all right-side output columns.
            void emit_left_only(const vector::data_chunk_t& L, uint64_t li) {
                ensure_space();
                copy_left_row(L, li);
                for (size_t c = 0; c < indices_right_.size(); ++c) {
                    cur_.data[indices_right_[c]].validity().set_invalid(filled_);
                }
                ++filled_;
            }

            // R row with NULLs on all left-side output columns.
            void emit_right_only(const vector::data_chunk_t& R, uint64_t rj) {
                ensure_space();
                copy_right_row(R, rj);
                for (size_t c = 0; c < indices_left_.size(); ++c) {
                    cur_.data[indices_left_[c]].validity().set_invalid(filled_);
                }
                ++filled_;
            }

        private:
            void ensure_space() {
                if (filled_ == vector::DEFAULT_VECTOR_CAPACITY) {
                    flush();
                }
            }

            void copy_left_row(const vector::data_chunk_t& L, uint64_t li) {
                for (size_t c = 0; c < L.column_count(); ++c) {
                    if (is_placeholder(L.data[c])) continue;
                    vector::vector_ops::copy(L.data[c], cur_.data[indices_left_[c]], li + 1, li, filled_);
                }
            }

            void copy_right_row(const vector::data_chunk_t& R, uint64_t rj) {
                for (size_t c = 0; c < R.column_count(); ++c) {
                    if (is_placeholder(R.data[c])) continue;
                    vector::vector_ops::copy(R.data[c], cur_.data[indices_right_[c]], rj + 1, rj, filled_);
                }
            }

            std::pmr::memory_resource* resource_;
            const std::pmr::vector<types::complex_logical_type>& out_types_;
            const std::vector<size_t>& indices_left_;
            const std::vector<size_t>& indices_right_;
            chunks_vector_t& out_chunks_;
            vector::data_chunk_t cur_;
            uint64_t filled_ = 0;
        };

        // Detect single equi-comparison `eq(key_t, key_t)` and return column indices for
        // each side. Validator marks `key.side()` and `key.path()` during JOIN validation
        // (`validate_logical_plan.cpp:260-265`); we rely on those.
        bool detect_equi_columns(const expressions::expression_ptr& expr,
                                  size_t& out_left_col,
                                  size_t& out_right_col) {
            if (!expr || expr->group() != expressions::expression_group::compare) return false;
            auto* cmp = static_cast<expressions::compare_expression_t*>(expr.get());
            if (cmp->type() != expressions::compare_type::eq) return false;
            if (!std::holds_alternative<expressions::key_t>(cmp->left())) return false;
            if (!std::holds_alternative<expressions::key_t>(cmp->right())) return false;
            const auto& lk = std::get<expressions::key_t>(cmp->left());
            const auto& rk = std::get<expressions::key_t>(cmp->right());
            if (lk.path().empty() || rk.path().empty()) return false;
            using expressions::side_t;
            if (lk.side() == side_t::left && rk.side() == side_t::right) {
                out_left_col = lk.path()[0];
                out_right_col = rk.path()[0];
                return true;
            }
            if (lk.side() == side_t::right && rk.side() == side_t::left) {
                out_left_col = rk.path()[0];
                out_right_col = lk.path()[0];
                return true;
            }
            return false;
        }

        // Build a multimap from `right_chunks[*][right_col]` value → (chunk_idx, row_idx).
        // NULL right values are skipped (they never join under SQL equi-join semantics).
        struct lv_hash {
            size_t operator()(const types::logical_value_t& v) const noexcept { return v.hash(); }
        };
        using right_index_t = std::unordered_multimap<types::logical_value_t,
                                                       std::pair<size_t, uint64_t>,
                                                       lv_hash,
                                                       std::equal_to<types::logical_value_t>>;
        right_index_t build_right_hash_index(const chunks_vector_t& right_chunks, size_t right_col) {
            right_index_t table;
            size_t total = 0;
            for (const auto& R : right_chunks) total += R.size();
            table.reserve(total);
            for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
                const auto& R = right_chunks[ci];
                if (right_col >= R.column_count()) continue;
                const auto& col = R.data[right_col];
                for (uint64_t rj = 0; rj < R.size(); ++rj) {
                    if (!col.validity().row_is_valid(rj)) continue;
                    table.emplace(col.value(rj), std::make_pair(ci, rj));
                }
            }
            return table;
        }
    } // namespace

    operator_join_t::operator_join_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     type join_type,
                                     const expressions::expression_ptr& expression)
        : read_only_operator_t(resource, log, operator_type::join)
        , join_type_(join_type)
        , expression_(expression) {}

    void operator_join_t::on_execute_impl(pipeline::context_t* context) {
        if (!left_ || !right_) {
            return;
        }
        if (!left_->output() || !right_->output()) {
            return;
        }

        auto left_out = left_->output();
        auto right_out = right_->output();
        auto& left_chunks = left_out->chunks();
        auto& right_chunks = right_out->chunks();

        // operator_data_t always holds at least one (possibly empty) chunk per side.
        assert(!left_chunks.empty());
        assert(!right_chunks.empty());

        auto res_types = left_chunks.front().types();
        auto right_types = right_chunks.front().types();
        size_t left_col_count = left_chunks.front().column_count();
        size_t right_col_count = right_chunks.front().column_count();

        // TODO: switch to PostgreSQL-style semantics (validate_logical_plan.cpp:1563).
        // Currently all joins collapse same-aliased columns into one slot (USING-like);
        // PG only does that for USING/NATURAL, JOIN ... ON keeps duplicates addressable
        // via table qualifier. This alias-dedup is a short-term fix for chained-JOIN.
        indices_left_.clear();
        indices_right_.clear();
        indices_left_.reserve(left_col_count);
        indices_right_.reserve(right_col_count);
        for (size_t i = 0; i < left_col_count; ++i) {
            indices_left_.emplace_back(i);
        }
        for (size_t i = 0; i < right_col_count; ++i) {
            const auto& alias = right_types[i].alias();
            auto dup = std::find_if(res_types.begin(), res_types.end(), [&](const auto& t) {
                return t.alias() == alias;
            });
            if (dup != res_types.end()) {
                indices_right_.emplace_back(static_cast<size_t>(std::distance(res_types.begin(), dup)));
            } else {
                indices_right_.emplace_back(res_types.size());
                res_types.push_back(right_types[i]);
            }
        }

        if (log_.is_valid()) {
            trace(log(), "operator_join::left_size(): {}", left_out->size());
            trace(log(), "operator_join::right_size(): {}", right_out->size());
        }

        auto predicate = expression_ ? predicates::create_predicate(left_out->resource(),
                                                                    context->function_registry,
                                                                    expression_,
                                                                    left_chunks.front().types(),
                                                                    right_chunks.front().types(),
                                                                    &context->parameters)
                                     : predicates::create_all_true_predicate(left_out->resource());

        auto* res_resource = left_out->resource();
        chunks_vector_t out_chunks(res_resource);

        // Equi-join detection: pick hash-table fast path when condition is a single
        // `eq(left.key, right.key)`. Otherwise fall back to nested-loop with predicate.
        // Verified correct for regular (fixed-schema) tables — `test_hash_join`.
        // For JOINs above sparse-computed-schema subquery wrappers, the user-level
        // JOIN currently doesn't execute at all (separate regression from the rewrite,
        // unrelated to the hash path).
        size_t left_col = 0;
        size_t right_col = 0;
        bool equi = detect_equi_columns(expression_, left_col, right_col);

        switch (join_type_) {
            case type::inner:
                if (equi) inner_join_hash_(left_col, right_col, res_types, out_chunks);
                else inner_join_(predicate, context, res_types, out_chunks);
                break;
            case type::full:
                if (equi) outer_full_join_hash_(left_col, right_col, res_types, out_chunks);
                else outer_full_join_(predicate, context, res_types, out_chunks);
                break;
            case type::left:
                if (equi) outer_left_join_hash_(left_col, right_col, res_types, out_chunks);
                else outer_left_join_(predicate, context, res_types, out_chunks);
                break;
            case type::right:
                if (equi) outer_right_join_hash_(left_col, right_col, res_types, out_chunks);
                else outer_right_join_(predicate, context, res_types, out_chunks);
                break;
            case type::cross:
                cross_join_(context, res_types, out_chunks);
                break;
            default:
                break;
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(res_resource, res_types, 0);
        }
        output_ = operators::make_operator_data(res_resource, std::move(out_chunks));

        if (log_.is_valid()) {
            trace(log(), "operator_join::result_size(): {}", output_->size());
        }
    }

    void operator_join_t::inner_join_(const predicates::predicate_ptr& predicate,
                                      pipeline::context_t*,
                                      const std::pmr::vector<types::complex_logical_type>& out_types,
                                      chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                for (const auto& R : right_chunks) {
                    if (R.size() == 0) {
                        continue;
                    }
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_full_join_(const predicates::predicate_ptr& predicate,
                                           pipeline::context_t*,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        // visited_right[ci_r][rj] — tracks which right rows got matched during the probe.
        std::vector<std::vector<bool>> visited_right(right_chunks.size());
        for (size_t ci_r = 0; ci_r < right_chunks.size(); ++ci_r) {
            visited_right[ci_r].assign(right_chunks[ci_r].size(), false);
        }

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool any_match = false;
                for (size_t ci_r = 0; ci_r < right_chunks.size(); ++ci_r) {
                    const auto& R = right_chunks[ci_r];
                    if (R.size() == 0) {
                        continue;
                    }
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
                            any_match = true;
                            visited_right[ci_r][rj] = true;
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
                if (!any_match) {
                    builder.emit_left_only(L, li);
                }
            }
        }

        // Emit all right rows not visited by any left row — NULL-padded on the left side.
        for (size_t ci_r = 0; ci_r < right_chunks.size(); ++ci_r) {
            const auto& R = right_chunks[ci_r];
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                if (!visited_right[ci_r][rj]) {
                    builder.emit_right_only(R, rj);
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_left_join_(const predicates::predicate_ptr& predicate,
                                           pipeline::context_t*,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool any_match = false;
                for (const auto& R : right_chunks) {
                    if (R.size() == 0) {
                        continue;
                    }
                    auto results = predicates::batch_check_1vN(predicate, L, R, li, R.size());
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        if (mask[rj]) {
                            any_match = true;
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
                if (!any_match) {
                    builder.emit_left_only(L, li);
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_right_join_(const predicates::predicate_ptr& predicate,
                                            pipeline::context_t*,
                                            const std::pmr::vector<types::complex_logical_type>& out_types,
                                            chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& R : right_chunks) {
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                bool any_match = false;
                for (const auto& L : left_chunks) {
                    if (L.size() == 0) {
                        continue;
                    }
                    auto results = predicates::batch_check_Nv1(predicate, L, R, L.size(), rj);
                    if (results.has_error()) {
                        set_error(results.error());
                        return;
                    }
                    const auto& mask = results.value();
                    for (uint64_t li = 0; li < L.size(); ++li) {
                        if (mask[li]) {
                            any_match = true;
                            builder.emit_matched(L, li, R, rj);
                        }
                    }
                }
                if (!any_match) {
                    builder.emit_right_only(R, rj);
                }
            }
        }
        builder.flush();
    }

    // ---- Hash-join fast paths (equi-join only) ----

    void operator_join_t::inner_join_hash_(size_t left_col,
                                            size_t right_col,
                                            const std::pmr::vector<types::complex_logical_type>& out_types,
                                            chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        auto table = build_right_hash_index(right_chunks, right_col);
        for (const auto& L : left_chunks) {
            if (left_col >= L.column_count()) continue;
            const auto& lcol = L.data[left_col];
            for (uint64_t li = 0; li < L.size(); ++li) {
                if (!lcol.validity().row_is_valid(li)) continue;
                auto rng = table.equal_range(lcol.value(li));
                for (auto it = rng.first; it != rng.second; ++it) {
                    auto [ci, rj] = it->second;
                    builder.emit_matched(L, li, right_chunks[ci], rj);
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_left_join_hash_(size_t left_col,
                                                 size_t right_col,
                                                 const std::pmr::vector<types::complex_logical_type>& out_types,
                                                 chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        auto table = build_right_hash_index(right_chunks, right_col);
        for (const auto& L : left_chunks) {
            if (left_col >= L.column_count()) {
                // Probe column missing — every left row gets NULL on the right side.
                for (uint64_t li = 0; li < L.size(); ++li) {
                    builder.emit_left_only(L, li);
                }
                continue;
            }
            const auto& lcol = L.data[left_col];
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool null_key = !lcol.validity().row_is_valid(li);
                bool matched = false;
                if (!null_key) {
                    auto rng = table.equal_range(lcol.value(li));
                    for (auto it = rng.first; it != rng.second; ++it) {
                        auto [ci, rj] = it->second;
                        builder.emit_matched(L, li, right_chunks[ci], rj);
                        matched = true;
                    }
                }
                if (!matched) {
                    builder.emit_left_only(L, li);
                }
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_right_join_hash_(size_t left_col,
                                                  size_t right_col,
                                                  const std::pmr::vector<types::complex_logical_type>& out_types,
                                                  chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        // For RIGHT join we need to remember which right rows got matched.
        std::vector<std::vector<bool>> visited(right_chunks.size());
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            visited[ci].assign(right_chunks[ci].size(), false);
        }

        auto table = build_right_hash_index(right_chunks, right_col);
        for (const auto& L : left_chunks) {
            if (left_col >= L.column_count()) continue;
            const auto& lcol = L.data[left_col];
            for (uint64_t li = 0; li < L.size(); ++li) {
                if (!lcol.validity().row_is_valid(li)) continue;
                auto rng = table.equal_range(lcol.value(li));
                for (auto it = rng.first; it != rng.second; ++it) {
                    auto [ci, rj] = it->second;
                    builder.emit_matched(L, li, right_chunks[ci], rj);
                    visited[ci][rj] = true;
                }
            }
        }
        // Right rows without any left match (or right rows skipped in build because of NULL).
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            const auto& R = right_chunks[ci];
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                if (!visited[ci][rj]) builder.emit_right_only(R, rj);
            }
        }
        builder.flush();
    }

    void operator_join_t::outer_full_join_hash_(size_t left_col,
                                                 size_t right_col,
                                                 const std::pmr::vector<types::complex_logical_type>& out_types,
                                                 chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        std::vector<std::vector<bool>> visited(right_chunks.size());
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            visited[ci].assign(right_chunks[ci].size(), false);
        }

        auto table = build_right_hash_index(right_chunks, right_col);
        for (const auto& L : left_chunks) {
            if (left_col >= L.column_count()) {
                for (uint64_t li = 0; li < L.size(); ++li) builder.emit_left_only(L, li);
                continue;
            }
            const auto& lcol = L.data[left_col];
            for (uint64_t li = 0; li < L.size(); ++li) {
                bool matched = false;
                if (lcol.validity().row_is_valid(li)) {
                    auto rng = table.equal_range(lcol.value(li));
                    for (auto it = rng.first; it != rng.second; ++it) {
                        auto [ci, rj] = it->second;
                        builder.emit_matched(L, li, right_chunks[ci], rj);
                        visited[ci][rj] = true;
                        matched = true;
                    }
                }
                if (!matched) builder.emit_left_only(L, li);
            }
        }
        for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
            const auto& R = right_chunks[ci];
            for (uint64_t rj = 0; rj < R.size(); ++rj) {
                if (!visited[ci][rj]) builder.emit_right_only(R, rj);
            }
        }
        builder.flush();
    }

    void operator_join_t::cross_join_(pipeline::context_t*,
                                      const std::pmr::vector<types::complex_logical_type>& out_types,
                                      chunks_vector_t& out_chunks) {
        auto& left_chunks = left_->output()->chunks();
        auto& right_chunks = right_->output()->chunks();
        auto* resource = left_->output()->resource();
        join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

        for (const auto& L : left_chunks) {
            for (uint64_t li = 0; li < L.size(); ++li) {
                for (const auto& R : right_chunks) {
                    for (uint64_t rj = 0; rj < R.size(); ++rj) {
                        builder.emit_matched(L, li, R, rj);
                    }
                }
            }
        }
        builder.flush();
    }

} // namespace components::operators
