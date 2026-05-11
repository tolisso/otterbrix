#include "expand_computing.hpp"

#include <components/catalog/computed_schema.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>

#include <set>
#include <unordered_set>

namespace components::planner {
    using namespace components::logical_plan;
    using namespace components::expressions;
    using catalog_t = components::catalog::catalog;
    using components::catalog::computed_schema;
    using components::catalog::table_id;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    namespace {

        node_ptr expand_one_aggregate(std::pmr::memory_resource* resource,
                                      node_aggregate_t* old_agg,
                                      const catalog_t& cat,
                                      core::error_t* out_error);

        // ---- Collecting referenced field names + casts ----
        struct field_ref_t {
            // True if the field is referenced WITHOUT a cast at least once
            // (e.g. `WHERE val > 0` instead of `WHERE val::bigint > 0`).
            bool any_cast = false;
            // Cast logical_type ids referenced ("val::bigint" → BIGINT here).
            std::set<types::logical_type> casts;
        };
        struct ref_collector_t {
            std::unordered_map<std::string, field_ref_t> fields;
            bool has_star = false;
        };

        void collect_keys_in_expr(const expressions::expression_ptr& expr, ref_collector_t& out);

        void collect_keys_in_param(const expressions::param_storage& p, ref_collector_t& out) {
            if (std::holds_alternative<expressions::key_t>(p)) {
                const auto& k = std::get<expressions::key_t>(p);
                if (k.storage().empty()) return;
                // Star handling: "*" or "table.*"
                const auto& last = k.storage().back();
                if (last == "*") {
                    out.has_star = true;
                    return;
                }
                std::string field_name(last);
                auto& fr = out.fields[field_name];
                if (k.has_cast_type()) {
                    fr.casts.insert(k.cast_type().type());
                } else {
                    fr.any_cast = true;
                }
            } else if (std::holds_alternative<expressions::expression_ptr>(p)) {
                collect_keys_in_expr(std::get<expressions::expression_ptr>(p), out);
            }
        }

        void collect_keys_in_expr(const expressions::expression_ptr& expr, ref_collector_t& out) {
            using expressions::expression_group;
            if (!expr) return;
            switch (expr->group()) {
                case expression_group::compare: {
                    auto* cmp = static_cast<expressions::compare_expression_t*>(expr.get());
                    collect_keys_in_param(cmp->left(), out);
                    collect_keys_in_param(cmp->right(), out);
                    for (const auto& c : cmp->children()) collect_keys_in_expr(c, out);
                    break;
                }
                case expression_group::scalar: {
                    auto* sc = static_cast<expressions::scalar_expression_t*>(expr.get());
                    // SELECT * is parsed as scalar_type::star_expand with an empty key.
                    if (sc->type() == expressions::scalar_type::star_expand) {
                        out.has_star = true;
                        break;
                    }
                    // Subtle: the SQL transformer's 4-arg `make_scalar_expression(... name, field)`
                    // (used for `SELECT field::cast` and aliased columns) stores the alias as
                    // `key()` and the actual field reference (with the cast attached) in
                    // `params()[0]`. So if params has a key_t at index 0, treat sc->key()
                    // as an alias-only and rely on the params walk below.
                    bool key_is_alias = !sc->params().empty() &&
                                         std::holds_alternative<expressions::key_t>(sc->params()[0]);
                    if (!key_is_alias && !sc->key().storage().empty()) {
                        const auto& last = sc->key().storage().back();
                        if (last == "*") {
                            out.has_star = true;
                        } else {
                            std::string fn(last);
                            auto& fr = out.fields[fn];
                            if (sc->key().has_cast_type()) {
                                fr.casts.insert(sc->key().cast_type().type());
                            } else {
                                fr.any_cast = true;
                            }
                        }
                    }
                    for (auto& p : sc->params()) collect_keys_in_param(p, out);
                    break;
                }
                case expression_group::function: {
                    auto* fn = static_cast<expressions::function_expression_t*>(expr.get());
                    for (auto& p : fn->args()) collect_keys_in_param(p, out);
                    break;
                }
                case expression_group::aggregate: {
                    auto* ag = static_cast<expressions::aggregate_expression_t*>(expr.get());
                    if (!ag->key().storage().empty()) {
                        const auto& last = ag->key().storage().back();
                        if (last == "*") {
                            out.has_star = true;
                        } else {
                            std::string fn(last);
                            auto& fr = out.fields[fn];
                            if (ag->key().has_cast_type()) {
                                fr.casts.insert(ag->key().cast_type().type());
                            } else {
                                fr.any_cast = true;
                            }
                        }
                    }
                    for (auto& p : ag->params()) collect_keys_in_param(p, out);
                    break;
                }
                case expression_group::sort: {
                    auto* so = static_cast<expressions::sort_expression_t*>(expr.get());
                    if (!so->key().storage().empty()) {
                        const auto& last = so->key().storage().back();
                        if (last == "*") {
                            out.has_star = true;
                        } else {
                            std::string fn(last);
                            auto& fr = out.fields[fn];
                            if (so->key().has_cast_type()) {
                                fr.casts.insert(so->key().cast_type().type());
                            } else {
                                fr.any_cast = true;
                            }
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }

        // Walk only the children that hold user expressions (match/select/sort/group/having/limit).
        // The future computing-subquery wrapper is excluded — it has its own internal references.
        //
        // Note on SELECT *: SQL transformer emits an EMPTY `node_select_t` for `SELECT *`
        // (the star_expand expressions are stripped during transform). An aggregate with
        // no select_t child at all (e.g. `SELECT count(*)`-only or other shapes) is also
        // treated as "all columns needed". Both cases set has_star.
        void collect_field_refs(const node_t* outer_agg, ref_collector_t& out) {
            bool has_explicit_select = false;
            for (const auto& ch : outer_agg->children()) {
                if (ch->type() == node_type::select_t) {
                    has_explicit_select = true;
                    if (ch->expressions().empty()) {
                        // Bare SELECT * — passthrough.
                        out.has_star = true;
                    }
                }
                for (const auto& expr : ch->expressions()) {
                    collect_keys_in_expr(expr, out);
                }
                // Recurse into having (lives on group_t separately).
                if (ch->type() == node_type::group_t) {
                    auto* g = reinterpret_cast<const node_group_t*>(ch.get());
                    if (g->having()) collect_keys_in_expr(g->having(), out);
                }
            }
            // No explicit select_t at all → also "all columns needed".
            if (!has_explicit_select) {
                out.has_star = true;
            }
        }

        node_ptr expand_recursive(std::pmr::memory_resource* resource,
                                  node_ptr root,
                                  const catalog_t& cat,
                                  core::error_t* out_error) {
            if (!root) {
                return root;
            }
            // Expand children first (depth-first).
            auto& children = root->children();
            for (size_t i = 0; i < children.size(); ++i) {
                children[i] = expand_recursive(resource, children[i], cat, out_error);
                if (out_error && out_error->contains_error()) return root;
            }
            if (root->type() == node_type::aggregate_t) {
                auto* agg = reinterpret_cast<node_aggregate_t*>(root.get());
                if (!agg->is_raw_computing_scan() && !agg->collection_full_name().database.empty()) {
                    table_id id(resource, agg->collection_full_name());
                    if (cat.table_computes(id)) {
                        return expand_one_aggregate(resource, agg, cat, out_error);
                    }
                }
            }
            return root;
        }

        // Build path key like `[table_name, column_name]` referencing a column.
        key_t build_qualified_key(std::pmr::memory_resource* resource,
                                  const std::string& table,
                                  const std::string& column) {
            std::pmr::vector<std::pmr::string> path(resource);
            path.push_back(std::pmr::string(table.c_str(), resource));
            path.push_back(std::pmr::string(column.c_str(), resource));
            return key_t(std::move(path));
        }

        // Builds: raw_main(coll) LEFT JOIN side_1 LEFT JOIN ... LEFT JOIN side_N
        // (anchored on raw_main, every side ON `main.row_id = side.row_id`).
        // Returns the top of the chain. `out_sides` is filled with each side's full
        // collection name, ordered as in `column_order` of the computing schema.
        node_ptr build_join_chain(std::pmr::memory_resource* resource,
                                   const catalog_t& cat,
                                   const collection_full_name_t& coll,
                                   std::vector<collection_full_name_t>& out_sides,
                                   std::vector<types::complex_logical_type>& out_field_types,
                                   bool& out_empty_schema) {
            table_id id(resource, coll);
            const auto& cs = cat.get_computing_table_schema(id);
            auto sch = cs.latest_types_struct();
            const auto& fields = sch.child_types();
            out_empty_schema = fields.empty();

            const std::string main_table_str{coll.collection};

            out_sides.clear();
            out_sides.reserve(fields.size());
            out_field_types.clear();
            out_field_types.reserve(fields.size());
            for (const auto& f : fields) {
                std::string side_name = computed_schema::side_table_name(main_table_str, f.alias(), f);
                out_sides.emplace_back(coll.database, side_name);
                out_field_types.push_back(f);
            }

            auto raw_main = make_node_aggregate(resource, coll);
            raw_main->set_raw_computing_scan(true);

            node_ptr join_root = raw_main;
            for (size_t i = 0; i < out_sides.size(); ++i) {
                auto right = make_node_aggregate(resource, out_sides[i]);
                auto join = make_node_join(resource, collection_full_name_t{}, join_type::left);
                join->append_child(join_root);
                join->append_child(right);

                key_t left_key = build_qualified_key(resource, main_table_str, "row_id");
                key_t right_key = build_qualified_key(resource, std::string{out_sides[i].collection}, "row_id");
                auto cmp = make_compare_expression(resource,
                                                   compare_type::eq,
                                                   param_storage{left_key},
                                                   param_storage{right_key});
                join->append_expression(cmp);

                join_root = join;
            }
            return join_root;
        }

        node_ptr expand_one_aggregate(std::pmr::memory_resource* resource,
                                      node_aggregate_t* old_agg,
                                      const catalog_t& cat,
                                      core::error_t* out_error) {
            auto coll = old_agg->collection_full_name();

            // Collect referenced fields + casts from user-side children.
            ref_collector_t refs;
            collect_field_refs(old_agg, refs);

            table_id id(resource, coll);
            const auto& cs = cat.get_computing_table_schema(id);
            auto sch = cs.latest_types_struct();
            const auto& all_fields = sch.child_types();

            if (all_fields.empty()) {
                // No data inserted yet — fall back to raw row_id-only scan.
                old_agg->set_raw_computing_scan(true);
                return node_ptr{old_agg};
            }

            // Decide which (field, type) pairs to actually JOIN, based on user references.
            // Also detect multi-type ambiguity: a field that has >1 type version AND is
            // referenced without a cast (or via `*`) is unresolvable.
            //
            //   has_star      → every field is needed; multi-type fields must error.
            //   any_cast=true → user wrote `field` (no cast); multi-type fields must error.
            //   casts={T1,T2} → keep only sides whose type matches a requested cast.
            //   not referenced AND no star → skip (performance pushdown).
            std::vector<size_t> selected_field_indices;
            selected_field_indices.reserve(all_fields.size());
            for (size_t i = 0; i < all_fields.size(); ++i) {
                const auto& f = all_fields[i];
                std::string field_name(f.alias());
                std::pmr::string pmr_name(field_name.c_str(), resource);
                size_t versions = cs.find_field_versions(pmr_name).size();

                if (refs.has_star) {
                    if (versions > 1) {
                        if (out_error) {
                            *out_error = core::error_t(core::error_code_t::schema_error,
                                                       std::pmr::string{
                                                           "ambiguous field '" + field_name +
                                                               "': multiple types — explicit cast required",
                                                           resource});
                        }
                        return node_ptr{old_agg};
                    }
                    selected_field_indices.push_back(i);
                    continue;
                }
                auto it = refs.fields.find(field_name);
                if (it == refs.fields.end()) {
                    continue; // not referenced — skip side (performance)
                }
                const auto& fr = it->second;
                if (fr.any_cast && versions > 1) {
                    if (out_error) {
                        *out_error = core::error_t(core::error_code_t::field_not_exists,
                                                   std::pmr::string{
                                                       "field '" + field_name +
                                                           "' has multiple types — explicit cast required",
                                                       resource});
                    }
                    return node_ptr{old_agg};
                }
                if (fr.any_cast || versions == 1 || fr.casts.count(f.type()) > 0) {
                    selected_field_indices.push_back(i);
                }
            }

            // Build sides list filtered to selected indices.
            const std::string main_table_str{coll.collection};
            std::vector<collection_full_name_t> sides;
            std::vector<types::complex_logical_type> fields;
            sides.reserve(selected_field_indices.size());
            fields.reserve(selected_field_indices.size());
            for (size_t idx : selected_field_indices) {
                const auto& f = all_fields[idx];
                std::string side_name = computed_schema::side_table_name(main_table_str, f.alias(), f);
                sides.emplace_back(coll.database, side_name);
                fields.push_back(f);
            }

            // No fields needed (e.g. `SELECT 1 FROM t` — no field refs at all). Fall back
            // to raw row_id-only scan.
            if (sides.empty()) {
                old_agg->set_raw_computing_scan(true);
                return node_ptr{old_agg};
            }

            // Build join chain over selected sides.
            auto raw_main = make_node_aggregate(resource, coll);
            raw_main->set_raw_computing_scan(true);

            node_ptr join_root = raw_main;
            for (size_t i = 0; i < sides.size(); ++i) {
                auto right = make_node_aggregate(resource, sides[i]);
                auto join = make_node_join(resource, collection_full_name_t{}, join_type::left);
                join->append_child(join_root);
                join->append_child(right);

                key_t left_key = build_qualified_key(resource, main_table_str, "row_id");
                key_t right_key = build_qualified_key(resource, std::string{sides[i].collection}, "row_id");
                auto cmp = make_compare_expression(resource,
                                                   compare_type::eq,
                                                   param_storage{left_key},
                                                   param_storage{right_key});
                join->append_expression(cmp);
                join_root = join;
            }

            // Subquery: aggregate({}) with select-projection of just the user fields,
            // result_alias = collection name so outer `t.field` refs resolve.
            auto subquery_agg = make_node_aggregate(resource, collection_full_name_t{});
            subquery_agg->append_child(join_root);

            auto sel = make_node_select(resource, collection_full_name_t{});
            for (size_t i = 0; i < fields.size(); ++i) {
                key_t k = build_qualified_key(resource,
                                              std::string{sides[i].collection},
                                              fields[i].alias());
                auto sx = make_scalar_expression(resource, scalar_type::get_field, k);
                sel->append_expression(sx);
            }
            subquery_agg->append_child(sel);
            // Insert a *barrier* limit_t::unlimit so the outer aggregate's LIMIT/OFFSET
            // doesn't propagate through `create_plan_aggregate`'s default-child branch
            // into the JOIN children's scans (which would clip main row_id reads to a
            // tiny prefix and break LIMIT/OFFSET semantics).
            subquery_agg->append_child(
                make_node_limit(resource, collection_full_name_t{}, limit_t::unlimit()));
            subquery_agg->set_result_alias(std::string{coll.collection});
            subquery_agg->set_computing_subquery_wrapper(true);

            // Outer wrapper aggregate (empty collection, purely a scope).
            auto new_outer = make_node_aggregate(resource, collection_full_name_t{});
            new_outer->set_distinct(old_agg->is_distinct());
            for (auto& child : old_agg->children()) {
                if (child->type() == node_type::match_t) {
                    // Rebind WHERE/match filter to empty collection.  Otherwise
                    // create_plan_match sees `coll = computing-table` and emits
                    // full_scan(t, filter) — the filter would push into the main
                    // table whose physical schema is just [row_id], not user fields,
                    // and assertion in row_group::get_column fires.  With empty
                    // collection the filter becomes a post-scan operator_match_t
                    // applied to the subquery output (which has the correct
                    // virtual schema).
                    auto rewritten = make_node_match(resource,
                                                     collection_full_name_t{},
                                                     child->expressions().empty()
                                                         ? expressions::expression_ptr{}
                                                         : child->expressions()[0]);
                    for (size_t i = 1; i < child->expressions().size(); ++i) {
                        rewritten->append_expression(child->expressions()[i]);
                    }
                    for (auto& sub : child->children()) {
                        rewritten->append_child(sub);
                    }
                    new_outer->append_child(rewritten);
                } else {
                    new_outer->append_child(child);
                }
            }
            new_outer->append_child(subquery_agg);
            return new_outer;
        }

    } // namespace

    node_ptr expand_computing_tables(std::pmr::memory_resource* resource,
                                     node_ptr root,
                                     const components::catalog::catalog& cat,
                                     core::error_t* out_error) {
        return expand_recursive(resource, root, cat, out_error);
    }

    namespace {
        void annotate_recursive(std::pmr::memory_resource* resource,
                                const node_ptr& node,
                                const catalog_t& cat) {
            if (!node) return;
            for (auto& ch : node->children()) {
                annotate_recursive(resource, ch, cat);
            }
            if (node->type() != node_type::aggregate_t) return;
            auto* agg = reinterpret_cast<node_aggregate_t*>(node.get());
            if (agg->is_raw_computing_scan()) return;
            const auto& full = agg->collection_full_name();
            if (full.database.empty()) return;
            table_id id(resource, full);
            if (!cat.table_computes(id)) return;

            const auto& cs = cat.get_computing_table_schema(id);
            auto sch = cs.latest_types_struct();
            const auto& fields = sch.child_types();
            std::string main_table_str{full.collection};

            std::vector<computing_side_t> sides;
            sides.reserve(fields.size());
            for (const auto& f : fields) {
                std::string side_name = computed_schema::side_table_name(main_table_str, f.alias(), f);
                sides.push_back(computing_side_t{
                    collection_full_name_t{full.database, side_name},
                    f});
            }
            agg->set_computing_sides(std::move(sides));
        }
    } // namespace

    void annotate_computing_aggregates(std::pmr::memory_resource* resource,
                                       node_ptr root,
                                       const components::catalog::catalog& cat) {
        annotate_recursive(resource, root, cat);
    }

    // ---------- Post-validate path fixup ----------

    namespace {
        using path_map = std::unordered_map<size_t, size_t>;

        // Forward decls.
        void rewrite_param_path(expressions::param_storage& p, const path_map& m);
        void rewrite_expr_path(const expressions::expression_ptr& expr, const path_map& m);

        void rewrite_key_path(expressions::key_t& k, const path_map& m) {
            if (k.path().empty()) return;
            auto it = m.find(k.path()[0]);
            if (it == m.end()) return;
            // Replace the first index; deeper-struct path elements (if any) stay intact.
            std::pmr::vector<size_t> new_path(k.path().get_allocator().resource());
            new_path.reserve(k.path().size());
            new_path.push_back(it->second);
            for (size_t i = 1; i < k.path().size(); ++i) {
                new_path.push_back(k.path()[i]);
            }
            k.set_path(std::move(new_path));
        }

        void rewrite_param_path(expressions::param_storage& p, const path_map& m) {
            if (std::holds_alternative<expressions::key_t>(p)) {
                rewrite_key_path(std::get<expressions::key_t>(p), m);
            } else if (std::holds_alternative<expressions::expression_ptr>(p)) {
                rewrite_expr_path(std::get<expressions::expression_ptr>(p), m);
            }
        }

        void rewrite_expr_path(const expressions::expression_ptr& expr, const path_map& m) {
            using expressions::expression_group;
            if (!expr) return;
            switch (expr->group()) {
                case expression_group::compare: {
                    auto* cmp = static_cast<expressions::compare_expression_t*>(expr.get());
                    rewrite_param_path(cmp->left(), m);
                    rewrite_param_path(cmp->right(), m);
                    for (const auto& c : cmp->children()) rewrite_expr_path(c, m);
                    break;
                }
                case expression_group::scalar: {
                    auto* sc = static_cast<expressions::scalar_expression_t*>(expr.get());
                    rewrite_key_path(sc->key(), m);
                    for (auto& p : sc->params()) rewrite_param_path(p, m);
                    break;
                }
                case expression_group::function: {
                    auto* fn = static_cast<expressions::function_expression_t*>(expr.get());
                    for (auto& p : fn->args()) rewrite_param_path(p, m);
                    break;
                }
                case expression_group::aggregate: {
                    auto* ag = static_cast<expressions::aggregate_expression_t*>(expr.get());
                    rewrite_key_path(ag->key(), m);
                    for (auto& p : ag->params()) rewrite_param_path(p, m);
                    break;
                }
                case expression_group::sort: {
                    auto* so = static_cast<expressions::sort_expression_t*>(expr.get());
                    rewrite_key_path(so->key(), m);
                    break;
                }
                default:
                    break;
            }
        }

        void rewrite_node_path(node_t* node, const path_map& m) {
            if (!node) return;
            for (auto& expr : node->expressions()) {
                rewrite_expr_path(expr, m);
            }
        }

        // Build path-rebasing map from the subquery wrapper's projection: each
        // expression in my_select_node has key.path()[0] = column index in the JOIN
        // schema; output position = index of that expression in select_node.
        path_map build_projection_map(const node_aggregate_t* sub) {
            path_map m;
            // Find the select_node child (we always append it as the last child after
            // join_root in expand_one_aggregate).
            for (const auto& ch : sub->children()) {
                if (ch->type() == node_type::select_t) {
                    const auto& exprs = ch->expressions();
                    for (size_t i = 0; i < exprs.size(); ++i) {
                        if (!exprs[i] || exprs[i]->group() != expressions::expression_group::scalar) {
                            continue;
                        }
                        const auto* sx =
                            static_cast<const expressions::scalar_expression_t*>(exprs[i].get());
                        if (sx->key().path().empty()) {
                            continue;
                        }
                        m.emplace(sx->key().path()[0], i);
                    }
                    break;
                }
            }
            return m;
        }

        void fixup_recursive(node_ptr node) {
            if (!node) return;
            // Recurse first so we fix up nested computing wrappers too.
            for (auto& ch : node->children()) {
                fixup_recursive(ch);
            }
            if (node->type() != node_type::aggregate_t) return;
            // Find a subquery wrapper among children. If we have one, rebase paths
            // in our other (sibling) children whose expressions reference user-fields.
            const node_aggregate_t* wrapper = nullptr;
            for (const auto& ch : node->children()) {
                if (ch->type() != node_type::aggregate_t) continue;
                const auto* a = reinterpret_cast<const node_aggregate_t*>(ch.get());
                if (a->is_computing_subquery_wrapper()) {
                    wrapper = a;
                    break;
                }
            }
            if (!wrapper) return;
            auto m = build_projection_map(wrapper);
            if (m.empty()) return;
            for (auto& ch : node->children()) {
                if (ch.get() == wrapper) continue;
                rewrite_node_path(ch.get(), m);
            }
            // Also rewrite paths attached directly to the outer aggregate's expressions
            // (group keys, etc., though normally they live in dedicated children).
            rewrite_node_path(node.get(), m);
        }
    } // namespace

    void fixup_computing_paths(node_ptr root) { fixup_recursive(root); }

    node_ptr build_select_row_ids(std::pmr::memory_resource* resource,
                                  const components::catalog::catalog& cat,
                                  const collection_full_name_t& main_full,
                                  const expressions::expression_ptr& where_expr) {
        std::vector<collection_full_name_t> sides;
        std::vector<types::complex_logical_type> fields;
        bool empty;
        node_ptr join_root = build_join_chain(resource, cat, main_full, sides, fields, empty);

        const std::string main_table_str{main_full.collection};

        // Outer aggregate({}) holding the join + select(row_id) [+ optional match].
        auto outer = make_node_aggregate(resource, collection_full_name_t{});

        // SELECT [main.row_id]
        auto sel = make_node_select(resource, collection_full_name_t{});
        key_t row_id_key = build_qualified_key(resource, main_table_str, "row_id");
        sel->append_expression(make_scalar_expression(resource, scalar_type::get_field, row_id_key));
        outer->append_child(sel);

        // Optional WHERE — collection={} so create_plan_match emits operator_match_t
        // (post-filter) over the join output rather than full_scan with pushdown.
        if (where_expr) {
            outer->append_child(make_node_match(resource, collection_full_name_t{}, where_expr));
        }

        // join_root is the data source.
        outer->append_child(join_root);

        return outer;
    }

} // namespace components::planner
