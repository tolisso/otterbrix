#pragma once

#include "node.hpp"
#include "node_aggregate.hpp"  // for computing_side_t
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/expressions/update_expression.hpp>

namespace components::logical_plan {

    class node_update_t final : public node_t {
    public:
        explicit node_update_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection_to,
                               const collection_full_name_t& collection_from,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit,
                               const std::pmr::vector<expressions::update_expr_ptr>& updates,
                               bool upsert = false);

        const std::pmr::vector<expressions::update_expr_ptr>& updates() const;
        bool upsert() const;
        const collection_full_name_t& collection_from() const;

        // Stamped by dispatcher pre-flight when this UPDATE targets a sparse
        // computing table. Carries the full side-table list so the executor's
        // create_plan_update can emit operator_update_computing and drive the
        // re-alloc-and-copy lifecycle (allocate new row_id range, copy all
        // fields with SET applied, delete old rows from main + every side) under
        // one transaction id.
        bool is_computing_table() const { return is_computing_table_; }
        const std::vector<computing_side_t>& computing_sides() const { return computing_sides_; }
        void set_computing_sides(std::vector<computing_side_t> sides) {
            is_computing_table_ = true;
            computing_sides_ = std::move(sides);
        }

    private:
        collection_full_name_t collection_from_;
        std::pmr::vector<expressions::update_expr_ptr> update_expressions_;
        bool upsert_;
        bool is_computing_table_{false};
        std::vector<computing_side_t> computing_sides_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_update_ptr = boost::intrusive_ptr<node_update_t>;

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert = false);

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection_to,
                                          const collection_full_name_t& collection_from,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection_to,
                                         const collection_full_name_t& collection_from,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection_to,
                                     const collection_full_name_t& collection_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert = false);

} // namespace components::logical_plan