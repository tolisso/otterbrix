#pragma once

#include "node.hpp"
#include "node_aggregate.hpp"  // for computing_side_t
#include "node_limit.hpp"
#include "node_match.hpp"

namespace components::logical_plan {

    class node_delete_t final : public node_t {
    public:
        explicit node_delete_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection_to,
                               const collection_full_name_t& collection_from,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit);

        const collection_full_name_t& collection_from() const;

        // Stamped by dispatcher pre-flight when this DELETE targets a sparse
        // computing table. Carries the full side-table list (every (field, type)
        // ever inserted into this table) so the executor's create_plan_delete
        // can emit operator_delete_computing and drive multi-collection deletes
        // under one txn_id.
        bool is_computing_table() const { return is_computing_table_; }
        const std::vector<computing_side_t>& computing_sides() const { return computing_sides_; }
        void set_computing_sides(std::vector<computing_side_t> sides) {
            is_computing_table_ = true;
            computing_sides_ = std::move(sides);
        }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        collection_full_name_t collection_from_;
        bool is_computing_table_{false};
        std::vector<computing_side_t> computing_sides_;
    };

    using node_delete_ptr = boost::intrusive_ptr<node_delete_t>;

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match);

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection_to,
                                          const collection_full_name_t& collection_from,
                                          const node_match_ptr& match);

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match);

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection_to,
                                         const collection_full_name_t& collection_from,
                                         const node_match_ptr& match);

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit);

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection_to,
                                     const collection_full_name_t& collection_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit);

} // namespace components::logical_plan