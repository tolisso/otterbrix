#pragma once

#include "node.hpp"
#include "node_aggregate.hpp"  // for computing_side_t

#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        std::pmr::vector<expressions::key_t>& key_translation();
        const std::pmr::vector<expressions::key_t>& key_translation() const;

        // Stamped by dispatcher pre-flight when this INSERT targets a sparse
        // computing table. Carries the side-table list so that executor's
        // create_plan_insert can emit operator_insert_computing instead of the
        // regular operator_insert. The vector is parallel to the chunk's columns
        // — entry i is the side for chunk.data[i] (i.e. the (field, type) of
        // that column). main collection lives on collection_full_name().
        bool is_computing_table() const { return is_computing_table_; }
        const std::vector<computing_side_t>& computing_sides() const { return computing_sides_; }
        void set_computing_sides(std::vector<computing_side_t> sides) {
            is_computing_table_ = true;
            computing_sides_ = std::move(sides);
        }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::pmr::vector<expressions::key_t> key_translation_;
        bool is_computing_table_{false};
        std::vector<computing_side_t> computing_sides_;
    };

    using node_insert_ptr = boost::intrusive_ptr<node_insert_t>;

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const components::vector::data_chunk_t& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk,
                                     std::pmr::vector<expressions::key_t>&& key_translation);

} // namespace components::logical_plan
