#pragma once

#include "node.hpp"

#include <components/catalog/table_metadata.hpp>
#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          std::pmr::vector<types::complex_logical_type> schema = {},
                                          catalog::used_format_t storage_format = catalog::used_format_t::undefined);

        static node_ptr deserialize(serializer::base_deserializer_t* deserializer);

        const std::pmr::vector<types::complex_logical_type>& schema() const;
        catalog::used_format_t storage_format() const;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::base_serializer_t* serializer) const final;

        std::pmr::vector<types::complex_logical_type> schema_;
        catalog::used_format_t storage_format_;
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema = {},
                                                           catalog::used_format_t storage_format = catalog::used_format_t::undefined);

} // namespace components::logical_plan
