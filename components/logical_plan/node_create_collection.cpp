#include "node_create_collection.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection,
                                                       std::pmr::vector<types::complex_logical_type> schema,
                                                       catalog::used_format_t storage_format)
        : node_t(resource, node_type::create_collection_t, collection)
        , schema_(std::move(schema))
        , storage_format_(storage_format) {}

    node_ptr node_create_collection_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        return make_node_create_collection(deserializer->resource(), deserializer->deserialize_collection(1));
    }

    std::pmr::vector<types::complex_logical_type>& node_create_collection_t::schema() { return schema_; }

    const std::pmr::vector<types::complex_logical_type>& node_create_collection_t::schema() const { return schema_; }

    catalog::used_format_t node_create_collection_t::storage_format() const { return storage_format_; }

    hash_t node_create_collection_t::hash_impl() const { return 0; }

    std::string node_create_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_collection: " << database_name() << "." << collection_name();
        return stream.str();
    }

    void node_create_collection_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append_enum(serializer::serialization_type::logical_node_create_collection);
        serializer->append(collection_);
        serializer->end_array();
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema,
                                                           catalog::used_format_t storage_format) {
        return {new node_create_collection_t{resource, collection, std::move(schema), storage_format}};
    }

} // namespace components::logical_plan
