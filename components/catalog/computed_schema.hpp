#pragma once

#include "schema.hpp"
#include "table_metadata.hpp"
#include "versioned_trie/versioned_trie.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>
#include <unordered_map>
#include <unordered_set>

namespace components::catalog {
    class computed_schema {
    public:
        explicit computed_schema(std::pmr::memory_resource* resource, used_format_t format = used_format_t::documents);

        // Returns true if success, throws on type conflict
        void append(std::pmr::string json, const types::complex_logical_type& type);
        // Try to append, returns error message on type conflict, empty string on success
        [[nodiscard]] std::string try_append(const std::pmr::string& json, const types::complex_logical_type& type);
        void drop(std::pmr::string json, const types::complex_logical_type& type);
        void drop_n(std::pmr::string json, const types::complex_logical_type& type, size_t n);

        [[nodiscard]] std::vector<types::complex_logical_type> find_field_versions(const std::pmr::string& name) const;
        [[nodiscard]] types::complex_logical_type latest_types_struct() const;
        [[nodiscard]] used_format_t storage_format() const;

        // Get all column definitions (name + type) in order
        [[nodiscard]] std::vector<std::pair<std::string, types::complex_logical_type>> get_column_definitions() const;

        // Check if a field exists
        [[nodiscard]] bool has_field(const std::pmr::string& name) const;

        // Get field type (returns NA type if not found)
        [[nodiscard]] types::complex_logical_type get_field_type(const std::pmr::string& name) const;

    private:
        using refcounted_entry_t = std::reference_wrapper<const versioned_value<types::complex_logical_type>>;

        bool try_use_refcout(const std::pmr::string& json,
                             const types::complex_logical_type& type,
                             bool is_append,
                             size_t n = 1);

        versioned_trie<std::pmr::string, types::complex_logical_type> fields_;
        std::pmr::unordered_map<std::pmr::string, refcounted_entry_t> existing_versions_;
        used_format_t storage_format_;
    };
} // namespace components::catalog
