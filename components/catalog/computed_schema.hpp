#pragma once

#include <components/types/types.hpp>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace components::catalog {
    class computed_schema {
    public:
        explicit computed_schema(std::pmr::memory_resource* resource);

        // Add a (field_name, type) pair. No-op if already present.
        void append(std::pmr::string field_name, const types::complex_logical_type& type);
        void append_n(std::pmr::string field_name, const types::complex_logical_type& type, size_t n);

        // Remove a (field_name, type) pair.
        void drop(std::pmr::string field_name, const types::complex_logical_type& type);
        void drop_n(std::pmr::string field_name, const types::complex_logical_type& type, size_t n);

        [[nodiscard]] std::vector<types::complex_logical_type>
        find_field_versions(const std::pmr::string& field_name) const;

        [[nodiscard]] types::complex_logical_type latest_types_struct() const;

        [[nodiscard]] bool has_type(const std::pmr::string& field_name,
                                    const types::complex_logical_type& type) const;

        // Canonical side-table name for one (field, type) pair of a computing table.
        // Pattern: "_dyn_<main_table>__<field>__<type_id>" — keeps multi-type fields
        // disambiguated by logical_type id, and prefix `_dyn_` is reserved for side tables.
        [[nodiscard]] static std::string side_table_name(std::string_view main_table,
                                                          std::string_view field,
                                                          const types::complex_logical_type& type);

    private:
        // field_name -> list of types currently present
        std::pmr::unordered_map<std::pmr::string,
                                std::pmr::vector<types::complex_logical_type>>
            fields_;

        // Preserves insertion order of (field_name, type) pairs for physical column ordering.
        std::pmr::vector<std::pair<std::pmr::string, types::complex_logical_type>> column_order_;
    };
} // namespace components::catalog
