#pragma once

#include <components/types/types.hpp>
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

        // Physical storage column name: "__field_name__TYPEID"
        [[nodiscard]] static std::string storage_column_name(const std::string& field_name,
                                                             const types::complex_logical_type& type);

        [[nodiscard]] bool has_type(const std::pmr::string& field_name,
                                    const types::complex_logical_type& type) const;

        // Sparse column support
        void set_sparse_threshold(size_t n) { sparse_threshold_ = n; }
        [[nodiscard]] size_t sparse_threshold() const { return sparse_threshold_; }
        [[nodiscard]] bool has_sparse() const { return sparse_threshold_ > 0; }

        // Returns current insert count for a (field, type) pair.
        [[nodiscard]] size_t get_count(const std::pmr::string& field_name,
                                       const types::complex_logical_type& type) const;

        // Returns true when sparse_threshold > 0 and count < threshold.
        [[nodiscard]] bool is_sparse(const std::pmr::string& field_name,
                                     const types::complex_logical_type& type) const;

        // Reserve n row IDs for the next INSERT batch. Returns the first ID in the block.
        uint64_t alloc_rowids(size_t n);

        // Track whether a (field, type) pair has been promoted to the main physical collection.
        [[nodiscard]] bool is_in_main(const std::pmr::string& field_name,
                                      const types::complex_logical_type& type) const;
        void set_in_main(const std::pmr::string& field_name, const types::complex_logical_type& type);

    private:
        // field_name -> list of types currently present
        std::pmr::unordered_map<std::pmr::string,
                                std::pmr::vector<types::complex_logical_type>>
            fields_;

        // Preserves insertion order of (field_name, type) pairs for physical column ordering.
        std::pmr::vector<std::pair<std::pmr::string, types::complex_logical_type>> column_order_;

        // Parallel to fields_: field_name -> per-type insert row counts (same index as fields_[name]).
        std::pmr::unordered_map<std::pmr::string, std::pmr::vector<size_t>> counts_;

        // Parallel to fields_: true if the (field, type) pair has been added to the main physical collection.
        std::pmr::unordered_map<std::pmr::string, std::pmr::vector<bool>> in_main_;

        size_t sparse_threshold_ = 0;
        uint64_t next_rowid_ = 0;
    };
} // namespace components::catalog
