#pragma once

#include <components/document/document.hpp>
#include <components/document/json_trie_node.hpp>
#include <components/types/types.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace services::collection {

    struct extracted_path_t {
        std::string path;
        components::types::logical_type type;
        bool is_array;
        size_t array_index;
        bool is_nullable;
    };

    class json_path_extractor_t {
    public:
        explicit json_path_extractor_t(std::pmr::memory_resource* resource);

        std::pmr::vector<extracted_path_t> extract_paths(const components::document::document_ptr& doc);
        std::pmr::vector<std::string> extract_field_names(const components::document::document_ptr& doc);

        struct config_t {
            size_t max_array_size = 100;
            bool flatten_arrays = true;
            bool use_separate_array_table = false;
            bool extract_nested_objects = true;
            size_t max_nesting_depth = 10;
        };

        config_t& config() { return config_; }
        const config_t& config() const { return config_; }

    private:
        void extract_recursive(const components::document::json::json_trie_node* node,
                               const std::string& current_path,
                               size_t depth,
                               std::pmr::vector<extracted_path_t>& result);

        void extract_field_names_recursive(const components::document::json::json_trie_node* node,
                                           const std::string& current_path,
                                           size_t depth,
                                           std::pmr::vector<std::string>& result);

        components::types::logical_type infer_type(const components::document::impl::element* elem) const;

        std::string join_path(const std::string& parent, const std::string& child) const;

        std::pmr::memory_resource* resource_;
        config_t config_;
    };

} // namespace services::collection
