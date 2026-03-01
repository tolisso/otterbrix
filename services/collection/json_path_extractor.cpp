#include "json_path_extractor.hpp"
#include <components/document/container/json_array.hpp>
#include <components/document/container/json_object.hpp>
#include <stdexcept>

namespace services::collection {

    json_path_extractor_t::json_path_extractor_t(std::pmr::memory_resource* resource)
        : resource_(resource) {}

    std::pmr::vector<extracted_path_t> json_path_extractor_t::extract_paths(const components::document::document_ptr& doc) {
        std::pmr::vector<extracted_path_t> result(resource_);

        if (!doc || !doc->is_valid()) {
            return result;
        }

        const auto* root = doc->json_trie().get();
        extract_recursive(root, "", 0, result);
        return result;
    }

    std::pmr::vector<std::string> json_path_extractor_t::extract_field_names(const components::document::document_ptr& doc) {
        std::pmr::vector<std::string> result(resource_);

        if (!doc || !doc->is_valid()) {
            return result;
        }

        const auto* root = doc->json_trie().get();
        extract_field_names_recursive(root, "", 0, result);
        return result;
    }

    void json_path_extractor_t::extract_recursive(const components::document::json::json_trie_node* node,
                                                   const std::string& current_path,
                                                   size_t depth,
                                                   std::pmr::vector<extracted_path_t>& result) {
        if (!node) {
            return;
        }

        if (depth >= config_.max_nesting_depth) {
            return;
        }

        if (node->is_object()) {
            const auto* obj = node->get_object();
            for (const auto& [key_node, value_node] : *obj) {
                std::string field_name;
                if (key_node->is_mut()) {
                    const auto* elem = key_node->get_mut();
                    if (elem->is_string()) {
                        auto str_result = elem->get_string();
                        if (!str_result.error()) {
                            field_name = std::string(str_result.value());
                        }
                    }
                }

                if (field_name.empty()) {
                    continue;
                }

                std::string field_path = join_path(current_path, field_name);
                const auto* child = value_node.get();

                if (child->is_object() || child->is_array()) {
                    if (config_.extract_nested_objects) {
                        extract_recursive(child, field_path, depth + 1, result);
                    }
                } else if (child->is_mut()) {
                    const auto* elem = child->get_mut();
                    result.push_back(extracted_path_t{
                        .path = field_path,
                        .type = infer_type(elem),
                        .is_array = false,
                        .array_index = 0,
                        .is_nullable = true});
                }
            }

        } else if (node->is_array()) {
            const auto* arr = node->get_array();

            if (config_.use_separate_array_table) {
                // skip
            } else if (config_.flatten_arrays) {
                if (arr->size() > config_.max_array_size) {
                    throw std::runtime_error(
                        "Array size exceeded limit for path '" + current_path + "': " +
                        "array has " + std::to_string(arr->size()) + " elements, " +
                        "but max_array_size is " + std::to_string(config_.max_array_size));
                }

                size_t max_index = arr->size();
                for (size_t i = 0; i < max_index; ++i) {
                    const auto* elem_node = arr->get(i);
                    if (!elem_node) {
                        continue;
                    }

                    std::string array_path = current_path + "_arr" + std::to_string(i) + "_";

                    if (elem_node->is_object() || elem_node->is_array()) {
                        extract_recursive(elem_node, array_path, depth + 1, result);
                    } else if (elem_node->is_mut()) {
                        const auto* elem = elem_node->get_mut();
                        result.push_back(extracted_path_t{
                            .path = array_path,
                            .type = infer_type(elem),
                            .is_array = true,
                            .array_index = i,
                            .is_nullable = true});
                    }
                }
            } else {
                result.push_back(extracted_path_t{
                    .path = current_path,
                    .type = components::types::logical_type::STRING_LITERAL,
                    .is_array = true,
                    .array_index = 0,
                    .is_nullable = true});
            }

        } else if (node->is_mut()) {
            const auto* elem = node->get_mut();
            result.push_back(extracted_path_t{
                .path = current_path.empty() ? "$root" : current_path,
                .type = infer_type(elem),
                .is_array = false,
                .array_index = 0,
                .is_nullable = true});
        }
    }

    components::types::logical_type json_path_extractor_t::infer_type(const components::document::impl::element* elem) const {
        if (!elem || elem->is_null()) {
            return components::types::logical_type::STRING_LITERAL;
        }
        if (elem->is_bool()) return components::types::logical_type::BOOLEAN;
        if (elem->is_int64()) return components::types::logical_type::BIGINT;
        if (elem->is_uint64()) return components::types::logical_type::UBIGINT;
        if (elem->is_int32()) return components::types::logical_type::INTEGER;
        if (elem->is_double()) return components::types::logical_type::DOUBLE;
        if (elem->is_float()) return components::types::logical_type::FLOAT;
        if (elem->is_string()) return components::types::logical_type::STRING_LITERAL;
        return components::types::logical_type::STRING_LITERAL;
    }

    std::string json_path_extractor_t::join_path(const std::string& parent, const std::string& child) const {
        if (parent.empty()) {
            return child;
        }
        return parent + "_dot_" + child;
    }

    void json_path_extractor_t::extract_field_names_recursive(const components::document::json::json_trie_node* node,
                                                               const std::string& current_path,
                                                               size_t depth,
                                                               std::pmr::vector<std::string>& result) {
        if (!node) {
            return;
        }

        if (depth >= config_.max_nesting_depth) {
            return;
        }

        if (node->is_object()) {
            const auto* obj = node->get_object();
            for (const auto& [key_node, value_node] : *obj) {
                std::string field_name;
                if (key_node->is_mut()) {
                    const auto* elem = key_node->get_mut();
                    if (elem->is_string()) {
                        auto str_result = elem->get_string();
                        if (!str_result.error()) {
                            field_name = std::string(str_result.value());
                        }
                    }
                }

                if (field_name.empty()) {
                    continue;
                }

                std::string field_path = join_path(current_path, field_name);
                const auto* child = value_node.get();

                if (child->is_object() || child->is_array()) {
                    if (config_.extract_nested_objects) {
                        extract_field_names_recursive(child, field_path, depth + 1, result);
                    }
                } else if (child->is_mut()) {
                    result.push_back(field_path);
                }
            }

        } else if (node->is_array()) {
            if (!config_.flatten_arrays) {
                return;
            }

            const auto* arr = node->get_array();
            size_t array_size = std::min(arr->size(), config_.max_array_size);

            for (size_t i = 0; i < array_size; ++i) {
                const auto* elem = arr->get(i);
                std::string array_path = current_path + "_arr" + std::to_string(i);

                if (elem->is_object() || elem->is_array()) {
                    if (config_.extract_nested_objects) {
                        extract_field_names_recursive(elem, array_path, depth + 1, result);
                    }
                } else if (elem->is_mut()) {
                    result.push_back(array_path);
                }
            }
        }
    }

} // namespace services::collection
