#include "insert_jsons.hpp"

#include <components/document/document.hpp>
#include <components/document/json_trie_node.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>

#include "table_state.hpp"

#include <map>
#include <set>
#include <stdexcept>
#include <string>

namespace components::table {

namespace {

using document_ptr = document::document_t::ptr;
using json_node_t = document::json::json_trie_node;

// ---------------------------------------------------------------------------
// Type-family helpers
// ---------------------------------------------------------------------------

// All JSON scalars fall into one of four "families".
// NONE means the value was null (no type information).
enum class type_family
{
    NONE,   // null / absent
    BOOL,   // physical BOOL
    INT,    // physical INT8..INT64, UINT8..UINT64
    FLOAT,  // physical FLOAT, DOUBLE
    STRING, // physical STRING
};

type_family to_family(types::physical_type pt) {
    switch (pt) {
        case types::physical_type::BOOL:
            return type_family::BOOL;
        case types::physical_type::INT8:
        case types::physical_type::INT16:
        case types::physical_type::INT32:
        case types::physical_type::INT64:
        case types::physical_type::UINT8:
        case types::physical_type::UINT16:
        case types::physical_type::UINT32:
        case types::physical_type::UINT64:
            return type_family::INT;
        case types::physical_type::FLOAT:
        case types::physical_type::DOUBLE:
            return type_family::FLOAT;
        case types::physical_type::STRING:
            return type_family::STRING;
        default:
            return type_family::NONE; // NA / null
    }
}

// Map a family to the column logical type that will be used.
types::logical_type family_to_logical(type_family f) {
    switch (f) {
        case type_family::BOOL:
            return types::logical_type::BOOLEAN;
        case type_family::INT:
            return types::logical_type::BIGINT;
        case type_family::FLOAT:
            return types::logical_type::DOUBLE;
        case type_family::STRING:
            return types::logical_type::STRING_LITERAL;
        default:
            return types::logical_type::STRING_LITERAL; // fallback for all-null
    }
}

const char* family_name(type_family f) {
    switch (f) {
        case type_family::BOOL:
            return "bool";
        case type_family::INT:
            return "int";
        case type_family::FLOAT:
            return "float";
        case type_family::STRING:
            return "string";
        default:
            return "null";
    }
}

// ---------------------------------------------------------------------------
// Path info: json-pointer + inferred family
// ---------------------------------------------------------------------------
struct path_info {
    std::string json_ptr;   // e.g. "/commit/collection"
    type_family family{type_family::NONE};
};

// ---------------------------------------------------------------------------
// Recursive trie traversal
// ---------------------------------------------------------------------------

// Walk the trie, collecting leaf paths and their type families.
// Throws std::runtime_error if the same path has conflicting types across docs.
void collect_paths_recursive(const json_node_t* node,
                             const std::string& json_ptr,
                             const std::string& col_name,
                             std::map<std::string, path_info>& paths) {
    if (node == nullptr || node->is_deleter()) {
        return;
    }

    if (node->is_object()) {
        const auto* obj = node->get_object();
        for (const auto& kv : *obj) {
            const auto& key_node = kv.first;
            const auto& val_node = kv.second;
            if (key_node && key_node->is_mut()) {
                auto key_res = key_node->get_mut()->get_string();
                if (!key_res.error()) {
                    std::string key(key_res.value());
                    collect_paths_recursive(val_node.get(),
                                            json_ptr + "/" + key,
                                            col_name.empty() ? key : col_name + "." + key,
                                            paths);
                }
            }
        }
    } else if (node->is_array()) {
        const auto* arr = node->get_array();
        std::size_t idx = 0;
        for (auto it = arr->begin(); it != arr->end(); ++it, ++idx) {
            if (*it) {
                std::string si = std::to_string(idx);
                collect_paths_recursive(it->get(),
                                        json_ptr + "/" + si,
                                        col_name + "[" + si + "]",
                                        paths);
            }
        }
    } else if (node->is_mut()) {
        const auto* elem = node->get_mut();
        type_family fam = to_family(elem->physical_type());

        auto it = paths.find(col_name);
        if (it == paths.end()) {
            paths[col_name] = path_info{json_ptr, fam};
        } else {
            // Merge: NONE is neutral (null values carry no type info).
            if (fam != type_family::NONE) {
                if (it->second.family == type_family::NONE) {
                    it->second.family = fam; // first non-null value for this path
                } else if (it->second.family != fam) {
                    throw std::runtime_error(
                        "insert_jsons: type conflict for path \"" + col_name +
                        "\": found both " + family_name(it->second.family) +
                        " and " + family_name(fam));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Value extractor
// ---------------------------------------------------------------------------

// Return the value at json_ptr from doc as a logical_value_t of the given family.
// Returns null logical_value_t for absent/null values.
types::logical_value_t value_at_path(const document_ptr& doc,
                                     const std::string& json_ptr,
                                     type_family expected_family) {
    auto val = doc->get_value(json_ptr);
    if (!static_cast<bool>(val)) {
        return types::logical_value_t{};
    }
    if (val.logical_type() == types::logical_type::NA) {
        return types::logical_value_t{};
    }

    type_family actual = to_family(val.physical_type());
    if (actual == type_family::NONE) {
        return types::logical_value_t{};
    }
    // Families must agree (they were validated during path collection).
    if (actual != expected_family) {
        return types::logical_value_t{};
    }

    switch (expected_family) {
        case type_family::BOOL:
            return types::logical_value_t{val.as_bool()};

        case type_family::INT:
            // JSON integers are INT64 or UINT64; cast everything to int64.
            if (val.physical_type() == types::physical_type::UINT64 ||
                val.physical_type() == types::physical_type::UINT32 ||
                val.physical_type() == types::physical_type::UINT16 ||
                val.physical_type() == types::physical_type::UINT8) {
                return types::logical_value_t{static_cast<int64_t>(val.as_unsigned())};
            }
            return types::logical_value_t{val.as_int()};

        case type_family::FLOAT:
            if (val.physical_type() == types::physical_type::FLOAT) {
                return types::logical_value_t{static_cast<double>(val.as_float())};
            }
            return types::logical_value_t{val.as_double()};

        case type_family::STRING:
            return types::logical_value_t{std::string(val.as_string())};

        default:
            return types::logical_value_t{};
    }
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

std::unique_ptr<data_table_t> insert_jsons(std::unique_ptr<data_table_t> table,
                                           std::pmr::memory_resource* resource,
                                           const std::vector<std::string>& jsons) {
    if (jsons.empty()) {
        return table;
    }

    // -------------------------------------------------------------------
    // Step 1: parse all JSON strings
    // -------------------------------------------------------------------
    std::vector<document_ptr> docs;
    docs.reserve(jsons.size());
    for (const auto& json_str : jsons) {
        try {
            auto doc = document::document_t::document_from_json(json_str, resource);
            docs.push_back((doc && doc->is_valid()) ? doc : nullptr);
        } catch (...) {
            docs.push_back(nullptr);
        }
    }

    // -------------------------------------------------------------------
    // Step 2: collect all leaf paths + infer types (throws on conflict)
    // -------------------------------------------------------------------
    std::map<std::string, path_info> all_paths; // col_name -> {json_ptr, family}
    for (const auto& doc : docs) {
        if (!doc) {
            continue;
        }
        auto trie = doc->json_trie();
        if (trie) {
            collect_paths_recursive(trie.get(), "", "", all_paths);
        }
    }

    // -------------------------------------------------------------------
    // Step 3: validate against / extend existing columns
    // -------------------------------------------------------------------
    // Build a name->type map of existing columns.
    std::map<std::string, types::logical_type> existing_cols;
    for (const auto& col : table->columns()) {
        existing_cols[col.name()] = col.type().type();
    }

    for (const auto& kv : all_paths) {
        const std::string& col_name = kv.first;
        const path_info& pi = kv.second;
        types::logical_type inferred = family_to_logical(pi.family);

        auto it = existing_cols.find(col_name);
        if (it != existing_cols.end()) {
            // Column already exists — check type compatibility.
            if (it->second != inferred) {
                throw std::runtime_error(
                    "insert_jsons: type mismatch for existing column \"" + col_name +
                    "\": column has type " +
                    std::to_string(static_cast<int>(it->second)) +
                    ", but JSON data implies type " +
                    std::to_string(static_cast<int>(inferred)));
            }
        } else {
            // New column — add it.
            column_definition_t new_col{col_name, inferred, std::make_unique<types::logical_value_t>()};
            auto extended = std::make_unique<data_table_t>(*table, new_col);
            table = std::move(extended);
            existing_cols[col_name] = inferred;
        }
    }

    // -------------------------------------------------------------------
    // Step 4: build col_name -> column_index lookup for the final table
    // -------------------------------------------------------------------
    std::map<std::string, std::size_t> col_index;
    {
        const auto& cols = table->columns();
        for (std::size_t i = 0; i < cols.size(); ++i) {
            col_index[cols[i].name()] = i;
        }
    }

    // Columns that were pre-existing and are NOT part of the new paths.
    std::vector<std::size_t> extra_col_indices;
    {
        const auto& cols = table->columns();
        for (std::size_t i = 0; i < cols.size(); ++i) {
            if (all_paths.find(cols[i].name()) == all_paths.end()) {
                extra_col_indices.push_back(i);
            }
        }
    }

    // -------------------------------------------------------------------
    // Step 5: append rows in batches of DEFAULT_VECTOR_CAPACITY
    // -------------------------------------------------------------------
    const std::size_t batch_cap = vector::DEFAULT_VECTOR_CAPACITY;
    const std::size_t num_rows = docs.size();
    auto types = table->copy_types();

    vector::data_chunk_t chunk(resource, types, static_cast<uint64_t>(batch_cap));

    table_append_state state(resource);
    table->append_lock(state);
    table->initialize_append(state);

    for (std::size_t batch_start = 0; batch_start < num_rows; batch_start += batch_cap) {
        std::size_t batch_end = std::min(batch_start + batch_cap, num_rows);
        std::size_t cur_batch = batch_end - batch_start;

        chunk.reset();
        chunk.set_cardinality(static_cast<uint64_t>(cur_batch));

        for (std::size_t ri = 0; ri < cur_batch; ++ri) {
            const auto& doc = docs[batch_start + ri];

            // Always null-out pre-existing columns (they have no JSON path).
            for (std::size_t ci : extra_col_indices) {
                chunk.set_value(static_cast<uint64_t>(ci),
                                static_cast<uint64_t>(ri),
                                types::logical_value_t{});
            }

            if (!doc) {
                // Null out all JSON-path columns for invalid/missing documents.
                for (const auto& kv : all_paths) {
                    chunk.set_value(static_cast<uint64_t>(col_index.at(kv.first)),
                                    static_cast<uint64_t>(ri),
                                    types::logical_value_t{});
                }
                continue;
            }

            for (const auto& kv : all_paths) {
                const std::string& col_name = kv.first;
                const path_info& pi = kv.second;
                chunk.set_value(static_cast<uint64_t>(col_index.at(col_name)),
                                static_cast<uint64_t>(ri),
                                value_at_path(doc, pi.json_ptr, pi.family));
            }
        }

        table->append(chunk, state);
    }

    table->finalize_append(state);

    return table;
}

} // namespace components::table
