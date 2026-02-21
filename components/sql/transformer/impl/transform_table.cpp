#include "logical_plan/node_drop_type.hpp"

#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::types;

namespace components::sql::transform {
    // It is guaranteed to be a table ref, but in form of a list of strings
    enum table_name
    {
        table = 1,
        database_table = 2,
        database_schema_table = 3,
        uuid_database_schema_table = 4
    };

    logical_plan::node_ptr transformer::transform_create_table(CreateStmt& node) {
        auto coldefs = reinterpret_cast<List*>(node.tableElts);
        auto columns = get_types(resource_, *coldefs);

        // Parse storage format from WITH options
        components::catalog::used_format_t storage_format = components::catalog::used_format_t::undefined;
        if (node.options) {
            for (auto opt : node.options->lst) {
                auto def_elem = pg_ptr_assert_cast<DefElem>(opt.data, T_DefElem);
                if (std::string(def_elem->defname) == "storage") {
                    auto storage_value = strVal(def_elem->arg);
                    if (std::strcmp(storage_value, "document_table") == 0) {
                        // document_table is now stored as dynamic-schema columns table
                        storage_format = components::catalog::used_format_t::columns;
                    } else if (std::strcmp(storage_value, "documents") == 0) {
                        storage_format = components::catalog::used_format_t::documents;
                    } else if (std::strcmp(storage_value, "columns") == 0) {
                        storage_format = components::catalog::used_format_t::columns;
                    } else {
                        throw parser_exception_t{"Unknown storage format: " + std::string(storage_value), ""};
                    }
                }
            }
        }

        if (columns.empty()) {
            // For schema-less tables, default to columns (dynamic schema) if not explicitly specified
            auto effective_format = (storage_format == components::catalog::used_format_t::undefined)
                                        ? components::catalog::used_format_t::columns
                                        : storage_format;
            return logical_plan::make_node_create_collection(resource_,
                                                            rangevar_to_collection(node.relation),
                                                            std::pmr::vector<complex_logical_type>(resource_),
                                                            effective_format);
        }

        return logical_plan::make_node_create_collection(resource_,
                                                         rangevar_to_collection(node.relation),
                                                         std::move(columns),
                                                         storage_format);
    }

    logical_plan::node_ptr transformer::transform_drop(DropStmt& node) {
        switch (node.removeType) {
            case OBJECT_TABLE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        return logical_plan::make_node_drop_collection(
                            resource_,
                            {database_name_t(), strVal(drop_name.front().data)});
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource_, {database, collection});
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource_, {database, schema, collection});
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        auto uuid = strVal(it++->data);
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource_, {uuid, database, schema, collection});
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_collection(resource_, {});
                }
            }
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    throw parser_exception_t{"incorrect drop: arguments size", ""};
                }

                //when casting to enum -1 is used to account for obligated index name
                switch (static_cast<table_name>(drop_name.size() - 1)) {
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource_, {database, collection}, name);
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource_, {database, schema, collection}, name);
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        auto uuid = strVal(it++->data);
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource_,
                                                                  {uuid, database, schema, collection},
                                                                  name);
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_index(resource_, {}, "");
                }
            }
            case OBJECT_TYPE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
                return logical_plan::make_node_drop_type(resource_, strVal(drop_name.back().data));
            }
            default:
                throw std::runtime_error("Unsupported removeType");
        }
    }

} // namespace components::sql::transform
