#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <unordered_map>

namespace services::collection {
    class context_collection_t;
}

namespace components::table::operators {

    // Columnar GROUP BY operator - works directly with columns without transpose
    // Algorithm:
    // 1. Scan GROUP BY key column(s) and assign group_id to each row
    // 2. For each aggregation, iterate over column + group_id together
    class columnar_group final : public base::operators::read_write_operator_t {
    public:
        explicit columnar_group(services::collection::context_collection_t* context);

        // Add GROUP BY key column (by name or index)
        void add_key(const std::string& name, const std::string& alias);

        // Add aggregation: type (count, sum, etc.), column name, result alias
        void add_aggregate(expressions::aggregate_type type,
                          const std::string& column_name,
                          const std::string& alias,
                          bool distinct = false);

    private:
        struct key_info_t {
            std::string column_name;
            std::string alias;
            size_t column_index = 0;
        };

        struct aggregate_info_t {
            expressions::aggregate_type type;
            std::string column_name;
            std::string alias;
            size_t column_index = 0;
            bool distinct = false;
        };

        std::pmr::vector<key_info_t> keys_;
        std::pmr::vector<aggregate_info_t> aggregates_;

        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        // Build group_id vector and collect unique keys
        // Returns: number of groups
        size_t build_group_ids(const vector::data_chunk_t& input,
                              std::vector<uint32_t>& group_ids,
                              std::vector<std::vector<types::logical_value_t>>& unique_keys);

        // Calculate aggregates for all groups
        void calculate_aggregates(const vector::data_chunk_t& input,
                                 const std::vector<uint32_t>& group_ids,
                                 size_t num_groups,
                                 vector::data_chunk_t& output,
                                 size_t key_count);
    };

} // namespace components::table::operators
