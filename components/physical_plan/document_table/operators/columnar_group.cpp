#include "columnar_group.hpp"

#include <chrono>
#include <iostream>
#include <unordered_set>
#include <services/collection/collection.hpp>
#include <sstream>

namespace components::document_table::operators {

    namespace {
        // Helper: convert logical_value_t to string for hashing
        std::string value_to_string(const types::logical_value_t& val) {
            if (val.is_null()) return "\x00NULL\x00";

            switch (val.type().type()) {
                case types::logical_type::STRING_LITERAL:
                    return val.value<const std::string&>();
                case types::logical_type::BIGINT:
                    return std::to_string(val.value<int64_t>());
                case types::logical_type::INTEGER:
                    return std::to_string(val.value<int32_t>());
                case types::logical_type::DOUBLE:
                    return std::to_string(val.value<double>());
                case types::logical_type::FLOAT:
                    return std::to_string(val.value<float>());
                case types::logical_type::BOOLEAN:
                    return val.value<bool>() ? "true" : "false";
                default: {
                    // Try to cast to string
                    std::stringstream ss;
                    ss << "type_" << static_cast<int>(val.type().type());
                    return ss.str();
                }
            }
        }

        // Helper: convert logical_value_t to double for aggregations
        double value_to_double(const types::logical_value_t& val) {
            if (val.is_null()) return 0.0;

            switch (val.type().type()) {
                case types::logical_type::DOUBLE:
                    return val.value<double>();
                case types::logical_type::FLOAT:
                    return static_cast<double>(val.value<float>());
                case types::logical_type::BIGINT:
                    return static_cast<double>(val.value<int64_t>());
                case types::logical_type::INTEGER:
                    return static_cast<double>(val.value<int32_t>());
                case types::logical_type::SMALLINT:
                    return static_cast<double>(val.value<int16_t>());
                case types::logical_type::TINYINT:
                    return static_cast<double>(val.value<int8_t>());
                case types::logical_type::UBIGINT:
                    return static_cast<double>(val.value<uint64_t>());
                case types::logical_type::UINTEGER:
                    return static_cast<double>(val.value<uint32_t>());
                default:
                    return 0.0;
            }
        }
    } // anonymous namespace

    columnar_group::columnar_group(services::collection::context_collection_t* context)
        : base::operators::read_write_operator_t(context, base::operators::operator_type::aggregate)
        , keys_(context->resource())
        , aggregates_(context->resource()) {}

    void columnar_group::add_key(const std::string& name, const std::string& alias) {
        keys_.push_back({name, alias, 0});
    }

    void columnar_group::add_aggregate(expressions::aggregate_type type,
                                       const std::string& column_name,
                                       const std::string& alias,
                                       bool distinct) {
        aggregates_.push_back({type, column_name, alias, 0, distinct});
    }

    void columnar_group::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !left_->output()) {
            return;
        }

        auto t0 = std::chrono::high_resolution_clock::now();

        auto& input = left_->output()->data_chunk();
        const auto& types = input.types();

        // Find column indices for keys
        for (auto& key : keys_) {
            for (size_t i = 0; i < types.size(); i++) {
                const auto& alias = types[i].alias();
                if (alias == key.column_name || alias == "/" + key.column_name) {
                    key.column_index = i;
                    break;
                }
            }
        }

        // Find column indices for aggregates
        for (auto& agg : aggregates_) {
            if (agg.column_name.empty() || agg.column_name == "*") {
                agg.column_index = SIZE_MAX; // Special marker for COUNT(*)
            } else {
                for (size_t i = 0; i < types.size(); i++) {
                    const auto& alias = types[i].alias();
                    if (alias == agg.column_name || alias == "/" + agg.column_name) {
                        agg.column_index = i;
                        break;
                    }
                }
            }
        }

        auto t1 = std::chrono::high_resolution_clock::now();

        // Build group_ids
        std::vector<uint32_t> group_ids;
        std::vector<std::vector<types::logical_value_t>> unique_keys;
        size_t num_groups = build_group_ids(input, group_ids, unique_keys);

        auto t2 = std::chrono::high_resolution_clock::now();

        // Build output types
        std::pmr::vector<types::complex_logical_type> output_types(context_->resource());

        // Add key columns
        for (const auto& key : keys_) {
            auto type = types[key.column_index];
            type.set_alias(key.alias);
            output_types.push_back(type);
        }

        // Add aggregate result columns
        for (const auto& agg : aggregates_) {
            types::complex_logical_type agg_type;
            switch (agg.type) {
                case expressions::aggregate_type::count:
                    agg_type = types::complex_logical_type(types::logical_type::BIGINT);
                    break;
                case expressions::aggregate_type::sum:
                case expressions::aggregate_type::avg:
                    agg_type = types::complex_logical_type(types::logical_type::DOUBLE);
                    break;
                case expressions::aggregate_type::min:
                case expressions::aggregate_type::max:
                    if (agg.column_index < types.size()) {
                        agg_type = types[agg.column_index];
                    } else {
                        agg_type = types::complex_logical_type(types::logical_type::DOUBLE);
                    }
                    break;
                default:
                    agg_type = types::complex_logical_type(types::logical_type::BIGINT);
                    break;
            }
            agg_type.set_alias(agg.alias);
            output_types.push_back(agg_type);
        }

        // Create output chunk with capacity for num_groups
        output_ = base::operators::make_operator_data(context_->resource(), output_types, num_groups);
        auto& output = output_->data_chunk();
        output.set_cardinality(num_groups);

        // Fill key columns in output
        for (size_t k = 0; k < keys_.size(); k++) {
            for (size_t g = 0; g < num_groups; g++) {
                output.set_value(k, g, unique_keys[g][k]);
            }
        }

        auto t3 = std::chrono::high_resolution_clock::now();

        // Calculate aggregates
        calculate_aggregates(input, group_ids, num_groups, output, keys_.size());

        auto t4 = std::chrono::high_resolution_clock::now();

        std::cout << "[TIMING columnar_group] rows=" << input.size()
                  << ", groups=" << num_groups
                  << " | find_cols: " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms"
                  << ", build_groups: " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms"
                  << ", setup_output: " << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms"
                  << ", calc_agg: " << std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count() << "ms"
                  << ", total: " << std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t0).count() << "ms"
                  << std::endl;
    }

    size_t columnar_group::build_group_ids(const vector::data_chunk_t& input,
                                           std::vector<uint32_t>& group_ids,
                                           std::vector<std::vector<types::logical_value_t>>& unique_keys) {
        size_t row_count = input.size();
        group_ids.resize(row_count);

        if (keys_.empty()) {
            // No GROUP BY - all rows in one group
            std::fill(group_ids.begin(), group_ids.end(), 0);
            unique_keys.push_back({});
            return 1;
        }

        // Hash map: key values -> group_id
        // For simplicity, use string representation of key as hash key
        std::unordered_map<std::string, uint32_t> key_to_group;
        uint32_t next_group_id = 0;

        for (size_t row = 0; row < row_count; row++) {
            // Build key string for this row
            std::string key_str;
            std::vector<types::logical_value_t> key_values;
            key_values.reserve(keys_.size());

            bool has_null = false;
            for (const auto& key : keys_) {
                auto value = input.value(key.column_index, row);
                if (value.is_null()) {
                    has_null = true;
                    break;
                }
                key_values.push_back(value);
                // Append to key string for hashing
                key_str += value_to_string(value) + "\x00";
            }

            if (has_null) {
                // Skip rows with NULL keys (standard SQL behavior)
                group_ids[row] = UINT32_MAX; // Mark as invalid
                continue;
            }

            auto it = key_to_group.find(key_str);
            if (it != key_to_group.end()) {
                group_ids[row] = it->second;
            } else {
                uint32_t gid = next_group_id++;
                key_to_group[key_str] = gid;
                group_ids[row] = gid;
                unique_keys.push_back(std::move(key_values));
            }
        }

        return next_group_id;
    }

    void columnar_group::calculate_aggregates(const vector::data_chunk_t& input,
                                              const std::vector<uint32_t>& group_ids,
                                              size_t num_groups,
                                              vector::data_chunk_t& output,
                                              size_t key_count) {
        size_t row_count = input.size();

        for (size_t agg_idx = 0; agg_idx < aggregates_.size(); agg_idx++) {
            const auto& agg = aggregates_[agg_idx];
            size_t output_col = key_count + agg_idx;

            switch (agg.type) {
                case expressions::aggregate_type::count: {
                    if (agg.distinct && agg.column_index < input.column_count()) {
                        // COUNT(DISTINCT column)
                        std::vector<std::unordered_set<std::string>> distinct_sets(num_groups);
                        for (size_t row = 0; row < row_count; row++) {
                            uint32_t gid = group_ids[row];
                            if (gid == UINT32_MAX) continue;
                            auto value = input.value(agg.column_index, row);
                            if (!value.is_null()) {
                                distinct_sets[gid].insert(value_to_string(value));
                            }
                        }
                        for (size_t g = 0; g < num_groups; g++) {
                            output.set_value(output_col, g,
                                types::logical_value_t(static_cast<int64_t>(distinct_sets[g].size())));
                        }
                    } else if (agg.column_index == SIZE_MAX) {
                        // COUNT(*)
                        std::vector<int64_t> counts(num_groups, 0);
                        for (size_t row = 0; row < row_count; row++) {
                            uint32_t gid = group_ids[row];
                            if (gid == UINT32_MAX) continue;
                            counts[gid]++;
                        }
                        for (size_t g = 0; g < num_groups; g++) {
                            output.set_value(output_col, g, types::logical_value_t(counts[g]));
                        }
                    } else {
                        // COUNT(column) - count non-null
                        std::vector<int64_t> counts(num_groups, 0);
                        for (size_t row = 0; row < row_count; row++) {
                            uint32_t gid = group_ids[row];
                            if (gid == UINT32_MAX) continue;
                            auto value = input.value(agg.column_index, row);
                            if (!value.is_null()) {
                                counts[gid]++;
                            }
                        }
                        for (size_t g = 0; g < num_groups; g++) {
                            output.set_value(output_col, g, types::logical_value_t(counts[g]));
                        }
                    }
                    break;
                }

                case expressions::aggregate_type::sum: {
                    std::vector<double> sums(num_groups, 0.0);
                    for (size_t row = 0; row < row_count; row++) {
                        uint32_t gid = group_ids[row];
                        if (gid == UINT32_MAX) continue;
                        auto value = input.value(agg.column_index, row);
                        if (!value.is_null()) {
                            sums[gid] += value_to_double(value);
                        }
                    }
                    for (size_t g = 0; g < num_groups; g++) {
                        output.set_value(output_col, g, types::logical_value_t(sums[g]));
                    }
                    break;
                }

                case expressions::aggregate_type::avg: {
                    std::vector<double> sums(num_groups, 0.0);
                    std::vector<int64_t> counts(num_groups, 0);
                    for (size_t row = 0; row < row_count; row++) {
                        uint32_t gid = group_ids[row];
                        if (gid == UINT32_MAX) continue;
                        auto value = input.value(agg.column_index, row);
                        if (!value.is_null()) {
                            sums[gid] += value_to_double(value);
                            counts[gid]++;
                        }
                    }
                    for (size_t g = 0; g < num_groups; g++) {
                        double avg = counts[g] > 0 ? sums[g] / counts[g] : 0.0;
                        output.set_value(output_col, g, types::logical_value_t(avg));
                    }
                    break;
                }

                case expressions::aggregate_type::min: {
                    std::vector<types::logical_value_t> mins(num_groups);
                    std::vector<bool> has_value(num_groups, false);
                    for (size_t row = 0; row < row_count; row++) {
                        uint32_t gid = group_ids[row];
                        if (gid == UINT32_MAX) continue;
                        auto value = input.value(agg.column_index, row);
                        if (!value.is_null()) {
                            if (!has_value[gid] || value < mins[gid]) {
                                mins[gid] = value;
                                has_value[gid] = true;
                            }
                        }
                    }
                    for (size_t g = 0; g < num_groups; g++) {
                        if (has_value[g]) {
                            output.set_value(output_col, g, mins[g]);
                        } else {
                            output.set_value(output_col, g, types::logical_value_t());
                        }
                    }
                    break;
                }

                case expressions::aggregate_type::max: {
                    std::vector<types::logical_value_t> maxs(num_groups);
                    std::vector<bool> has_value(num_groups, false);
                    for (size_t row = 0; row < row_count; row++) {
                        uint32_t gid = group_ids[row];
                        if (gid == UINT32_MAX) continue;
                        auto value = input.value(agg.column_index, row);
                        if (!value.is_null()) {
                            if (!has_value[gid] || value > maxs[gid]) {
                                maxs[gid] = value;
                                has_value[gid] = true;
                            }
                        }
                    }
                    for (size_t g = 0; g < num_groups; g++) {
                        if (has_value[g]) {
                            output.set_value(output_col, g, maxs[g]);
                        } else {
                            output.set_value(output_col, g, types::logical_value_t());
                        }
                    }
                    break;
                }

                default:
                    // Unknown aggregate type - output zeros
                    for (size_t g = 0; g < num_groups; g++) {
                        output.set_value(output_col, g, types::logical_value_t(int64_t(0)));
                    }
                    break;
            }
        }
    }

} // namespace components::document_table::operators
