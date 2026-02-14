#include "operator_aggregate.hpp"
#include <services/collection/collection.hpp>

namespace components::table::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(services::collection::context_collection_t* context)
        : read_only_operator_t(context, operator_type::aggregate) {}

    void operator_aggregate_t::on_execute_impl(pipeline::context_t*) { aggregate_result_ = aggregate_impl(); }

    void operator_aggregate_t::set_value(std::pmr::vector<types::logical_value_t>& row, std::string_view key) const {
        // Use the actual key parameter (e.g., "count", "users") instead of key_impl()
        // which returns a hardcoded value like "count" for all COUNT operators.
        // This allows multiple aggregate functions of the same type in one query.
        auto res_it = std::find_if(row.begin(), row.end(), [&](const types::logical_value_t& v) {
            return !v.type().extension() ? false : v.type().alias() == key;
        });
        if (res_it == row.end()) {
            auto result = aggregate_result_;
            result.set_alias(std::string(key));  // Set the correct alias for this aggregate
            row.emplace_back(std::move(result));
        } else {
            *res_it = aggregate_result_;
        }
    }

    types::logical_value_t operator_aggregate_t::value() const { return aggregate_result_; }
} // namespace components::table::operators::aggregate
