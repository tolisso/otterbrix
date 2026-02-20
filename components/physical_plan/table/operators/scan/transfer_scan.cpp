#include "transfer_scan.hpp"

#include <services/collection/collection.hpp>

namespace components::table::operators {

    transfer_scan::transfer_scan(services::collection::context_collection_t* context, logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , limit_(limit) {}

    void transfer_scan::on_execute_impl(pipeline::context_t*) {
        trace(context_->log(), "transfer_scan");
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }

        auto types = context_->data_table().copy_types();
        output_ = base::operators::make_operator_data(context_->resource(), types);
        std::vector<table::storage_index_t> column_indices;
        column_indices.reserve(context_->data_table().column_count());
        for (int64_t i = 0; i < context_->data_table().column_count(); i++) {
            column_indices.emplace_back(i);
        }
        if (column_indices.empty()) {
            return; // 0-column table: nothing to scan
        }
        table::table_scan_state state(std::pmr::get_default_resource());
        context_->data_table().initialize_scan(state, column_indices);
        // TODO: check limit inside scan
        context_->data_table().scan(output_->data_chunk(), state);
        if (limit_.limit() >= 0) {
            output_->data_chunk().set_cardinality(std::min<size_t>(output_->data_chunk().size(), limit_.limit()));
        }
    }

} // namespace components::table::operators
