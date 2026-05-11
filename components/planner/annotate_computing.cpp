#include "annotate_computing.hpp"

#include <components/catalog/computed_schema.hpp>
#include <components/logical_plan/node_aggregate.hpp>

namespace components::planner {
    using components::logical_plan::computing_side_t;
    using components::logical_plan::node_aggregate_t;
    using components::logical_plan::node_ptr;
    using components::logical_plan::node_type;
    using catalog_t = components::catalog::catalog;
    using components::catalog::computed_schema;
    using components::catalog::table_id;

    namespace {

        void annotate_recursive(std::pmr::memory_resource* resource,
                                const node_ptr& node,
                                const catalog_t& cat) {
            if (!node) return;
            for (auto& ch : node->children()) {
                annotate_recursive(resource, ch, cat);
            }
            if (node->type() != node_type::aggregate_t) return;
            auto* agg = reinterpret_cast<node_aggregate_t*>(node.get());
            const auto& full = agg->collection_full_name();
            if (full.database.empty()) return;
            table_id id(resource, full);
            if (!cat.table_computes(id)) return;

            const auto& cs = cat.get_computing_table_schema(id);
            // Legacy dynamic-single-table layout doesn't need scan_computing_table —
            // fields live as physical columns of `main`, regular transfer_scan works.
            if (!cs.is_sparse()) return;
            auto sch = cs.latest_types_struct();
            const auto& fields = sch.child_types();
            std::string main_table_str{full.collection};

            std::vector<computing_side_t> sides;
            sides.reserve(fields.size());
            for (const auto& f : fields) {
                std::string side_name = computed_schema::side_table_name(main_table_str, f.alias(), f);
                sides.push_back(computing_side_t{
                    collection_full_name_t{full.database, side_name},
                    f});
            }
            agg->set_computing_sides(std::move(sides));
        }

    } // namespace

    void annotate_computing_aggregates(std::pmr::memory_resource* resource,
                                       node_ptr root,
                                       const components::catalog::catalog& cat) {
        annotate_recursive(resource, root, cat);
    }

} // namespace components::planner
