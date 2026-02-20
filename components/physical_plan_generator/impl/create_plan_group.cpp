#include "create_plan_group.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <components/physical_plan/collection/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/collection/operators/get/simple_value.hpp>
#include <components/physical_plan/collection/operators/operator_group.hpp>

#include <components/physical_plan/table/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/table/operators/get/simple_value.hpp>
#include <components/physical_plan/table/operators/operator_group.hpp>
#include <components/physical_plan/table/operators/columnar_group.hpp>

namespace services::collection::planner::impl {

    namespace {

        void add_group_scalar(boost::intrusive_ptr<components::collection::operators::operator_group_t>& group,
                              const components::expressions::scalar_expression_t* expr) {
            using components::expressions::scalar_type;

            switch (expr->type()) {
                case scalar_type::get_field: {
                    auto field = expr->params().empty()
                                     ? expr->key()
                                     : std::get<components::expressions::key_t>(expr->params().front());
                    group->add_key(expr->key().as_string(),
                                   components::collection::operators::get::simple_value_t::create(field));
                    break;
                }
                default:
                    assert(false && "not implemented create plan to scalar exression");
                    break;
            }
        }

        void add_group_aggregate(context_collection_t* context,
                                 boost::intrusive_ptr<components::collection::operators::operator_group_t>& group,
                                 const components::expressions::aggregate_expression_t* expr) {
            using components::expressions::aggregate_type;

            switch (expr->type()) {
                case aggregate_type::count: {
                    group->add_value(expr->key().as_string(),
                                     boost::intrusive_ptr(
                                         new components::collection::operators::aggregate::operator_count_t(context)));
                    break;
                }
                case aggregate_type::sum: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::sum:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_sum_t(context, field)));
                    break;
                }
                case aggregate_type::avg: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::avg:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_avg_t(context, field)));
                    break;
                }
                case aggregate_type::min: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::min:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_min_t(context, field)));
                    break;
                }
                case aggregate_type::max: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::max:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_max_t(context, field)));
                    break;
                }
                default:
                    assert(false && "not implemented create plan to aggregate exression");
                    break;
            }
        }

    } // namespace

    components::collection::operators::operator_ptr create_plan_group(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node) {
        boost::intrusive_ptr<components::collection::operators::operator_group_t> group;
        auto collection_context = context.at(node->collection_full_name());
        if (collection_context) {
            group = new components::collection::operators::operator_group_t(collection_context);
        } else {
            group = new components::collection::operators::operator_group_t(node->resource());
        }
        std::for_each(node->expressions().begin(),
                      node->expressions().end(),
                      [&](const components::expressions::expression_ptr& expr) {
                          if (expr->group() == components::expressions::expression_group::scalar) {
                              add_group_scalar(
                                  group,
                                  static_cast<const components::expressions::scalar_expression_t*>(expr.get()));
                          } else if (expr->group() == components::expressions::expression_group::aggregate) {
                              add_group_aggregate(
                                  context.at(node->collection_full_name()),
                                  group,
                                  static_cast<const components::expressions::aggregate_expression_t*>(expr.get()));
                          }
                      });
        return group;
    }

} // namespace services::collection::planner::impl

namespace services::table::planner::impl {

    using components::expressions::expression_group;

    components::base::operators::operator_ptr create_plan_group(const context_storage_t& context,
                                                                const components::logical_plan::node_ptr& node) {
        auto collection_context = context.at(node->collection_full_name());
        // columnar_group requires non-null context for resource allocation
        assert(collection_context && "create_plan_group: null context for columnar_group");
        auto group = boost::intrusive_ptr(new components::table::operators::columnar_group(collection_context));

        for (const auto& expr : node->expressions()) {
            if (expr->group() == expression_group::scalar) {
                auto scalar = static_cast<const components::expressions::scalar_expression_t*>(expr.get());

                // Get column name from params or key
                std::string column_name;
                if (!scalar->params().empty()) {
                    const auto& param = scalar->params()[0];
                    if (std::holds_alternative<components::expressions::key_t>(param)) {
                        column_name = std::get<components::expressions::key_t>(param).as_string();
                    }
                }
                if (column_name.empty() && scalar->key().is_string()) {
                    column_name = scalar->key().as_string();
                }

                std::string alias = scalar->key().as_string();
                group->add_key(column_name, alias);

            } else if (expr->group() == expression_group::aggregate) {
                auto agg = static_cast<const components::expressions::aggregate_expression_t*>(expr.get());

                std::string column_name;
                bool is_distinct = false;

                if (!agg->params().empty()) {
                    const auto& param = agg->params()[0];
                    if (std::holds_alternative<components::expressions::key_t>(param)) {
                        const auto& key = std::get<components::expressions::key_t>(param);
                        if (key.is_string() && key.as_string() != "*") {
                            column_name = key.as_string();
                        }
                    }
                }

                std::string alias = agg->key().as_string();

                // COUNT with a column name = COUNT(DISTINCT column)
                if (agg->type() == components::expressions::aggregate_type::count &&
                    !column_name.empty()) {
                    is_distinct = true;
                }

                group->add_aggregate(agg->type(), column_name, alias, is_distinct);
            }
        }

        return group;
    }

} // namespace services::table::planner::impl
