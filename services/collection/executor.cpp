#include "executor.hpp"

#include <stdexcept>
#include <components/index/disk/route.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/operator_update.hpp>
#include <components/physical_plan/collection/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/system_command.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/memory_storage.hpp>
#include <services/memory_storage/route.hpp>

using namespace components::cursor;

namespace services::collection::executor {

    plan_t::plan_t(std::stack<components::collection::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage) {}

    executor_t::executor_t(services::memory_storage_t* memory_storage, log_t&& log)
        : actor_zeta::basic_actor<executor_t>{memory_storage}
        , memory_storage_(memory_storage->address())
        , plans_(resource())
        , log_(log)
        , execute_plan_(
              actor_zeta::make_behavior(resource(), handler_id(route::execute_plan), this, &executor_t::execute_plan))
        , create_documents_(actor_zeta::make_behavior(resource(),
                                                      handler_id(route::create_documents),
                                                      this,
                                                      &executor_t::create_documents))
        , create_index_finish_(actor_zeta::make_behavior(resource(),
                                                         handler_id(index::route::success_create),
                                                         this,
                                                         &executor_t::create_index_finish))
        , create_index_finish_index_exist_(actor_zeta::make_behavior(resource(),
                                                                     handler_id(index::route::error),
                                                                     this,
                                                                     &executor_t::create_index_finish_index_exist))
        , index_modify_finish_(actor_zeta::make_behavior(resource(),
                                                         handler_id(index::route::success),
                                                         this,
                                                         &executor_t::index_modify_finish))
        , index_find_finish_(actor_zeta::make_behavior(resource(),
                                                       handler_id(index::route::success_find),
                                                       this,
                                                       &executor_t::index_find_finish)) {}

    actor_zeta::behavior_t executor_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case handler_id(route::create_documents): {
                    create_documents_(msg);
                    break;
                }
                case handler_id(index::route::success_create): {
                    create_index_finish_(msg);
                    break;
                }
                case handler_id(index::route::error): {
                    create_index_finish_index_exist_(msg);
                    break;
                }
                case handler_id(index::route::success): {
                    index_modify_finish_(msg);
                    break;
                }
                case handler_id(index::route::success_find): {
                    index_find_finish_(msg);
                    break;
                }
            }
        });
    }

    auto executor_t::make_type() const noexcept -> const char* const { return "executor"; }

    void executor_t::execute_plan(const components::session::session_id_t& session,
                                  components::logical_plan::node_ptr logical_plan,
                                  components::logical_plan::storage_parameters parameters,
                                  services::context_storage_t&& context_storage,
                                  components::catalog::used_format_t data_format) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        // Preprocessing: convert document_table INSERT documents → data_chunk
        // so we can route through the standard table planner
        if (data_format == components::catalog::used_format_t::document_table &&
            logical_plan->type() == components::logical_plan::node_type::insert_t) {
            // Find node_data_t with documents in children
            for (auto& child : logical_plan->children()) {
                if (child->type() == components::logical_plan::node_type::data_t) {
                    auto* data_node = static_cast<components::logical_plan::node_data_t*>(child.get());
                    if (data_node->uses_documents()) {
                        // Get context collection
                        auto it = context_storage.find(logical_plan->collection_full_name());
                        if (it != context_storage.end() &&
                            it->second->storage_type() == storage_type_t::DOCUMENT_TABLE) {
                            auto& storage = it->second->document_table_storage().storage();

                            // Collect (id, doc) pairs
                            auto& docs = data_node->documents();
                            std::pmr::vector<std::pair<components::document::document_id_t,
                                                       components::document::document_ptr>>
                                pairs(resource());
                            pairs.reserve(docs.size());
                            for (const auto& doc : docs) {
                                if (doc && doc->is_valid()) {
                                    pairs.emplace_back(components::document::get_document_id(doc), doc);
                                }
                            }

                            // Convert: schema evolution + documents → data_chunk + id_to_row update
                            auto chunk = storage.prepare_insert(pairs);

                            // Replace documents with data_chunk in logical plan
                            data_node->set_data_chunk(std::move(chunk));

                            // Route through table planner
                            data_format = components::catalog::used_format_t::columns;
                        }
                        break;
                    }
                }
            }
        }

        // document_table routes through table planner for all operations.
        // INSERT was preprocessed above (documents → data_chunk, format set to columns).
        // SELECT/DELETE/UPDATE go directly to table planner since table operators
        // now have columnar_group (fast GROUP BY) and projection-aware full_scan.
        if (data_format == components::catalog::used_format_t::document_table) {
            data_format = components::catalog::used_format_t::columns;
        }

        // TODO: this does not handle cross documents/columns operations
        components::base::operators::operator_ptr plan;
        if (data_format == components::catalog::used_format_t::documents) {
            plan = collection::planner::create_plan(context_storage,
                                                    logical_plan,
                                                    components::logical_plan::limit_t::unlimit());
        } else if (data_format == components::catalog::used_format_t::columns) {
            plan = table::planner::create_plan(context_storage,
                                               logical_plan,
                                               components::logical_plan::limit_t::unlimit());
        }

        if (!plan) {
            actor_zeta::send(memory_storage_,
                             address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"));
            return;
        }
        plan->set_as_root();
        traverse_plan_(session, std::move(plan), std::move(parameters), std::move(context_storage));
    }

    void executor_t::create_documents(const components::session::session_id_t& session,
                                      context_collection_t* collection,
                                      const std::pmr::vector<document_ptr>& documents) {
        trace(log_,
              "executor_t::create_documents: {}::{}, count: {}",
              collection->name().database,
              collection->name().collection,
              documents.size());
        //components::pipeline::context_t pipeline_context{session, address(), components::logical_plan::storage_parameters{}};
        //insert_(&pipeline_context, documents);
        for (const auto& doc : documents) {
            collection->document_storage().emplace(components::document::get_document_id(doc), doc);
        }
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_documents_finish), session);
    }

    void executor_t::traverse_plan_(const components::session::session_id_t& session,
                                    components::collection::operators::operator_ptr&& plan,
                                    components::logical_plan::storage_parameters&& parameters,
                                    services::context_storage_t&& context_storage) {
        std::stack<components::collection::operators::operator_ptr> look_up;
        std::stack<components::collection::operators::operator_ptr> sub_plans;
        look_up.push(plan);
        while (!look_up.empty()) {
            auto check_op = look_up.top();
            while (check_op->right() == nullptr) {
                check_op = check_op->left();
                if (check_op == nullptr) {
                    break;
                }
            }
            sub_plans.push(look_up.top());
            look_up.pop();
            if (check_op != nullptr) {
                look_up.push(check_op->right());
                look_up.push(check_op->left());
            }
        }

        trace(log_, "executor::subplans count {}", sub_plans.size());
        // start execution chain by sending first avaliable sub_plan
        auto current_plan =
            (plans_.emplace(session, plan_t{std::move(sub_plans), parameters, std::move(context_storage)}))
                .first->second.sub_plans.top();
        execute_sub_plan_(session, current_plan, parameters);
    }

    void executor_t::execute_sub_plan_(const components::session::session_id_t& session,
                                       components::collection::operators::operator_ptr plan,
                                       components::logical_plan::storage_parameters parameters) {
        trace(log_, "executor::execute_sub_plan, session: {}", session.data());
        if (!plan) {
            execute_sub_plan_finish_(
                session,
                make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"));
            return;
        }
        auto collection = plan->context();
        if (collection && collection->dropped()) {
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), error_code_t::collection_dropped, "collection dropped"));
            return;
        }
        components::pipeline::context_t pipeline_context{session, address(), memory_storage_, parameters};
        try {
            plan->on_execute(&pipeline_context);
        } catch (const std::exception& e) {
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), error_code_t::other_error, e.what()));
            return;
        }
        if (!plan->is_executed()) {
            sessions::make_session(
                collection->sessions(),
                session,
                sessions::suspend_plan_t{memory_storage_, std::move(plan), std::move(pipeline_context)});
            return;
        }

        switch (plan->type()) {
            case components::collection::operators::operator_type::insert: {
                insert_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::remove: {
                delete_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::update: {
                update_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::raw_data:
            case components::collection::operators::operator_type::join:
            case components::collection::operators::operator_type::aggregate: {
                aggregate_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::add_index:
            case components::collection::operators::operator_type::drop_index: {
                // nothing to do
                return;
            }
            default: {
                execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
                return;
            }
        }
    }

    void executor_t::execute_sub_plan_finish_(
        const components::session::session_id_t& session,
        cursor_t_ptr result,
        components::base::operators::operator_write_data_t::updated_types_map_t updates) {
        if (result->is_error() || !plans_.contains(session)) {
            execute_plan_finish_(session, std::move(result), std::move(updates));
            return;
        }
        auto& plan = plans_.at(session);
        if (plan.sub_plans.size() == 1) {
            execute_plan_finish_(session, std::move(result), std::move(updates));
        } else {
            assert(!plan.sub_plans.empty() && "executor_t:execute_sub_plan_finish_: sub plans execution failed");
            plan.sub_plans.pop();
            execute_sub_plan_(session, plan.sub_plans.top(), plan.parameters);
        }
    }

    void
    executor_t::execute_plan_finish_(const components::session::session_id_t& session,
                                     cursor_t_ptr&& cursor,
                                     components::base::operators::operator_write_data_t::updated_types_map_t updates) {
        trace(log_, "executor::execute_plan_finish, success: {}", cursor->is_success());
        if (updates.empty()) {
            actor_zeta::send(memory_storage_,
                             address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             std::move(cursor));
        } else {
            actor_zeta::send(memory_storage_,
                             address(),
                             handler_id(memory_storage::route::execute_plan_delete_finish),
                             session,
                             std::move(cursor),
                             std::move(updates));
        }
        plans_.erase(session);
    }

    void executor_t::aggregate_document_impl(const components::session::session_id_t& session,
                                             context_collection_t* collection,
                                             components::collection::operators::operator_ptr plan) {
        if (plan->type() == components::collection::operators::operator_type::aggregate) {
            trace(log_, "executor::execute_plan : operators::operator_type::agreggate");
        } else if (plan->type() == components::collection::operators::operator_type::join) {
            trace(log_, "executor::execute_plan : operators::operator_type::join");
        } else {
            trace(log_, "executor::execute_plan : operators::operator_type::raw_data");
        }
        if (plan->is_root()) {
            if (!collection) {
                if (plan->output()->uses_data_chunk()) {
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(plan->output()->data_chunk())));
                } else {
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(plan->output()->documents())));
                }
            } else if (collection->uses_datatable()) {
                components::vector::data_chunk_t chunk(resource(), collection->data_table().copy_types());
                if (plan->output()) {
                    chunk = std::move(plan->output()->data_chunk());
                }
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(chunk)));
            } else {
                std::pmr::vector<document_ptr> docs;
                if (plan->output()) {
                    docs = std::move(plan->output()->documents());
                }
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(docs)));
            }
        } else {
            execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
        }
    }

    void executor_t::update_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          components::collection::operators::operator_ptr plan) {
        trace(log_, "executor::execute_plan : operators::operator_type::update");

        if (collection->uses_datatable()) {
            if (plan->output()) {
                actor_zeta::send(collection->disk(),
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 plan->modified()->ids());
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(plan->output()->data_chunk())));
            } else {
                if (plan->modified()) {
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     plan->modified()->ids());
                    components::vector::data_chunk_t chunk(resource(),
                                                           collection->data_table().copy_types());
                    chunk.set_cardinality(std::get<std::pmr::vector<size_t>>(plan->modified()->ids()).size());
                    // TODO: fill chunk with modified rows
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(chunk)));
                } else {
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<size_t>{resource()});
                    execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
                }
            }
        } else {
            if (plan->output()) {
                auto new_id = components::document::get_document_id(plan->output()->documents().front());
                std::pmr::vector<document_id_t> ids{resource()};
                std::pmr::vector<document_ptr> documents{resource()};
                ids.emplace_back(new_id);
                actor_zeta::send(collection->disk(),
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 ids);
                for (const auto& id : ids) {
                    documents.emplace_back(collection->document_storage().at(id));
                }
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(documents)));
            } else {
                if (plan->modified()) {
                    std::pmr::vector<document_ptr> documents(resource());
                    for (const auto& id :
                         std::get<std::pmr::vector<components::document::document_id_t>>(plan->modified()->ids())) {
                        documents.emplace_back(collection->document_storage().at(id));
                    }
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     plan->modified()->ids());
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(documents)));
                } else {
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<document_id_t>{resource()});
                    execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
                }
            }
        }
    }

    void executor_t::insert_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          components::collection::operators::operator_ptr plan) {
        trace(log_,
              "executor::execute_plan : operators::operator_type::insert {}",
              plan->output() ? plan->output()->size() : 0);
        
        // DEBUG: Log storage type and output info
        trace(log_, 
              "executor::insert_document_impl DEBUG: storage_type={}, has_output={}, uses_documents={}, uses_data_chunk={}",
              static_cast<int>(collection->storage_type()),
              plan->output() != nullptr,
              plan->output() ? plan->output()->uses_documents() : false,
              plan->output() ? plan->output()->uses_data_chunk() : false);
        
        // TODO: disk support for data_table
        if (!plan->output() || plan->output()->uses_documents()) {
            trace(log_, "executor::insert_document_impl: Using documents path");
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::write_documents),
                             session,
                             collection->name().database,
                             collection->name().collection,
                             plan->output() ? std::move(plan->output()->documents())
                                            : std::pmr::vector<document_ptr>{resource()});
            std::pmr::vector<document_ptr> documents(resource());
            if (plan->modified()) {
                for (const auto& id :
                     std::get<std::pmr::vector<components::document::document_id_t>>(plan->modified()->ids())) {
                    documents.emplace_back(collection->document_storage().at(id));
                }
            } else {
                for (const auto& doc : collection->document_storage()) {
                    documents.emplace_back(doc.second);
                }
            }
            execute_sub_plan_finish_(session, make_cursor(resource(), std::move(documents)));
        } else {
            trace(log_, "executor::insert_document_impl: Using data_chunk path");
            // For document_table, use the output data_chunk directly since it already contains the inserted data
            if (collection->storage_type() == storage_type_t::DOCUMENT_TABLE && plan->output() && plan->output()->uses_data_chunk()) {
                trace(log_, "executor::insert_document_impl: DOCUMENT_TABLE branch, output size={}", plan->output()->size());
                // Get reference to the output chunk
                auto& output_chunk = plan->output()->data_chunk();
                trace(log_, "executor::insert_document_impl: output_chunk size={}, column_count={}", output_chunk.size(), output_chunk.column_count());
                
                // Create a copy for disk
                components::vector::data_chunk_t chunk_for_disk(resource(), output_chunk.types(), output_chunk.size());
                output_chunk.copy(chunk_for_disk, 0);
                trace(log_, "executor::insert_document_impl: Created chunk_for_disk, size={}", chunk_for_disk.size());
                
                actor_zeta::send(collection->disk(),
                                 address(),
                                 disk::handler_id(disk::route::write_documents),
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 std::move(chunk_for_disk));
                
                // Create another copy for the cursor
                components::vector::data_chunk_t chunk_for_cursor(resource(), output_chunk.types(), output_chunk.size());
                output_chunk.copy(chunk_for_cursor, 0);
                trace(log_, "executor::insert_document_impl: Created chunk_for_cursor, size={}, creating cursor", chunk_for_cursor.size());
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(chunk_for_cursor)));
                trace(log_, "executor::insert_document_impl: DOCUMENT_TABLE branch completed");
                return;
            }
            
            trace(log_, "executor::insert_document_impl: Not DOCUMENT_TABLE or conditions not met, using regular table path");
            
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::write_documents),
                             session,
                             collection->name().database,
                             collection->name().collection,
                             plan->output() ? std::move(plan->output()->data_chunk())
                                            : components::vector::data_chunk_t{resource(), {}});
            size_t size = 0;
            if (plan->modified()) {
                size = std::get<std::pmr::vector<size_t>>(plan->modified()->ids()).size();
            } else {
                size = collection->data_table().calculate_size();
            }
            components::vector::data_chunk_t chunk(resource(), {}, size);
            chunk.set_cardinality(size);
            execute_sub_plan_finish_(session, make_cursor(resource(), std::move(chunk)));
        }
    }

    void executor_t::delete_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          components::collection::operators::operator_ptr plan) {
        trace(log_, "executor::execute_plan : operators::operator_type::remove");

        if (collection->uses_datatable()) {
            auto modified = plan->modified() ? plan->modified()->ids() : std::pmr::vector<size_t>{resource()};
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::remove_documents),
                             session,
                             collection->name().database,
                             collection->name().collection,
                             modified);
            size_t size = plan->modified()->size();
            components::vector::data_chunk_t chunk(resource(), collection->data_table().copy_types(), size);
            chunk.set_cardinality(size);
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), std::move(chunk)),
                                     std::move(plan->modified()->updated_types_map()));
        } else {
            auto modified = plan->modified() ? plan->modified()->ids() : std::pmr::vector<document_id_t>{resource()};
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::remove_documents),
                             session,
                             collection->name().database,
                             collection->name().collection,
                             modified);
            std::pmr::vector<document_ptr> documents(resource());
            documents.resize(plan->modified()->size());
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), std::move(documents)),
                                     std::move(plan->modified()->updated_types_map()));
        }
    }

} // namespace services::collection::executor