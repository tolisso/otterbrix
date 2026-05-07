#include "dispatcher.hpp"
#include "validate_logical_plan.hpp"

#include <components/logical_plan/node_checkpoint.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/table/column_definition.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/compare_expression.hpp>

#include <chrono>
#include <core/executor.hpp>
#include <core/tracy/tracy.hpp>
#include <thread>

#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/expand_computing.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/planner.hpp>

#include <services/collection/context_storage.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;
using namespace components::types;

namespace services::dispatcher {

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log,
                                               run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone())
        , run_fn_(std::move(run_fn))
        , catalog_(resource_ptr)
        , databases_(resource_ptr)
        , collections_(resource_ptr)
        , executors_(resource_ptr)
        , executor_addresses_(resource_ptr)
        , update_result_(resource_ptr)
        , pending_void_(resource_ptr)
        , pending_cursor_(resource_ptr)
        , pending_size_(resource_ptr)
        , pending_execute_(resource_ptr)
        , pending_signatures_(resource_ptr) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* { return "manager_dispatcher"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_dispatcher_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        std::lock_guard<std::mutex> guard(mutex_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_dispatcher_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::execute_plan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::size>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::size, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::get_schema>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::get_schema, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::register_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::unregister_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::close_cursor>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::close_cursor, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::begin_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::begin_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::commit_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::commit_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::abort_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::abort_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::lowest_active_start_time>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::lowest_active_start_time, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_dispatcher_t::poll_pending() {
        for (auto it = pending_void_.begin(); it != pending_void_.end();) {
            if (it->available()) {
                it = pending_void_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_cursor_.begin(); it != pending_cursor_.end();) {
            if (it->available()) {
                it = pending_cursor_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_size_.begin(); it != pending_size_.end();) {
            if (it->available()) {
                it = pending_size_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_execute_.begin(); it != pending_execute_.end();) {
            if (it->available()) {
                it = pending_execute_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_signatures_.begin(); it != pending_signatures_.end();) {
            if (it->available()) {
                it = pending_signatures_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void manager_dispatcher_t::sync(sync_pack pack) {
        constexpr static int wal_idx = 0;
        constexpr static int disk_idx = 1;
        constexpr static int index_idx = 2;
        wal_address_ = std::get<wal_idx>(pack);
        disk_address_ = std::get<disk_idx>(pack);
        index_address_ = std::get<index_idx>(pack);

        executors_.reserve(executor_pool_size_);
        executor_addresses_.reserve(executor_pool_size_);
        for (std::size_t i = 0; i < executor_pool_size_; ++i) {
            auto exec = actor_zeta::spawn<collection::executor::executor_t>(resource(),
                                                                            address(),
                                                                            wal_address_,
                                                                            disk_address_,
                                                                            index_address_,
                                                                            &txn_manager_,
                                                                            log_.clone());
            executor_addresses_.push_back(exec->address());
            executors_.push_back(std::move(exec));
        }
        trace(log_, "manager_dispatcher_t: spawned {} executors with WAL/Disk/Index addresses", executor_pool_size_);
    }

    void manager_dispatcher_t::init_from_state(std::pmr::set<database_name_t> databases,
                                               std::pmr::set<collection_full_name_t> collections) {
        trace(log_, "manager_dispatcher_t::init_from_state: populating storage");

        databases_ = std::move(databases);
        trace(log_, "manager_dispatcher_t::init_from_state: initialized {} databases", databases_.size());

        for (const auto& full_name : collections) {
            debug(log_,
                  "manager_dispatcher_t::init_from_state: creating collection {}.{}",
                  full_name.database,
                  full_name.collection);
            collections_.insert(full_name);
            debug(log_,
                  "manager_dispatcher_t::init_from_state: collection {}.{} initialized",
                  full_name.database,
                  full_name.collection);
        }

        trace(log_, "manager_dispatcher_t::init_from_state: complete - {} collections", collections_.size());
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::execute_plan(components::session::session_id_t session,
                                       node_ptr plan,
                                       parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());

        auto params_for_wal = make_parameter_node(resource());
        params_for_wal->set_parameters(params->parameters());

        auto logic_plan = create_logic_plan(plan);
        // Optimizer: constant folding, etc.
        logic_plan = components::planner::optimize(resource(), logic_plan, &catalog_, params.get());
        table_id id(resource(), logic_plan->collection_full_name());
        core::error_t error = core::error_t::no_error();
        switch (logic_plan->type()) {
            case node_type::create_database_t:
                if (auto res = check_namespace_exists(resource(), catalog_, id); !res.contains_error()) {
                    error = core::error_t(core::error_code_t::database_already_exists,
                                          std::pmr::string{"database already exists", resource()});
                }
                break;
            case node_type::drop_database_t:
                if (auto res = check_namespace_exists(resource(), catalog_, id); res.contains_error()) {
                    error = std::move(res);
                }
                break;
            case node_type::create_collection_t:
                if (auto database_res = check_namespace_exists(resource(), catalog_, id);
                    database_res.contains_error()) {
                    error = std::move(database_res);
                } else if (auto table_res = check_collection_exists(resource(), catalog_, id);
                           !table_res.contains_error()) {
                    error = core::error_t(core::error_code_t::database_already_exists,
                                          std::pmr::string{"table already exists", resource()});
                } else {
                    auto& n = reinterpret_cast<node_create_collection_ptr&>(logic_plan);
                    for (auto& col_def : n->column_definitions()) {
                        if (col_def.type().type() == logical_type::UNKNOWN &&
                            !(error = check_type_exists(resource(), catalog_, col_def.type().type_name()))
                                 .contains_error()) {
                            auto proper_type = catalog_.get_type(col_def.type().type_name());
                            std::string alias = col_def.type().alias();
                            col_def.type() = std::move(proper_type);
                            col_def.type().set_alias(alias);
                        }
                    }
                }
                break;
            case node_type::drop_collection_t:
                error = check_collection_exists(resource(), catalog_, id);
                break;
            case node_type::create_type_t: {
                auto& n = reinterpret_cast<node_create_type_ptr&>(logic_plan);
                if (auto res = check_type_exists(resource(), catalog_, n->type().type_name()); !res.contains_error()) {
                    error = core::error_t(
                        core::error_code_t::type_already_exists,
                        std::pmr::string{"type: \'" + n->type().alias() + "\' already exists", resource()});
                    break;
                } else {
                    if (n->type().type() == logical_type::STRUCT) {
                        for (auto& field : n->type().child_types()) {
                            if (field.type() == logical_type::UNKNOWN) {
                                error = check_type_exists(resource(), catalog_, field.type_name());
                                if (error.contains_error()) {
                                    break;
                                } else {
                                    std::string alias = field.alias();
                                    field = catalog_.get_type(field.type_name());
                                    field.set_alias(alias);
                                }
                            }
                        }
                        if (error.contains_error()) {
                            break;
                        }
                    }
                    catalog_.create_type(n->type());
                    // if we got here, then error is in 'no_error' state
                    co_return make_cursor(resource(), std::move(error));
                }
            }
            case node_type::drop_type_t: {
                const auto& n = boost::polymorphic_pointer_downcast<node_create_type_t>(logic_plan);
                error = check_type_exists(resource(), catalog_, n->type().alias());
                if (error.contains_error()) {
                    break;
                } else {
                    catalog_.drop_type(n->type().alias());
                    co_return make_cursor(resource(), std::move(error));
                }
            }
            case node_type::checkpoint_t:
            case node_type::vacuum_t:
            case node_type::create_sequence_t:
            case node_type::drop_sequence_t:
            case node_type::create_view_t:
            case node_type::drop_view_t:
            case node_type::create_macro_t:
            case node_type::drop_macro_t:
                break;
            default: {
                // Computing-schema expansion: replace `aggregate(t)` for any computing
                // table `t` with a subquery that LEFT JOINs side tables. Done before
                // validate so the validator sees the expanded plan with proper schema.
                // INSERT into computing tables is handled separately (case insert_t below)
                // and is not subject to this rewrite.
                if (logic_plan->type() != node_type::insert_t) {
                    logic_plan = components::planner::expand_computing_tables(resource(), logic_plan, catalog_);
                }
                error = validate_types(resource(), catalog_, logic_plan.get());
                if (!error.contains_error()) {
                    auto schema_res = validate_schema(resource(), catalog_, logic_plan.get(), params->parameters());
                    if (schema_res.has_error()) {
                        error = schema_res.error();
                    } else {
                        // Rebase outer-level expression paths through computing-subquery
                        // wrappers' projections. Validator resolves user-side keys against
                        // the JOIN-internal schema; my_select_node prunes that to user-fields,
                        // so the indices need translation before physical-plan generation.
                        components::planner::fixup_computing_paths(logic_plan);
                        // Post-validate optimization pass: column pruning, etc.
                        // Runs here because it needs paths resolved by the schema validator.
                        logic_plan = components::planner::post_validate_optimize(resource(), logic_plan, &catalog_);
                    }
                }
            }
        }

        if (error.contains_error()) {
            trace(log_, "manager_dispatcher_t::execute_plan: validation error");
            co_return make_cursor(resource(), std::move(error));
        }

        // DML transactions are now managed by executor (Phase 8C)
        components::table::transaction_data txn_data{0, 0};

        collection::executor::execute_result_t exec_result;
        switch (logic_plan->type()) {
            case node_type::create_database_t:
                exec_result = create_database_(logic_plan);
                break;
            case node_type::drop_database_t:
                exec_result = drop_database_(logic_plan);
                break;
            case node_type::create_collection_t:
                exec_result = create_collection_(logic_plan);
                break;
            case node_type::drop_collection_t:
                exec_result = drop_collection_(logic_plan);
                break;
            case node_type::checkpoint_t: {
                trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                // Flush all dirty index btrees before table checkpoint
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_fi, fif] =
                        actor_zeta::send(index_address_, &index::manager_index_t::flush_all_indexes, session);
                    co_await std::move(fif);
                }
                // Query WAL for current max ID before checkpoint
                services::wal::id_t wal_max_id{0};
                if (wal_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_wi, wif] =
                        actor_zeta::send(wal_address_, &wal::manager_wal_replicate_t::current_wal_id, session);
                    wal_max_id = co_await std::move(wif);
                }
                auto [_cp, cpf] =
                    actor_zeta::send(disk_address_, &disk::manager_disk_t::checkpoint_all, session, wal_max_id);
                auto checkpoint_wal_id = co_await std::move(cpf);
                // After checkpoint, trim old WAL segments (id=0 means no-op: IN_MEMORY tables need WAL)
                if (checkpoint_wal_id > 0 && wal_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_wt, wtf] = actor_zeta::send(wal_address_,
                                                       &wal::manager_wal_replicate_t::truncate_before,
                                                       session,
                                                       checkpoint_wal_id);
                    co_await std::move(wtf);
                }
                co_return make_cursor(resource(), std::move(error));
            }
            case node_type::vacuum_t: {
                trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                auto lowest = txn_manager_.lowest_active_start_time();
                auto [_v, vf] = actor_zeta::send(disk_address_, &disk::manager_disk_t::vacuum_all, session, lowest);
                co_await std::move(vf);
                // Cleanup old index versions + rebuild (compact changes row positions)
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_cv, cvf] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::cleanup_all_versions,
                                                       session,
                                                       lowest);
                    co_await std::move(cvf);
                    // Rebuild indexes for each collection (compact invalidates row positions)
                    for (const auto& coll : collections_) {
                        auto [_rb, rbf] =
                            actor_zeta::send(index_address_, &index::manager_index_t::rebuild_indexes, session, coll);
                        co_await std::move(rbf);
                        // Re-populate indexes from storage
                        auto [_tr, trf] =
                            actor_zeta::send(disk_address_, &disk::manager_disk_t::storage_total_rows, session, coll);
                        auto total = co_await std::move(trf);
                        if (total > 0) {
                            auto [_ss, ssf] = actor_zeta::send(disk_address_,
                                                               &disk::manager_disk_t::storage_scan_segment,
                                                               session,
                                                               coll,
                                                               int64_t{0},
                                                               total);
                            auto scan_chunks = co_await std::move(ssf);
                            uint64_t count = 0;
                            for (const auto& c : scan_chunks) {
                                count += c.size();
                            }
                            if (count > 0) {
                                auto [_ir, irf] = actor_zeta::send(index_address_,
                                                                   &index::manager_index_t::insert_rows,
                                                                   index::execution_context_t{session, txn_data, coll},
                                                                   std::move(scan_chunks),
                                                                   uint64_t{0},
                                                                   count);
                                co_await std::move(irf);
                            }
                        }
                    }
                }
                co_return make_cursor(resource(), std::move(error));
            }
            case node_type::create_sequence_t:
            case node_type::drop_sequence_t:
            case node_type::create_view_t:
            case node_type::drop_view_t:
            case node_type::create_macro_t:
            case node_type::drop_macro_t: {
                // DDL for sequences/views/macros — catalog-only, no storage needed
                exec_result = {make_cursor(resource(), std::move(error)), {}};
                break;
            }
            case node_type::insert_t: {
                if (catalog_.table_computes(id)) {
                    // Computing-schema INSERT (sparse model):
                    //   main table physically holds only `row_id BIGINT`.
                    //   Each user (field, type) pair lives in side table
                    //     `_dyn_<main>__<field>__<type_id>` with shape (row_id, value).
                    //   For a row that has no value for a given field we simply do not
                    //   insert into that side table — LEFT JOIN at SELECT time yields NULL.
                    //
                    // NOTE: this raund is non-atomic. Main + each side INSERT is its own
                    // transaction. A crash mid-batch leaves side tables partially filled.
                    auto& children = logic_plan->children();
                    if (!children.empty() && children.front()->type() == node_type::data_t) {
                        auto data_node = boost::static_pointer_cast<node_data_t>(children.front());
                        auto& user_chunk = data_node->data_chunk();
                        auto& schema = catalog_.get_computing_table_schema(id);
                        uint64_t row_count = user_chunk.size();
                        auto main_full = logic_plan->collection_full_name();
                        update_result_.clear();

                        // Step 1: ensure side collections exist + extend computed_schema.
                        std::pmr::vector<collection_full_name_t> side_names(resource());
                        side_names.reserve(user_chunk.data.size());
                        for (auto& col : user_chunk.data) {
                            auto field_name = col.type().alias();
                            auto field_type = col.type();
                            std::pmr::string pmr_field(field_name.c_str(), resource());

                            std::string side_name_str = computed_schema::side_table_name(
                                std::string_view(main_full.collection.data(), main_full.collection.size()),
                                field_name,
                                field_type);
                            collection_full_name_t side_full(main_full.database, side_name_str);
                            side_names.push_back(side_full);

                            if (collections_.count(side_full) == 0) {
                                // Inline `ensure_side_collection_`: GCC ABI requires coroutines
                                // be defined inline at the call site (out-of-line member
                                // coroutines mangle `this` and trip an actor-zeta assert).
                                //
                                // The value column carries `alias = field_name` so that subquery
                                // expansion can reference it as `_dyn_*.field_name` (no rename
                                // step needed at validation time).
                                std::vector<components::table::column_definition_t> side_cols;
                                complex_logical_type rid_def(logical_type::BIGINT);
                                rid_def.set_alias("row_id");
                                side_cols.emplace_back("row_id", rid_def, /*not_null=*/true, std::nullopt);
                                complex_logical_type val_def = field_type;
                                val_def.set_alias(std::string(field_name));
                                side_cols.emplace_back("value", val_def, /*not_null=*/true, std::nullopt);

                                {
                                    auto [_ac, acf] = actor_zeta::send(disk_address_,
                                                                       &disk::manager_disk_t::append_collection,
                                                                       session,
                                                                       side_full.database,
                                                                       side_full.collection);
                                    co_await std::move(acf);
                                }
                                {
                                    auto [_cs2, csf2] = actor_zeta::send(
                                        disk_address_,
                                        &disk::manager_disk_t::create_storage_with_columns,
                                        session,
                                        side_full,
                                        side_cols);
                                    co_await std::move(csf2);
                                }
                                if (index_address_ != actor_zeta::address_t::empty_address()) {
                                    auto [_ri, rif] = actor_zeta::send(index_address_,
                                                                       &index::manager_index_t::register_collection,
                                                                       session,
                                                                       side_full);
                                    co_await std::move(rif);
                                }
                                {
                                    auto [_fc, fcf] = actor_zeta::send(disk_address_,
                                                                       &disk::manager_disk_t::flush,
                                                                       session,
                                                                       wal::id_t{0});
                                    co_await std::move(fcf);
                                }
                                // Register in catalog as a regular table.
                                {
                                    std::vector<components::table::column_definition_t> sch_cols = side_cols;
                                    std::vector<field_description> descs;
                                    descs.reserve(sch_cols.size());
                                    for (size_t i = 0; i < sch_cols.size(); ++i) {
                                        descs.emplace_back(static_cast<field_id_t>(i));
                                    }
                                    table_id sid(resource(), side_full);
                                    auto sch = components::catalog::schema(
                                        resource(), std::move(sch_cols), std::move(descs));
                                    auto err = catalog_.create_table(sid,
                                                                     table_metadata(resource(), std::move(sch)));
                                    (void)err;
                                }
                                collections_.insert(side_full);
                            }

                            schema.append(pmr_field, field_type);
                            col.set_type_alias(std::string(field_name));
                            update_result_[{std::pmr::string(field_name.c_str(), resource()), field_type}] +=
                                row_count;
                        }

                        // Step 2: allocate contiguous row_ids from main's current row count.
                        // Concurrency note: hash-routing pins one collection to one executor,
                        // and dispatcher serializes its own messages under mutex_, so the
                        // total_rows snapshot is stable for the duration of this case.
                        uint64_t next_row_id = 0;
                        {
                            auto [_tr, trf] = actor_zeta::send(disk_address_,
                                                               &disk::manager_disk_t::storage_total_rows,
                                                               session,
                                                               main_full);
                            next_row_id = co_await std::move(trf);
                        }

                        // Step 3: per side, build (row_id, value) chunk with non-null rows only,
                        //         and insert.
                        for (size_t col_idx = 0; col_idx < user_chunk.data.size(); ++col_idx) {
                            auto& src_col = user_chunk.data[col_idx];
                            auto field_type = src_col.type();
                            const auto& side_full = side_names[col_idx];

                            // Count non-null rows up front to allocate exact capacity.
                            uint64_t non_null = 0;
                            for (uint64_t r = 0; r < row_count; ++r) {
                                if (!src_col.is_null(r)) {
                                    ++non_null;
                                }
                            }
                            if (non_null == 0) {
                                continue;
                            }

                            std::pmr::vector<complex_logical_type> side_types(resource());
                            complex_logical_type rid_t(logical_type::BIGINT);
                            rid_t.set_alias("row_id");
                            side_types.emplace_back(rid_t);
                            complex_logical_type val_t = field_type;
                            val_t.set_alias("value");
                            side_types.emplace_back(val_t);

                            components::vector::data_chunk_t side_chunk(resource(), side_types, non_null);
                            side_chunk.set_cardinality(non_null);
                            uint64_t out_idx = 0;
                            for (uint64_t r = 0; r < row_count; ++r) {
                                if (src_col.is_null(r)) {
                                    continue;
                                }
                                side_chunk.set_value(0,
                                                     out_idx,
                                                     logical_value_t(resource(), static_cast<int64_t>(next_row_id + r)));
                                side_chunk.set_value(1, out_idx, src_col.value(r));
                                ++out_idx;
                            }

                            // Inline insert into side collection (must be inline for GCC coroutine ABI).
                            {
                                auto side_plan = make_node_insert(resource(), side_full, std::move(side_chunk));
                                components::table::transaction_data sub_txn{0, 0};
                                components::logical_plan::storage_parameters sub_params(resource());
                                auto sub_fut = execute_plan_impl(session,
                                                                 side_plan,
                                                                 std::move(sub_params),
                                                                 sub_txn);
                                auto sub_res = co_await std::move(sub_fut);
                                if (!sub_res.cursor || sub_res.cursor->is_error()) {
                                    trace(log_,
                                          "computing INSERT: side {}.{} failed",
                                          side_full.database,
                                          side_full.collection);
                                }
                            }
                        }

                        // Step 4: replace child data node so that the main INSERT carries only row_ids.
                        std::pmr::vector<complex_logical_type> main_types(resource());
                        complex_logical_type rid_main(logical_type::BIGINT);
                        rid_main.set_alias("row_id");
                        main_types.emplace_back(rid_main);
                        components::vector::data_chunk_t main_chunk(resource(), main_types, row_count);
                        main_chunk.set_cardinality(row_count);
                        for (uint64_t r = 0; r < row_count; ++r) {
                            main_chunk.set_value(0,
                                                 r,
                                                 logical_value_t(resource(), static_cast<int64_t>(next_row_id + r)));
                        }

                        // Rebuild the insert plan around the new main_chunk.
                        auto new_insert = make_node_insert(resource(), main_full, std::move(main_chunk));
                        logic_plan = new_insert;
                    }
                }
                exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                break;
            }
            case node_type::delete_t: {
                if (catalog_.table_computes(id)) {
                    // Computing-schema DELETE strategy:
                    //   1. SELECT row_id FROM <expanded virtual t> WHERE <user predicate>
                    //   2. For each affected row_id: DELETE from main + each side using
                    //      a `row_id IN (...)` compound match.
                    // Non-atomic (each phase is its own transaction). After step (2) on
                    // main, SELECT * already won't see the row (LEFT JOIN drops side rows
                    // whose row_id is missing in main); subsequent side-deletes are GC.
                    auto del_node = boost::static_pointer_cast<node_delete_t>(logic_plan);
                    auto main_full = del_node->collection_full_name();

                    // Find original WHERE expression from match child.
                    components::expressions::expression_ptr where_expr;
                    for (auto& child : del_node->children()) {
                        if (child->type() == node_type::match_t && !child->expressions().empty()) {
                            where_expr = child->expressions()[0];
                            break;
                        }
                    }

                    // Detect "DELETE without WHERE" (transformer maps to all_true match).
                    bool delete_all = false;
                    if (where_expr &&
                        where_expr->group() == components::expressions::expression_group::compare) {
                        auto* cmp =
                            static_cast<components::expressions::compare_expression_t*>(where_expr.get());
                        if (cmp->type() == components::expressions::compare_type::all_true) {
                            delete_all = true;
                        }
                    }

                    // === Step 1: build & validate SELECT row_id FROM t [WHERE expr] ===
                    auto sel_plan = components::planner::build_select_row_ids(
                        resource(),
                        catalog_,
                        main_full,
                        delete_all ? components::expressions::expression_ptr{} : where_expr);
                    {
                        auto verror = validate_types(resource(), catalog_, sel_plan.get());
                        if (verror.contains_error()) {
                            exec_result = {make_cursor(resource(), std::move(verror)), {}};
                            break;
                        }
                        auto schema_res =
                            validate_schema(resource(), catalog_, sel_plan.get(), params->parameters());
                        if (schema_res.has_error()) {
                            exec_result = {make_cursor(resource(), schema_res.error()), {}};
                            break;
                        }
                    }
                    components::planner::fixup_computing_paths(sel_plan);
                    // Skipping post_validate_optimize (column_pruning) — it may incorrectly
                    // prune columns from our hand-built JOIN chain.

                    // === Step 2: execute SELECT, collect row_ids ===
                    components::logical_plan::storage_parameters sel_params(resource());
                    for (const auto& [pid, val] : params->parameters().parameters) {
                        sel_params.parameters.emplace(pid, val);
                    }
                    auto sel_result = co_await execute_plan_impl(session,
                                                                  sel_plan,
                                                                  std::move(sel_params),
                                                                  txn_data);
                    if (!sel_result.cursor || sel_result.cursor->is_error()) {
                        exec_result = std::move(sel_result);
                        break;
                    }

                    std::vector<int64_t> row_ids_to_delete;
                    if (sel_result.cursor) {
                        auto n = sel_result.cursor->size();
                        row_ids_to_delete.reserve(n);
                        for (std::size_t r = 0; r < n; ++r) {
                            row_ids_to_delete.push_back(
                                sel_result.cursor->value(0, static_cast<uint64_t>(r)).value<int64_t>());
                        }
                    }

                    // No matches → nothing to delete (success no-op).
                    if (row_ids_to_delete.empty()) {
                        update_result_.clear();
                        exec_result = {make_cursor(resource(), core::error_t::no_error()), {}};
                        break;
                    }

                    // === Step 3: DELETE FROM main + each side ===
                    // For simplicity (and because compound OR with parameters has edge cases
                    // I'd rather avoid for MVP), issue ONE delete per (target, row_id) pair.
                    // O(N * (1 + sides)) DELETE statements — slow on big batches but correct.
                    std::vector<collection_full_name_t> targets;
                    targets.push_back(main_full);
                    {
                        const auto& cs = catalog_.get_computing_table_schema(id);
                        auto sch = cs.latest_types_struct();
                        for (const auto& f : sch.child_types()) {
                            std::string side_name = computed_schema::side_table_name(
                                std::string{main_full.collection}, f.alias(), f);
                            targets.emplace_back(main_full.database, side_name);
                        }
                    }

                    // row_id key with path explicitly set: row_id is column 0 in both
                    // main (only column) and every side (first column of (row_id, value)).
                    // Validator for DELETE on computing main returns latest_types_struct
                    // (without row_id), so we pre-resolve the path here and skip validation.
                    auto make_row_id_key = [&]() {
                        components::expressions::key_t k(resource(), "row_id");
                        std::pmr::vector<size_t> p(resource());
                        p.push_back(0);
                        k.set_path(std::move(p));
                        return k;
                    };

                    bool any_error = false;
                    for (int64_t rid : row_ids_to_delete) {
                        // One parameter_node per row: contains a single param holding rid.
                        auto pn = make_parameter_node(resource());
                        auto pid = pn->add_parameter(
                            components::types::logical_value_t(resource(), rid));
                        for (const auto& target : targets) {
                            auto cmp = make_compare_expression(
                                resource(),
                                components::expressions::compare_type::eq,
                                components::expressions::param_storage{make_row_id_key()},
                                components::expressions::param_storage{pid});
                            auto m = make_node_match(resource(), target, cmp);
                            node_ptr del = make_node_delete_many(resource(), target, m);
                            components::logical_plan::storage_parameters sub_params(resource());
                            for (const auto& [p2, val] : pn->parameters().parameters) {
                                sub_params.parameters.emplace(p2, val);
                            }
                            auto sub_res = co_await execute_plan_impl(session,
                                                                       del,
                                                                       std::move(sub_params),
                                                                       txn_data);
                            if (!sub_res.cursor || sub_res.cursor->is_error()) {
                                trace(log_,
                                      "computing DELETE: failed for {}.{} row_id={}",
                                      target.database,
                                      target.collection,
                                      rid);
                                any_error = true;
                            }
                        }
                    }

                    update_result_.clear();
                    exec_result = {make_cursor(resource(),
                                               any_error
                                                   ? core::error_t(core::error_code_t::other_error,
                                                                   std::pmr::string{"computing DELETE: partial failure",
                                                                                    resource()})
                                                   : core::error_t::no_error()),
                                   {}};
                    break;
                }
                exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                break;
            }
            default:
                exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                break;
        }

        auto& result = exec_result.cursor;
        trace(log_, "manager_dispatcher_t::execute_plan: result received, success: {}", result->is_success());

        // For computed-schema INSERT, update_result_ was pre-populated in the insert_t case;
        // don't overwrite it. For other DML (DELETE), exec_result.updates carries the data.
        if (!exec_result.updates.empty() && logic_plan->type() != node_type::insert_t) {
            update_result_ = exec_result.updates;
        }

        if (result->is_success()) {
            switch (logic_plan->type()) {
                case node_type::create_database_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                    auto [_d1, df1] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::append_database,
                                                       session,
                                                       logic_plan->database_name());
                    co_await std::move(df1);
                    auto [_fd, fdf] =
                        actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal::id_t{0});
                    co_await std::move(fdf);
                    update_catalog(logic_plan);
                    co_return result;
                }

                case node_type::drop_database_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                    auto db_name = logic_plan->database_name();
                    // Drop all collections in this database from index + disk
                    for (const auto& coll : collections_) {
                        if (coll.database == db_name) {
                            if (index_address_ != actor_zeta::address_t::empty_address()) {
                                auto [_ui, uif] = actor_zeta::send(index_address_,
                                                                   &index::manager_index_t::unregister_collection,
                                                                   session,
                                                                   coll);
                                co_await std::move(uif);
                            }
                            auto [_ds, dsf] =
                                actor_zeta::send(disk_address_, &disk::manager_disk_t::drop_storage, session, coll);
                            co_await std::move(dsf);
                            auto [_dr, drf] = actor_zeta::send(disk_address_,
                                                               &disk::manager_disk_t::remove_collection,
                                                               session,
                                                               coll.database,
                                                               coll.collection);
                            co_await std::move(drf);
                        }
                    }
                    // Remove database from disk metadata
                    auto [_rd, rdf] =
                        actor_zeta::send(disk_address_, &disk::manager_disk_t::remove_database, session, db_name);
                    co_await std::move(rdf);
                    auto [_fdd, fddf] =
                        actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal::id_t{0});
                    co_await std::move(fddf);
                    update_catalog(logic_plan);
                    co_return result;
                }

                case node_type::create_collection_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                    auto [_c1, cf1] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::append_collection,
                                                       session,
                                                       logic_plan->database_name(),
                                                       logic_plan->collection_name());
                    co_await std::move(cf1);
                    auto create_collection = boost::static_pointer_cast<node_create_collection_t>(logic_plan);
                    // Create storage in manager_disk_t
                    if (create_collection->column_definitions().empty()) {
                        // Computing (dynamic-schema) main table now physically has a single
                        // row_id BIGINT NOT NULL column. User fields go to side tables `_dyn_*`
                        // and are joined back at SELECT time.
                        std::vector<components::table::column_definition_t> row_id_cols;
                        components::types::complex_logical_type row_id_type(
                            components::types::logical_type::BIGINT);
                        row_id_type.set_alias("row_id");
                        row_id_cols.emplace_back("row_id", row_id_type, /*not_null=*/true, std::nullopt);
                        auto [_cs, csf] = actor_zeta::send(disk_address_,
                                                           &disk::manager_disk_t::create_storage_with_columns,
                                                           session,
                                                           logic_plan->collection_full_name(),
                                                           std::move(row_id_cols));
                        co_await std::move(csf);
                    } else {
                        std::vector<components::table::column_definition_t> storage_columns =
                            create_collection->column_definitions();
                        // Resolve UDT references (UNKNOWN types) in storage columns
                        for (auto& col : storage_columns) {
                            auto& col_type = col.type();
                            if (col_type.type() == logical_type::UNKNOWN &&
                                !check_type_exists(resource_, catalog_, col_type.type_name()).contains_error()) {
                                auto proper = catalog_.get_type(col_type.type_name());
                                std::string alias = col_type.alias();
                                col_type = std::move(proper);
                                col_type.set_alias(alias);
                            }
                            if (col_type.type() == logical_type::STRUCT) {
                                for (auto& field : col_type.child_types()) {
                                    if (field.type() == logical_type::UNKNOWN &&
                                        !check_type_exists(resource_, catalog_, field.type_name()).contains_error()) {
                                        auto proper = catalog_.get_type(field.type_name());
                                        std::string fa = field.alias();
                                        field = std::move(proper);
                                        field.set_alias(fa);
                                    }
                                }
                            }
                        }
                        if (create_collection->is_disk_storage()) {
                            auto [_cs, csf] = actor_zeta::send(disk_address_,
                                                               &disk::manager_disk_t::create_storage_disk,
                                                               session,
                                                               logic_plan->collection_full_name(),
                                                               std::move(storage_columns));
                            co_await std::move(csf);
                        } else {
                            auto [_cs, csf] = actor_zeta::send(disk_address_,
                                                               &disk::manager_disk_t::create_storage_with_columns,
                                                               session,
                                                               logic_plan->collection_full_name(),
                                                               std::move(storage_columns));
                            co_await std::move(csf);
                        }
                    }
                    // Register collection in manager_index_t
                    if (index_address_ != actor_zeta::address_t::empty_address()) {
                        auto [_ri, rif] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::register_collection,
                                                           session,
                                                           logic_plan->collection_full_name());
                        co_await std::move(rif);
                    }
                    {
                        auto [_fc, fcf] =
                            actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal::id_t{0});
                        co_await std::move(fcf);
                    }
                    update_catalog(logic_plan);
                    co_return result;
                }

                case node_type::insert_t:
                case node_type::update_t:
                case node_type::delete_t: {
                    // Executor owns full DML lifecycle (begin/commit/abort + WAL + side-effects)
                    trace(log_,
                          "manager_dispatcher_t::execute_plan: DML {} completed by executor",
                          to_string(logic_plan->type()));
                    update_catalog(logic_plan);
                    co_return result;
                }

                case node_type::drop_collection_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                    // Unregister collection from manager_index_t
                    if (index_address_ != actor_zeta::address_t::empty_address()) {
                        auto [_ui, uif] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::unregister_collection,
                                                           session,
                                                           logic_plan->collection_full_name());
                        co_await std::move(uif);
                    }
                    // Drop storage from manager_disk_t
                    auto [_ds, dsf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::drop_storage,
                                                       session,
                                                       logic_plan->collection_full_name());
                    co_await std::move(dsf);
                    auto [_dr1, drf1] = actor_zeta::send(disk_address_,
                                                         &disk::manager_disk_t::remove_collection,
                                                         session,
                                                         logic_plan->database_name(),
                                                         logic_plan->collection_name());
                    co_await std::move(drf1);
                    {
                        auto [_fdc, fdcf] =
                            actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal::id_t{0});
                        co_await std::move(fdcf);
                    }
                    update_catalog(logic_plan);
                    co_return result;
                }

                case node_type::create_index_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                    co_return result;
                }

                case node_type::drop_index_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                    co_return result;
                }

                case node_type::create_sequence_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: create_sequence");
                    auto seq_node = boost::static_pointer_cast<node_create_sequence_t>(logic_plan);
                    disk::catalog_sequence_entry_t seq_entry;
                    seq_entry.name = seq_node->collection_name();
                    seq_entry.start_value = seq_node->start();
                    seq_entry.increment = seq_node->increment();
                    seq_entry.current_value = seq_node->start();
                    seq_entry.min_value = seq_node->min_value();
                    seq_entry.max_value = seq_node->max_value();
                    auto [_s1, sf1] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::catalog_append_sequence,
                                                       session,
                                                       seq_node->database_name(),
                                                       std::move(seq_entry));
                    co_await std::move(sf1);
                    co_return result;
                }
                case node_type::drop_sequence_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: drop_sequence");
                    auto [_s2, sf2] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::catalog_remove_sequence,
                                                       session,
                                                       logic_plan->database_name(),
                                                       std::string(logic_plan->collection_name()));
                    co_await std::move(sf2);
                    co_return result;
                }
                case node_type::create_view_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: create_view");
                    auto view_node = boost::static_pointer_cast<node_create_view_t>(logic_plan);
                    disk::catalog_view_entry_t view_entry;
                    view_entry.name = view_node->collection_name();
                    view_entry.query_sql = view_node->query_sql();
                    auto [_v1, vf1] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::catalog_append_view,
                                                       session,
                                                       view_node->database_name(),
                                                       std::move(view_entry));
                    co_await std::move(vf1);
                    co_return result;
                }
                case node_type::drop_view_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: drop_view");
                    auto [_v2, vf2] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::catalog_remove_view,
                                                       session,
                                                       logic_plan->database_name(),
                                                       std::string(logic_plan->collection_name()));
                    co_await std::move(vf2);
                    co_return result;
                }
                case node_type::create_macro_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: create_macro");
                    auto macro_node = boost::static_pointer_cast<node_create_macro_t>(logic_plan);
                    disk::catalog_macro_entry_t macro_entry;
                    macro_entry.name = macro_node->collection_name();
                    macro_entry.parameters = macro_node->parameters();
                    macro_entry.body_sql = macro_node->body_sql();
                    auto [_m1, mf1] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::catalog_append_macro,
                                                       session,
                                                       macro_node->database_name(),
                                                       std::move(macro_entry));
                    co_await std::move(mf1);
                    co_return result;
                }
                case node_type::drop_macro_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: drop_macro");
                    auto [_m2, mf2] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::catalog_remove_macro,
                                                       session,
                                                       logic_plan->database_name(),
                                                       std::string(logic_plan->collection_name()));
                    co_await std::move(mf2);
                    co_return result;
                }

                default: {
                    trace(log_,
                          "manager_dispatcher_t::execute_plan: non processed type - {}",
                          to_string(logic_plan->type()));
                }
            }
        } else {
            // Executor handles abort + revert for DML errors
            trace(log_, "manager_dispatcher_t::execute_plan: error: \"{}\"", result->get_error().what);
        }

        co_return std::move(result);
    }

    manager_dispatcher_t::unique_future<std::unique_ptr<core::error_t>>
    manager_dispatcher_t::register_udf(components::session::session_id_t session,
                                       components::compute::function_ptr function) {
        trace(log_, "dispatcher_t::register_udf session: {}, function name: {}", session.data(), function->name());
        auto func_name = function->name();
        auto func_signatures = function->get_signatures();
        // TODO: return error code of why there is conflict
        if (!catalog_.check_function_conflicts(func_name, func_signatures)) {
            co_return std::make_unique<core::error_t>(
                core::error_code_t::function_registry_error,
                std::pmr::string{"there was a conflict, while trying to register a function", resource()});
        } else {
            // we have to send it to all executors and validate, that results are the same...
            std::pmr::vector<collection::executor::function_result_t> results(resource_);
            results.reserve(executor_pool_size_);
            for (size_t i = 0; i < executor_pool_size_; i++) {
                auto [needs_sched, future] =
                    actor_zeta::otterbrix::send(executor_addresses_[i],
                                                &collection::executor::executor_t::register_udf,
                                                session,
                                                function->get_copy(resource()));
                if (needs_sched && executors_[i]) {
                    scheduler_->enqueue(executors_[i].get());
                }
                results.emplace_back(std::move(*co_await std::move(future)));
            }
            // TODO: if executors return different uids once, they continue to disagree and any call to register_udf will fail
            if (!results.front().has_error() &&
                std::all_of(results.begin() + 1,
                            results.end(),
                            [first_uid = results.front().value()](const collection::executor::function_result_t& uid) {
                                return !uid.has_error() && uid.value() == first_uid;
                            })) {
                catalog_.create_function(func_name, {results.front().value(), std::move(func_signatures)});
                co_return std::make_unique<core::error_t>(core::error_t::no_error());
            } else {
                co_return std::make_unique<core::error_t>(
                    core::error_code_t::function_registry_error,
                    std::pmr::string{"executors disagree on assigned uid, currently the only way "
                                     "to fix this is a reboot, but we are working on it",
                                     resource()});
            }
        }
    }

    manager_dispatcher_t::unique_future<std::unique_ptr<core::error_t>>
    manager_dispatcher_t::unregister_udf(components::session::session_id_t session,
                                         std::string function_name,
                                         std::pmr::vector<complex_logical_type> inputs) {
        trace(log_, "dispatcher_t::unregister_udf: session {}, {}", session.data(), function_name);
        if (catalog_.function_exists(function_name, inputs)) {
            catalog_.drop_function(function_name, inputs);
            co_return std::make_unique<core::error_t>(core::error_t::no_error());
        }
        co_return std::make_unique<core::error_t>(core::error_code_t::function_registry_error,
                                                  std::pmr::string{"could not find the function", resource()});
    }

    manager_dispatcher_t::unique_future<size_t> manager_dispatcher_t::size(components::session::session_id_t session,
                                                                           std::string database_name,
                                                                           std::string collection) {
        trace(log_,
              "manager_dispatcher_t::size session:{}, database: {}, collection: {}",
              session.data(),
              database_name,
              collection);

        auto error = check_collection_exists(resource(), catalog_, {resource(), {database_name, collection}});
        // TODO: return error
        if (error.contains_error()) {
            co_return size_t(0);
        }

        collection_full_name_t name{database_name, collection};
        if (collections_.find(name) == collections_.end()) {
            co_return size_t(0);
        }
        // Get size from storage in manager_disk_t
        auto [_s, sf] = actor_zeta::send(disk_address_,
                                         &disk::manager_disk_t::storage_calculate_size,
                                         components::session::session_id_t{},
                                         name);
        auto sz = co_await std::move(sf);
        co_return static_cast<size_t>(sz);
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::get_schema(components::session::session_id_t session,
                                     std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids) {
        trace(log_, "manager_dispatcher_t::get_schema session: {}, ids count: {}", session.data(), ids.size());
        std::pmr::vector<complex_logical_type> schemas;
        schemas.reserve(ids.size());

        for (const auto& [db, coll] : ids) {
            table_id id(resource(), {db, coll});
            if (catalog_.table_exists(id)) {
                const auto& schema = catalog_.get_table_schema(id);
                schemas.push_back(create_struct("schema", schema.types(), schema.descriptions()));
                continue;
            }
            if (catalog_.table_computes(id)) {
                schemas.push_back(catalog_.get_computing_table_schema(id).latest_types_struct());
                continue;
            }
            schemas.push_back(logical_type::INVALID);
        }

        co_return make_cursor(resource(), std::move(schemas));
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::close_cursor(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::close_cursor, session: {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            cursor_.erase(it);
        }
        co_return;
    }

    collection::executor::execute_result_t manager_dispatcher_t::create_database_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:create_database {}", logical_plan->database_name());
        databases_.insert(logical_plan->database_name());
        return {make_cursor(resource(), core::error_t::no_error()), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::drop_database_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:drop_database {}", logical_plan->database_name());
        auto db_name = logical_plan->database_name();
        for (auto it = collections_.begin(); it != collections_.end();) {
            if (it->database == db_name) {
                it = collections_.erase(it);
            } else {
                ++it;
            }
        }
        databases_.erase(db_name);
        return {make_cursor(resource(), core::error_t::no_error()), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::create_collection_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:create_collection {}", logical_plan->collection_full_name().to_string());
        collections_.insert(logical_plan->collection_full_name());
        return {make_cursor(resource(), core::error_t::no_error()), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::drop_collection_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:drop_collection {}", logical_plan->collection_full_name().to_string());
        collections_.erase(logical_plan->collection_full_name());
        return {make_cursor(resource(), core::error_t::no_error()), {}};
    }

    manager_dispatcher_t::unique_future<collection::executor::execute_result_t>
    manager_dispatcher_t::execute_plan_impl(components::session::session_id_t session,
                                            node_ptr logical_plan,
                                            storage_parameters parameters,
                                            components::table::transaction_data txn) {
        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: collection: {}, session: {}",
              logical_plan->collection_full_name().to_string(),
              session.data());

        auto dependency_tree_collections_names = logical_plan->collection_dependencies();
        context_storage_t collections_context_storage(resource(), log_.clone());
        for (auto& name : dependency_tree_collections_names) {
            if (!name.empty() && collections_.count(name) > 0) {
                collections_context_storage.known_collections.insert(name);
            }
        }

        // Populate index metadata for optimizer-driven index selection
        if (index_address_ != actor_zeta::address_t::empty_address()) {
            auto coll = logical_plan->collection_full_name();
            if (!coll.empty()) {
                auto [_ik, ikf] =
                    actor_zeta::send(index_address_, &index::manager_index_t::get_indexed_keys, session, coll);
                collections_context_storage.indexed_keys = co_await std::move(ikf);
            }
        }
        collections_context_storage.parameters = &parameters;

        assert(!executors_.empty());
        auto pool_idx = collection_name_hash{}(logical_plan->collection_full_name()) % executors_.size();
        trace(log_, "manager_dispatcher_t:execute_plan_impl: calling executor[{}]", pool_idx);
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 &collection::executor::executor_t::execute_plan,
                                                                 session,
                                                                 logical_plan,
                                                                 parameters,
                                                                 std::move(collections_context_storage),
                                                                 txn);
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto result = co_await std::move(future);

        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: executor returned, success: {}",
              result.cursor->is_success());
        co_return result;
    }

    manager_dispatcher_t::unique_future<components::table::transaction_data>
    manager_dispatcher_t::begin_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::begin_transaction, session: {}", session.data());
        auto& txn = txn_manager_.begin_transaction(session);
        co_return txn.data();
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::commit_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::commit_transaction, session: {}", session.data());
        co_return txn_manager_.commit(session);
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::abort_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::abort_transaction, session: {}", session.data());
        txn_manager_.abort(session);
        co_return;
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::lowest_active_start_time(components::session::session_id_t /*session*/) {
        co_return txn_manager_.lowest_active_start_time();
    }

    node_ptr manager_dispatcher_t::create_logic_plan(node_ptr plan) {
        components::planner::planner_t planner;
        return planner.create_plan(resource(), std::move(plan), &catalog_);
    }

    void manager_dispatcher_t::update_catalog(node_ptr node) {
        table_id id(resource(), node->collection_full_name());
        switch (node->type()) {
            case node_type::create_database_t:
                catalog_.create_namespace(id.get_namespace());
                break;
            case node_type::drop_database_t:
                catalog_.drop_namespace(id.get_namespace());
                break;
            case node_type::create_collection_t: {
                auto node_info = boost::polymorphic_pointer_downcast<node_create_collection_t>(node);
                if (node_info->column_definitions().empty()) {
                    auto err = catalog_.create_computing_table(id);
                    assert(!err.contains_error());
                } else {
                    auto types = node_info->schema();
                    std::vector<field_description> desc;
                    desc.reserve(types.size());
                    for (size_t i = 0; i < types.size(); desc.push_back(field_description(i++))) {
                    }

                    auto sch = schema(resource(), node_info->column_definitions(), std::move(desc));
                    auto err = catalog_.create_table(id, table_metadata(resource(), std::move(sch)));
                    assert(!err.contains_error());
                }
                break;
            }
            case node_type::drop_collection_t:
                if (catalog_.table_exists(id)) {
                    catalog_.drop_table(id);
                } else {
                    catalog_.drop_computing_table(id);
                }
                break;
            case node_type::insert_t:
                break;
            case node_type::delete_t:
                update_result_.clear();
                break;
            default:
                break;
        }
    }

} // namespace services::dispatcher
