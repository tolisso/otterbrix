#include "computed_schema.hpp"

namespace components::catalog {
    computed_schema::computed_schema(std::pmr::memory_resource* resource)
        : fields_(resource)
        , column_order_(resource)
        , counts_(resource)
        , in_main_(resource) {}

    void computed_schema::append(std::pmr::string field_name, const types::complex_logical_type& type) {
        append_n(std::move(field_name), type, 0);
    }

    void computed_schema::append_n(std::pmr::string field_name,
                                   const types::complex_logical_type& type,
                                   size_t n) {
        auto& list = fields_[field_name];
        auto& cnt_list = counts_[field_name];
        auto& main_list = in_main_[field_name];
        for (size_t i = 0; i < list.size(); ++i) {
            if (list[i] == type) {
                cnt_list[i] += n;
                return;
            }
        }
        list.emplace_back(type);
        cnt_list.push_back(n);
        main_list.push_back(false);
        column_order_.emplace_back(field_name, type);
    }

    void computed_schema::drop(std::pmr::string field_name, const types::complex_logical_type& type) {
        drop_n(std::move(field_name), type, 0);
    }

    void computed_schema::drop_n(std::pmr::string field_name,
                                  const types::complex_logical_type& type,
                                  size_t /*n*/) {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return;
        }
        auto& list = it->second;
        auto& cnt_list = counts_[field_name];
        auto& main_list = in_main_[field_name];
        for (size_t i = 0; i < list.size(); ++i) {
            if (list[i] == type) {
                list.erase(list.begin() + static_cast<std::ptrdiff_t>(i));
                cnt_list.erase(cnt_list.begin() + static_cast<std::ptrdiff_t>(i));
                main_list.erase(main_list.begin() + static_cast<std::ptrdiff_t>(i));
                break;
            }
        }
        if (list.empty()) {
            fields_.erase(it);
            counts_.erase(field_name);
            in_main_.erase(field_name);
        }
    }

    std::vector<types::complex_logical_type>
    computed_schema::find_field_versions(const std::pmr::string& field_name) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return {};
        }
        return {it->second.begin(), it->second.end()};
    }

    types::complex_logical_type computed_schema::latest_types_struct() const {
        std::vector<types::complex_logical_type> retval;
        retval.reserve(column_order_.size());
        for (const auto& [name, type] : column_order_) {
            if (!has_type(name, type)) {
                continue;
            }
            auto t = type;
            t.set_alias(name.c_str());
            retval.push_back(std::move(t));
        }
        return types::complex_logical_type::create_struct("latest_types", std::move(retval));
    }

    std::string computed_schema::storage_column_name(const std::string& field_name,
                                                      const types::complex_logical_type& type) {
        return "__" + field_name + "__" + std::to_string(static_cast<unsigned>(type.type()));
    }

    bool computed_schema::has_type(const std::pmr::string& field_name,
                                    const types::complex_logical_type& type) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return false;
        }
        for (const auto& t : it->second) {
            if (t == type) {
                return true;
            }
        }
        return false;
    }

    size_t computed_schema::get_count(const std::pmr::string& field_name,
                                      const types::complex_logical_type& type) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return 0;
        }
        const auto& list = it->second;
        auto cnt_it = counts_.find(field_name);
        if (cnt_it == counts_.end()) {
            return 0;
        }
        const auto& cnt_list = cnt_it->second;
        for (size_t i = 0; i < list.size(); ++i) {
            if (list[i] == type) {
                return cnt_list[i];
            }
        }
        return 0;
    }

    bool computed_schema::is_sparse(const std::pmr::string& field_name,
                                     const types::complex_logical_type& type) const {
        if (sparse_threshold_ == 0) {
            return false;
        }
        return get_count(field_name, type) < sparse_threshold_;
    }

    uint64_t computed_schema::alloc_rowids(size_t n) {
        uint64_t start = next_rowid_;
        next_rowid_ += n;
        return start;
    }

    bool computed_schema::is_in_main(const std::pmr::string& field_name,
                                     const types::complex_logical_type& type) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return false;
        }
        const auto& list = it->second;
        auto main_it = in_main_.find(field_name);
        if (main_it == in_main_.end()) {
            return false;
        }
        const auto& main_list = main_it->second;
        for (size_t i = 0; i < list.size(); ++i) {
            if (list[i] == type) {
                return main_list[i];
            }
        }
        return false;
    }

    void computed_schema::set_in_main(const std::pmr::string& field_name,
                                      const types::complex_logical_type& type) {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return;
        }
        auto& list = it->second;
        auto& main_list = in_main_[field_name];
        for (size_t i = 0; i < list.size(); ++i) {
            if (list[i] == type) {
                main_list[i] = true;
                return;
            }
        }
    }
} // namespace components::catalog
