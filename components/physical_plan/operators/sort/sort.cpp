#include "sort.hpp"

namespace components::sort {

    columnar_sorter_t::columnar_sorter_t(size_t index, order order_) { add(index, order_); }

    void columnar_sorter_t::add(size_t index, order order_) {
        std::pmr::vector<size_t> path;
        path.push_back(index);
        keys_.push_back({std::move(path), order_, nullptr});
    }

    void columnar_sorter_t::add(const std::pmr::vector<size_t>& col_path, order order_) {
        assert(!col_path.empty());
        keys_.push_back({col_path, order_, nullptr});
    }

    void columnar_sorter_t::set_chunk(const vector::data_chunk_t& chunk) {
        chunk_ = &chunk;
        multi_chunks_.clear();
        chunk_start_rows_.clear();
        for (auto& k : keys_) {
            assert(!k.col_path.empty());
            k.vec = chunk.at(k.col_path);
        }
    }

    void columnar_sorter_t::set_chunks(const chunk_list& chunks) {
        chunk_ = nullptr;
        multi_chunks_ = chunks;
        chunk_start_rows_.clear();
        chunk_start_rows_.reserve(chunks.size());
        uint64_t offset = 0;
        for (const auto* c : chunks) {
            chunk_start_rows_.push_back(offset);
            offset += c->size();
        }
        // Invalidate single-chunk cached vec pointers
        for (auto& k : keys_) {
            k.vec = nullptr;
        }
    }

    std::pair<size_t, size_t> columnar_sorter_t::locate(size_t global_idx) const {
        // Binary search: find the chunk whose start_row <= global_idx
        size_t lo = 0, hi = chunk_start_rows_.size();
        while (lo + 1 < hi) {
            size_t mid = (lo + hi) / 2;
            if (chunk_start_rows_[mid] <= global_idx) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return {lo, global_idx - static_cast<size_t>(chunk_start_rows_[lo])};
    }

    bool columnar_sorter_t::operator()(size_t row_a, size_t row_b) const {
        if (!multi_chunks_.empty()) {
            auto [ca, la] = locate(row_a);
            auto [cb, lb] = locate(row_b);
            for (const auto& k : keys_) {
                const auto* vec_a = multi_chunks_[ca]->at(k.col_path);
                const auto* vec_b = multi_chunks_[cb]->at(k.col_path);
                if (!vec_a || !vec_b) continue;
                int cmp;
                if (ca == cb) {
                    cmp = compare_raw(*vec_a, la, lb);
                } else {
                    cmp = compare_cross(*vec_a, la, *vec_b, lb);
                }
                if (cmp == 0) continue;
                return (k.order_ == order::ascending) ? (cmp < 0) : (cmp > 0);
            }
            return false;
        }
        // Single chunk mode
        for (const auto& k : keys_) {
            if (!k.vec) continue;
            int cmp = compare_raw(*k.vec, row_a, row_b);
            if (cmp == 0) continue;
            return (k.order_ == order::ascending) ? (cmp < 0) : (cmp > 0);
        }
        return false;
    }

    namespace {

        template<typename T>
        int compare_typed(const vector::vector_t& vec, size_t a, size_t b) {
            auto* d = vec.data<T>();
            return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
        }

        template<typename T>
        int compare_cross_typed(const vector::vector_t& va, size_t a,
                                const vector::vector_t& vb, size_t b) {
            auto da = va.data<T>()[a];
            auto db = vb.data<T>()[b];
            return (da < db) ? -1 : (da > db) ? 1 : 0;
        }

    } // anonymous namespace

    int columnar_sorter_t::compare_raw(const vector::vector_t& vec, size_t a, size_t b) {
        // Handle NULLs: NULLs sort last
        bool a_null = vec.is_null(a);
        bool b_null = vec.is_null(b);
        if (a_null && b_null) return 0;
        if (a_null) return 1;
        if (b_null) return -1;

        switch (vec.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return compare_typed<int8_t>(vec, a, b);
            case types::physical_type::INT16:
                return compare_typed<int16_t>(vec, a, b);
            case types::physical_type::INT32:
                return compare_typed<int32_t>(vec, a, b);
            case types::physical_type::INT64:
                return compare_typed<int64_t>(vec, a, b);
            case types::physical_type::UINT8:
                return compare_typed<uint8_t>(vec, a, b);
            case types::physical_type::UINT16:
                return compare_typed<uint16_t>(vec, a, b);
            case types::physical_type::UINT32:
                return compare_typed<uint32_t>(vec, a, b);
            case types::physical_type::UINT64:
                return compare_typed<uint64_t>(vec, a, b);
            case types::physical_type::INT128:
                return compare_typed<types::int128_t>(vec, a, b);
            case types::physical_type::UINT128:
                return compare_typed<types::uint128_t>(vec, a, b);
            case types::physical_type::FLOAT:
                return compare_typed<float>(vec, a, b);
            case types::physical_type::DOUBLE:
                return compare_typed<double>(vec, a, b);
            case types::physical_type::STRING:
                return compare_typed<std::string_view>(vec, a, b);
            default: {
                if (!vec.resource()) return 0;
                auto va = vec.value(a);
                auto vb = vec.value(b);
                auto cmp = va.compare(vb);
                return (cmp == types::compare_t::less) ? -1 : (cmp == types::compare_t::more) ? 1 : 0;
            }
        }
    }

    int columnar_sorter_t::compare_cross(const vector::vector_t& vec_a, size_t a,
                                         const vector::vector_t& vec_b, size_t b) {
        bool a_null = vec_a.is_null(a);
        bool b_null = vec_b.is_null(b);
        if (a_null && b_null) return 0;
        if (a_null) return 1;
        if (b_null) return -1;

        switch (vec_a.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return compare_cross_typed<int8_t>(vec_a, a, vec_b, b);
            case types::physical_type::INT16:
                return compare_cross_typed<int16_t>(vec_a, a, vec_b, b);
            case types::physical_type::INT32:
                return compare_cross_typed<int32_t>(vec_a, a, vec_b, b);
            case types::physical_type::INT64:
                return compare_cross_typed<int64_t>(vec_a, a, vec_b, b);
            case types::physical_type::UINT8:
                return compare_cross_typed<uint8_t>(vec_a, a, vec_b, b);
            case types::physical_type::UINT16:
                return compare_cross_typed<uint16_t>(vec_a, a, vec_b, b);
            case types::physical_type::UINT32:
                return compare_cross_typed<uint32_t>(vec_a, a, vec_b, b);
            case types::physical_type::UINT64:
                return compare_cross_typed<uint64_t>(vec_a, a, vec_b, b);
            case types::physical_type::INT128:
                return compare_cross_typed<types::int128_t>(vec_a, a, vec_b, b);
            case types::physical_type::UINT128:
                return compare_cross_typed<types::uint128_t>(vec_a, a, vec_b, b);
            case types::physical_type::FLOAT:
                return compare_cross_typed<float>(vec_a, a, vec_b, b);
            case types::physical_type::DOUBLE:
                return compare_cross_typed<double>(vec_a, a, vec_b, b);
            case types::physical_type::STRING:
                return compare_cross_typed<std::string_view>(vec_a, a, vec_b, b);
            default: {
                if (!vec_a.resource()) return 0;
                auto va = vec_a.value(a);
                auto vb = vec_b.value(b);
                auto cmp = va.compare(vb);
                return (cmp == types::compare_t::less) ? -1 : (cmp == types::compare_t::more) ? 1 : 0;
            }
        }
    }

} // namespace components::sort
