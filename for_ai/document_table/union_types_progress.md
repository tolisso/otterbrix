# Progress: Union Types Integration in document_table

## ✅ Completed Steps

### 1. Extended `column_info_t` structure
**File**: `components/document_table/dynamic_schema.hpp`

Added union support fields:
```cpp
struct column_info_t {
    // ... existing fields ...
    
    // Union type support
    bool is_union = false;
    std::pmr::vector<types::logical_type> union_types;
};
```

### 2. Added new methods to `dynamic_schema_t`
**File**: `components/document_table/dynamic_schema.hpp`

```cpp
// Public:
uint8_t get_union_tag(const column_info_t* col, types::logical_type type) const;

// Private:
void create_union_column(const std::string& json_path,
                        types::logical_type type1,
                        types::logical_type type2);

void extend_union_column(const std::string& json_path,
                        types::logical_type new_type);
```

### 3. Implemented union type methods
**File**: `components/document_table/dynamic_schema.cpp`

- ✅ `create_union_column()` - creates union from two types
- ✅ `extend_union_column()` - adds new type to existing union
- ✅ `get_union_tag()` - returns tag index for a type in union

### 4. Modified `evolve()` logic
**File**: `components/document_table/dynamic_schema.cpp`

Changed type mismatch handling:
- ❌ **Before**: `throw std::runtime_error("Type mismatch...")`
- ✅ **After**: Creates union or extends existing union

### 5. Added helper methods declaration
**File**: `components/document_table/document_table_storage.hpp`

```cpp
types::logical_value_t extract_value_from_document(...);
types::logical_type detect_value_type_in_document(...);
```

## ⏭️ Next Steps

### 6. Implement helper methods in `document_table_storage.cpp`

```cpp
types::logical_value_t document_table_storage_t::extract_value_from_document(
    const document::document_ptr& doc,
    const std::string& json_path,
    types::logical_type expected_type) {
    
    // Check document has the field
    if (!doc->is_exists(json_path)) {
        return types::logical_value_t(); // null
    }
    
    // Extract based on type
    switch (expected_type) {
        case types::logical_type::BOOLEAN:
            if (doc->is_bool(json_path)) {
                return types::logical_value_t(doc->get_bool(json_path));
            }
            break;
        // ... other types ...
    }
    
    return types::logical_value_t(); // null if type mismatch
}

types::logical_type document_table_storage_t::detect_value_type_in_document(
    const document::document_ptr& doc,
    const std::string& json_path) {
    
    if (!doc->is_exists(json_path)) {
        return types::logical_type::NA;
    }
    
    if (doc->is_bool(json_path)) return types::logical_type::BOOLEAN;
    if (doc->is_int(json_path)) return types::logical_type::INTEGER;
    if (doc->is_long(json_path)) return types::logical_type::BIGINT;
    if (doc->is_double(json_path)) return types::logical_type::DOUBLE;
    if (doc->is_string(json_path)) return types::logical_type::STRING_LITERAL;
    // ...
    
    return types::logical_type::NA;
}
```

### 7. Add UNION case to `document_to_row()` switch

```cpp
// In document_to_row() method, add before default:
case types::logical_type::UNION:
    if (col_info->is_union) {
        // Detect actual type in document
        auto actual_type = detect_value_type_in_document(doc, col_info->json_path);
        
        if (actual_type == types::logical_type::NA) {
            vec.set_null(0, true);
            break;
        }
        
        // Get tag for this type
        uint8_t tag = schema_->get_union_tag(col_info, actual_type);
        
        // Extract value
        auto value = extract_value_from_document(doc, col_info->json_path, actual_type);
        
        // Create union types vector
        std::vector<types::complex_logical_type> union_types;
        for (auto t : col_info->union_types) {
            union_types.emplace_back(t);
        }
        
        // Create union value
        auto union_value = types::logical_value_t::create_union(
            std::move(union_types),
            tag,
            std::move(value)
        );
        
        vec.set_value(0, std::move(union_value));
    } else {
        vec.set_null(0, true);
    }
    break;
```

### 8. Handle schema evolution with existing data

When a column becomes union, existing data needs to be converted:

**Option A: Lazy conversion** (simpler, recommended for first version)
- Keep old data as-is
- When reading: if column is union but value is not, wrap it automatically

**Option B: Eager conversion** (more complex)
- When union is created, rescan all existing data
- Wrap each value in union with appropriate tag

### 9. Test the implementation

Create tests:
```cpp
TEST(DynamicSchemaUnion, BasicTypeConflict) {
    // Insert doc with int
    auto doc1 = R"({"age": 30})";
    storage.insert(id1, doc1);
    
    // Insert doc with string - should create union
    auto doc2 = R"({"age": "thirty"})";
    storage.insert(id2, doc2);  // Should NOT throw!
    
    // Verify schema has union
    auto* col = schema.get_column_info("age");
    ASSERT_TRUE(col->is_union);
    ASSERT_EQ(col->union_types.size(), 2);
}
```

### 10. Document the changes

Update documentation:
- How union types work
- When they are created
- Performance implications
- Migration guide

## 📝 Current File Status

| File | Status | Changes |
|------|--------|---------|
| `dynamic_schema.hpp` | ✅ Modified | Added union fields and methods |
| `dynamic_schema.cpp` | ✅ Modified | Implemented union logic |
| `document_table_storage.hpp` | ✅ Modified | Added helper methods declaration |
| `document_table_storage.cpp` | ⏭️ In Progress | Need to implement helpers and UNION case |

## 🧪 Testing Plan

1. **Unit tests for dynamic_schema**:
   - Test `create_union_column()`
   - Test `extend_union_column()`
   - Test `get_union_tag()`

2. **Integration tests**:
   - Insert documents with different types for same path
   - Verify no exceptions thrown
   - Verify correct union types created

3. **Edge cases**:
   - Three or more different types
   - Nested structures with type conflicts
   - Array elements with type conflicts

## 💡 Implementation Notes

### Key Design Decisions

1. **Union member order**: Types are added in the order they are first encountered
2. **Tag assignment**: Tag = index in `union_types` vector
3. **Schema evolution**: Happens automatically on insert
4. **Backward compatibility**: Existing non-union columns work as before

### Performance Considerations

- Union values have overhead (tag + struct)
- Reading union requires type checking
- Creating union forces schema migration

### Future Improvements

- [ ] Support union in nested structures
- [ ] Support union in array elements
- [ ] Optimize union storage for common cases
- [ ] Add statistics: which types are most common

## 🔗 Related Files

- `/for_ai/document_table/union_types_integration_plan.md` - Full plan
- `/for_ai/document_table_key_points.md` - document_table overview
- `/for_ai/document_table/00_summary.md` - Architecture summary

---

**Last Updated**: 2024-11-30
**Status**: In Progress (Steps 1-5 completed, 6-10 remaining)

