project(document_table)

set(${PROJECT_NAME}_SOURCES
        json_path_extractor.cpp
        document_table_storage.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC "${CMAKE_SOURCE_DIR}")

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::document
        otterbrix::types
        otterbrix::table
        otterbrix::expressions
        otterbrix::vector
        magic_enum::magic_enum
)

