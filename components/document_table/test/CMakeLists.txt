project(test_document_table)

set(${PROJECT_NAME}_SOURCES
        test_operators.cpp
        test_comprehensive.cpp
        test_primary_key_scan.cpp
        )

add_executable(otterbrix_${PROJECT_NAME}
        main.cpp
        ${${PROJECT_NAME}_SOURCES}
        )

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::document_table
        otterbrix::physical_plan
        otterbrix::collection
        otterbrix::test_generaty
        Catch2::Catch2
)

add_test(NAME ${PROJECT_NAME} COMMAND otterbrix_${PROJECT_NAME})
