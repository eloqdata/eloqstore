project(Examples)

set(EXAMPLE_DIR ${CMAKE_SOURCE_DIR}/examples)
set(EXAMPLE_INCLUDE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR})

set(EXAMPLE_SOURCES
    basic_example.cpp
)

string( REPLACE ".cpp" "" ALL_TARGETS "${EXAMPLE_SOURCES}" )

foreach( name ${ALL_TARGETS} )
   add_executable( ${name} ${EXAMPLE_DIR}/${name}.cpp)
endforeach()

foreach( name ${ALL_TARGETS} )
    target_include_directories(${name} PUBLIC ${EXAMPLE_INCLUDE} ${INCLUDE_DIR})
endforeach()

foreach( name ${ALL_TARGETS} )
    target_link_libraries( ${name} PRIVATE eloqstore)
endforeach()
