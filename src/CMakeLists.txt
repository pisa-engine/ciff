add_executable(generate_pisa_index generate_pisa_index.cpp)
target_link_libraries(generate_pisa_index
  cif
  CLI11
)

