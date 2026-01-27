git ls-files '*.c' '*.cc' '*.cpp' '*.h' '*.hpp' \
  | grep -Ev '^(third_party|vendor|build|external)/' \
  | xargs clang-format -i