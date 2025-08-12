# EloqStore

Compile:
```shell
# Debug mode
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DWITH_ASAN=ON
cmake --build . -j8
cd ..

# Release mode
mkdir Release
cd Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
cd ..
```

Run unit tests:
```shell
ctest --test-dir build/tests/
```

Benchmark for bulk load:
```shell
./Release/benchmark/load_bench --kvoptions <path>
```
