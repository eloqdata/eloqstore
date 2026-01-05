# EloqStore

## 🔨 Compile

### Debug Mode

```shell
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
cmake --build . -j8
cd ..
```

### Release Mode

```shell
mkdir Release
cd Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j8
cd ..
```

## 🧪 Testing

### Run Unit Tests

```shell
ctest --test-dir build/tests/
```

### Benchmark

```shell
# An example to run eloqstore with 10GB data, with each record 1K.
# load
./build/benchmark/simple_bench --kvoptions=./benchmark/opts_append.ini --workload=write-read --kv_size=1024 --batch_size=20000 --max_key=10000000 --read_per_part=4 --partitions=1 --load
# run
./build/benchmark/simple_bench --kvoptions=./benchmark/opts_append.ini --workload=write-read --kv_size=1024 --batch_size=20000 --max_key=10000000 --read_per_part=4 --partitions=1
```