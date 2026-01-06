<div align="center">
<a href='https://www.eloqdata.com'>
<img src="images/eloqstore_github_logo.jpg" alt="EloqStore" height=150></img>
</a>

---

[![License: BSL 2.0](https://img.shields.io/badge/License-BSL_2.0-blue.svg)](https://github.com/eloqdata/eloqstore/blob/main/LICENSE.md)
[![Language](https://img.shields.io/badge/language-C++-orange)](https://isocpp.org/)
[![GitHub issues](https://img.shields.io/github/issues/eloqdata/eloqstore)](https://github.com/eloqdata/eloqstore/issues)
[![Release](https://img.shields.io/badge/release-latest-blue)](https://www.eloqdata.com/download)
<a href="https://discord.com/invite/nmYjBkfak6">
  <img alt="EloqKV" src="https://img.shields.io/badge/discord-blue.svg?logo=discord&logoColor=white">
</a>
</div>

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