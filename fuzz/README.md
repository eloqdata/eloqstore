# Download dependencies
git clone https://github.com/AFLplusplus/AFLplusplus.git
cd AFLplusplus
make distrib
sudo make install

# How to build
cmake -B build-fuzz -DWITH_FUZZ=ON -DWITH_UNIT_TESTS=OFF -DWITH_EXAMPLE=OFF -DWITH_DB_STRESS=OFF -DWITH_BENCHMARK=OFF -DWITH_ASAN=ON -DCMAKE_C_COMPILER=afl-clang-fast -DCMAKE_CXX_COMPILER=afl-clang-fast

cmake --build build-fuzz --target afl_store_fuzz -j$(nproc)


# use asan and coverage
cmake -B build-fuzz \
  -DWITH_FUZZ=ON \
  -DWITH_UNIT_TESTS=OFF \
  -DWITH_EXAMPLE=OFF \
  -DWITH_DB_STRESS=OFF \
  -DWITH_BENCHMARK=OFF \
  -DWITH_ASAN=ON \
  -DWITH_COVERAGE=ON \
  -DCMAKE_C_COMPILER=afl-clang-fast \
  -DCMAKE_CXX_COMPILER=afl-clang-fast
cmake --build build-fuzz --target afl_store_fuzz -j$(nproc)  

# How to run it 
# The first option loads the data from the last session, while the second option starts over.
AFL_AUTORESUME=1 AFL_SKIP_CPUFREQ=1 afl-fuzz -i - -o fuzz/out -- ./build-fuzz/fuzz/afl_store_fuzz
AFL_SKIP_CPUFREQ=1 afl-fuzz -i fuzz/seeds -o fuzz/out -- ./build-fuzz/fuzz/afl_store_fuzz

# Debug for crash case
./build-fuzz/fuzz/afl_store_fuzz < "the path to the crash case"
ps: the path to the crash case is like in fuzz/out/default/crashes.2025-12-11-15:03:12/id:000050,sig:11,src:000480,time:5080307,execs:100059,op:havoc,rep:1