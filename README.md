# EloqStore

## 🔨 Compile

### Install jsoncpp
```shell
sudo apt install libjsoncpp-dev
```

### Debug Mode
```shell
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DWITH_ASAN=ON
cmake --build . -j8
cd ..

# Note that https://www.boost.org/doc/libs/master/libs/context/doc/html/context/stack/sanitizers.html
wget https://github.com/boostorg/boost/releases/download/boost-1.90.0/boost-1.90.0-b2-nodocs.tar.gz && \
    tar zxf boost-1.90.0-b2-nodocs.tar.gz && cd boost-1.90.0 && ./bootstrap.sh && \
    sudo ./b2 --with-context context-impl=ucontext --buildid=asan cxxflags="-fsanitize=address -DBOOST_USE_ASAN" linkflags="-fsanitize=address" variant=release threading=multi link=static,shared install && \
    cd ../ &&
    sudo ldconfig && \
    sudo rm -rf boost-1.90.0-b2-nodocs.tar.gz boost-1.90.0 /usr/local/include/boost/context/
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

### Benchmark for Bulk Load
```shell
./Release/benchmark/load_bench --kvoptions <path>
./build/benchmark/simple_bench --kvoptions=./benchmark/opts_append.ini --workload=write-read --test_secs=60 --partitions=4 --read_thds=2 --write_batchs=8192  // You can change the workload to write-read/write-scan/read/write/scan
```

## 🔗 Integration with EloqKV

### 📥 Download Code

1. **Download EloqKV:**
   ```bash
   git clone git@github.com:eloqdata/eloqkv.git
   ```

2. **Initialize Submodules:**
   ```bash
   cd eloqkv
   git submodule update --init --recursive
   ```

3. **Download EloqStore:**
   ```bash
   cd data_substrate/store_handler/eloq_data_store_service
   git clone git@github.com:eloqdata/eloqstore.git
   ```

### 🏗️ Build EloqKV using EloqStore as the DataStore

```bash
cd ../../../

mkdir build
cd build

cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=./install -DWITH_DATA_STORE=ELOQDSS_ELOQSTORE
make -j 8
make install
```

### 🚀 Run EloqKV using EloqStore

1. **Edit config file `eloqkv.ini`:**
   ```ini
   [local]
   ip=127.0.0.1
   port=6379
   core_number=8
   enable_data_store=true
   eloq_data_path={your_data_path}

   [cluster]
   ip_port_list=127.0.0.1:6379

   [store]
   ## data store service
   # The shard count of the eloqstore, it must be the same with the value of @@core_num
   eloq_store_worker_num=8
   # The eloqstore data path list, if not set, it will store as {eloq_data_path}/eloq_dss/eloqstore_data
   eloq_store_data_path_list={your_eloqstore_data_path1, your_eloqstore_data_path2,...}
   # Max number of open files used by eloqstore(default: 1024)
   eloq_store_open_files_limit=1024
   # Max amount of cached index pages
   eloq_store_index_buffer_pool_size=536870912
   ```

2. **Start EloqKV:**
   ```bash
   cd install
   ./bin/eloqkv --config=${config_file_path}/eloqkv.ini
   ```

### ☁️ Run EloqKV using EloqStore Cloud

1. **Edit config file `eloqkv.ini`:**
   ```ini
   [local]
   ip=127.0.0.1
   port=6379
   core_number=8
   enable_data_store=true

   [cluster]
   ip_port_list=127.0.0.1:6379

   [store]
   ## data store service
   eloq_store_worker_num=8
   # The eloqstore data path list, if not set, it will store as {eloq_data_path}/eloq_dss/eloqstore_data
   eloq_store_data_path_list={your_eloqstore_data_path1, your_eloqstore_data_path2,...}
   # Max number of open files used by eloqstore(default: 1024), cannot be larger than maxclients
   eloq_store_open_files_limit=1024
   eloq_store_cloud_store_path=dss-eloqstore-dev
   # Max amount of cached index pages (supports KB/MB/GB units, e.g., 32MB)
   eloq_store_index_buffer_pool_size=32MB
   # Local disk space usage limit
   eloq_store_local_space_limit=10GB
   # Cloud mode must use append mode.
   eloq_store_data_append_mode=true
   eloq_store_cloud_provider=aws
   eloq_store_cloud_endpoint=http://127.0.0.1:9900
   eloq_store_cloud_region=us-east-1
   eloq_store_cloud_access_key=minioadmin
   eloq_store_cloud_secret_key=minioadmin
   ```

2. **Install MinIO and start:**
   ```bash
   ./minio server ${your_minio_data_path} --address :9900 --console-address :9901
   ```

3. **Configure S3 credentials for EloqStore:**
   EloqStore now talks directly to MinIO/AWS-compatible endpoints using the
   credentials in `kv_options`. Make sure the `[permanent]` section sets the
   proper bucket/prefix and endpoint:
   ```ini
   [permanent]
   cloud_store_path = eloqstore/unit-test
   cloud_endpoint = http://127.0.0.1:9900
   cloud_region = us-east-1
   cloud_access_key = minioadmin
   cloud_secret_key = minioadmin
   cloud_verify_ssl = false
   ```
   The `cloud_store_path` takes the form `bucket[/prefix]`. No additional
   proxy daemons are required.
5. **Start EloqKV:**
  ```bash
  cd install
  ./bin/eloqkv --config=${config_file_path}/eloqkv.ini
  ```
