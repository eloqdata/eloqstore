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
   cd store_handler/eloq_data_store_service
   git clone git@github.com:eloqdata/eloqstore.git
   ```

### 🏗️ Build EloqKV using EloqStore as the DataStore

```bash
cd ../../

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
   # Max number of open files used by eloqstore(default: 1024)
   eloq_store_open_files_limit=1024
   eloq_store_cloud_store_path=eloqstore_cloud:dss-eloqstore-dev
   # Max amount of cached index pages (supports KB/MB/GB units, e.g., 32MB)
   eloq_store_index_buffer_pool_size=536870912
   ```

2. **Install MinIO and start:**
   ```bash
   ./minio server ${your_minio_data_path} --address :9900 --console-address :9901
   ```

3. **Install `rclone`:**
   ```bash
   sudo apt update
   sudo apt install rclone
   ```

4. **Configure and start `rclone`:**
   ```bash
   rclone config
   ```
   
   Follow the prompts to add your cloud storage service configuration.

   **Example configuration:**
   ```ini
   $ cat ~/.config/rclone/rclone.conf
   [eloqstore_cloud]
   type = s3
   provider = Minio
   env_auth = false
   access_key_id = minioadmin
   secret_access_key = minioadmin
   region = us-east-1
   endpoint = http://127.0.0.1:9900
   acl = private
   ```
   ```bash
   rclone rcd \
   --rc-no-auth \
   --rc-addr=127.0.0.1:5572 \
   --transfers=16 \
   --checkers=16 \
   --s3-upload-concurrency=8 \
   --s3-chunk-size=8M \
   --s3-no-head \
   --fast-list \
   -v 
   ```
   To maximize throughput you can launch multiple `rclone rcd` instances listening on different ports and list all of them in `cloud_store_daemon_ports` (space or comma separated) inside your `kv_options` file:
   ```bash
   for p in $(seq 5572 5581); do
     rclone rcd \
       --rc-no-auth \
       --rc-addr=127.0.0.1:$p \
       --transfers=64 \
       --checkers=32 \
       -v &
   done
   ```
   After starting EloqStore, terminate the background `rclone` processes when you are done (e.g., using `pkill -f "rclone rcd"`).
5. **Start EloqKV:**
  ```bash
  cd install
  ./bin/eloqkv --config=${config_file_path}/eloqkv.ini
  ```
