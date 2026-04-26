# IpcSync (libipcsync)

一个基于 **POSIX 共享内存** 的跨进程 Key-Value 同步小库，支持：

- 共享内存 `shm_open/mmap` 存储数据
- `pthread_rwlock(PTHREAD_PROCESS_SHARED)` 读写保护
- 命名信号量通知订阅者数据变更
- 版本号递增避免脏读
- 环形队列传递变更事件

本仓库同时提供：

- 静态库：`build/libipcsync.a`
- 示例：`build/producer`、`build/consumer`
- 单元测试：`build/test_ipcsync`

## 目录结构

```
.
├── Makefile
├── include/
│   └── ipcsync.h
├── src/
│   ├── ipcsync.c
│   └── ipcsync_internal.h
├── examples/
│   ├── producer.c
│   └── consumer.c
└── tests/
    └── test_ipcsync.c
```

## 依赖

- Linux / WSL2
- `gcc`、`make`（Debian/Ubuntu 可用 `build-essential`）
- `pthread`、`librt`（链接参数已在 `Makefile` 中包含：`-lpthread -lrt`）

安装（Ubuntu/WSL2）：

```bash
sudo apt update
sudo apt install -y build-essential
```

## 编译

```bash
cd /home/zayne/code/IpcSync
make
```

产物在 `build/`：

- `build/libipcsync.a`
- `build/producer`
- `build/consumer`
- `build/test_ipcsync`

清理：

```bash
make clean
```

## 运行示例

示例使用的共享内存名是 `"/demo_sync"`（见 `examples/producer.c` / `examples/consumer.c`）。

终端 1（先启动 producer，创建共享内存并周期写入）：

```bash
./build/producer
```

终端 2（再启动 consumer，订阅 `*` 并轮询读取）：

```bash
./build/consumer
```

停止：两边 `Ctrl+C`。

## 运行单元测试

```bash
make test
```

`make test` 会尝试清理 `/dev/shm/` 下 `test_ipcsync_tmp*` 的残留文件，然后执行 `./build/test_ipcsync`。

## 作为库使用（API 速览）

头文件：`include/ipcsync.h`

- **创建/打开/关闭**
  - `ipc_create(name, shm_size, &ctx)`
  - `ipc_open(name, &ctx)`
  - `ipc_close(ctx)`
  - `ipc_destroy(name)`
- **读写**
  - `ipc_publish(ctx, key, data, len)`
  - `ipc_get(ctx, key, buf, buf_len, &out_val)`
  - `ipc_delete(ctx, key)`
- **订阅**
  - `ipc_subscribe(ctx, key_or_star, cb, userdata)`
  - `ipc_unsubscribe(ctx, key)`
- **等待/统计**
  - `ipc_wait_change(ctx, timeout_ms, &event)`
  - `ipc_stat(ctx, &stat)`
  - `ipc_strerror(err)`

### 共享内存命名约定

- `name` 需要以 `/` 开头，例如 `"/demo_sync"`、`"/myapp_data"`。
- 示例会在 `/dev/shm/` 下创建对应对象。

## 常见问题

### consumer 提示 “请先启动 producer”

`consumer` 会 `ipc_open("/demo_sync", ...)`，如果共享内存还没被创建，打开会失败。请先运行：

```bash
./build/producer
```

### 清理共享内存

如果异常退出导致残留，可手动清理（谨慎使用，确保名字只属于本程序）：

```bash
ls -la /dev/shm | rg demo_sync
rm -f /dev/shm/demo_sync*
```

