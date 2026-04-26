/**
 * ipcsync.h — 跨进程数据同步库公共 API
 *
 * 设计要点：
 *  - 共享内存 (POSIX shm_open / mmap) 作为数据载体
 *  - pthread_rwlock (PTHREAD_PROCESS_SHARED) 保护并发读写
 *  - POSIX 命名信号量通知订阅者有新数据
 *  - 版本号 (单调递增) 防止 ABA 脏读
 *  - 无锁环形队列传递变更事件
 */

#ifndef IPCSYNC_H
#define IPCSYNC_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ===== 编译期常量 ===== */
#define IPC_MAX_KEY_LEN      64     /* key 最大长度（含 \0）         */
#define IPC_MAX_ENTRIES      256    /* 最多可存储的 key 数量          */
#define IPC_MAX_SUBSCRIBERS  64     /* 最多同时订阅的进程数           */
#define IPC_RING_SIZE        256    /* 变更事件环形队列容量（2 的幂）  */
#define IPC_DEFAULT_SHM_SIZE (4 * 1024 * 1024)  /* 默认共享内存 4MB  */
#define IPC_MAGIC            0x53435049u  /* "IPCS" LE               */
#define IPC_SHM_VERSION      1

/* ===== 错误码 ===== */
typedef enum {
    IPC_OK              =  0,
    IPC_ERR_INVALID     = -1,   /* 参数非法                    */
    IPC_ERR_NOT_FOUND   = -2,   /* key 不存在                  */
    IPC_ERR_NO_SPACE    = -3,   /* 共享内存空间不足             */
    IPC_ERR_LOCK        = -4,   /* 加锁失败                    */
    IPC_ERR_SHM         = -5,   /* shm 操作失败                */
    IPC_ERR_SEM         = -6,   /* 信号量操作失败              */
    IPC_ERR_VERSION     = -7,   /* 共享内存版本不兼容          */
    IPC_ERR_TIMEOUT     = -8,   /* 等待超时                    */
    IPC_ERR_EXISTS      = -9,   /* 已存在（创建时冲突）         */
    IPC_ERR_FULL        = -10,  /* 订阅者表已满                */
} ipc_err_t;

/* ===== 变更事件操作类型 ===== */
typedef enum {
    IPC_OP_SET = 1,
    IPC_OP_DEL = 2,
} ipc_op_t;

/* ===== 变更事件（环形队列中的消息） ===== */
typedef struct {
    char     key[IPC_MAX_KEY_LEN];
    uint64_t version;
    uint64_t timestamp_ns;   /* 写入时的 CLOCK_MONOTONIC 纳秒值 */
    uint8_t  op;             /* ipc_op_t                        */
    uint8_t  _pad[7];
} ipc_change_event_t;

/* ===== 读取结果（ipc_get 返回） ===== */
typedef struct {
    void    *data;           /* 指向调用者提供的 buf         */
    size_t   data_len;       /* 实际数据字节数               */
    uint64_t version;        /* 该值对应的版本号             */
    uint64_t timestamp_ns;   /* 写入时间戳                   */
} ipc_value_t;

/* ===== 订阅回调类型 ===== */
/* 在后台通知线程中调用，不要在回调里做耗时操作 */
typedef void (*ipc_callback_t)(const char *key,
                               const ipc_value_t *val,
                               void *userdata);

/* ===== 上下文句柄（不透明） ===== */
typedef struct ipc_ctx ipc_ctx_t;

/* ===== 创建 / 打开 API ===== */

/**
 * ipc_create — 创建并初始化共享内存区（通常由第一个进程调用）
 *
 * @param name      共享内存名称，如 "/myapp_data"（需以 / 开头）
 * @param shm_size  共享内存总大小（字节），0 表示使用默认值
 * @param[out] ctx  成功时填充上下文指针
 * @return IPC_OK 或负数错误码
 */
ipc_err_t ipc_create(const char *name, size_t shm_size, ipc_ctx_t **ctx);

/**
 * ipc_open — 打开已存在的共享内存区
 *
 * @param name  与 ipc_create 相同的名称
 * @param[out] ctx
 */
ipc_err_t ipc_open(const char *name, ipc_ctx_t **ctx);

/**
 * ipc_close — 关闭上下文，解除内存映射（不删除共享内存）
 */
void ipc_close(ipc_ctx_t *ctx);

/**
 * ipc_destroy — 删除共享内存及信号量（通常由创建者最后调用）
 */
ipc_err_t ipc_destroy(const char *name);

/* ===== 数据读写 API ===== */

/**
 * ipc_publish — 写入或更新一个 key-value 对
 *
 * 线程/进程安全，内部加写锁，写完后递增版本号并推送变更事件。
 *
 * @param ctx   上下文
 * @param key   键名（不超过 IPC_MAX_KEY_LEN-1 字节）
 * @param data  数据指针
 * @param len   数据字节数
 */
ipc_err_t ipc_publish(ipc_ctx_t *ctx, const char *key,
                      const void *data, size_t len);

/**
 * ipc_get — 读取一个 key 的值
 *
 * 调用者需提供足够大的 buf；函数内部持读锁，读完后做版本二次确认。
 *
 * @param ctx      上下文
 * @param key      键名
 * @param buf      调用者分配的接收缓冲区
 * @param buf_len  缓冲区大小
 * @param[out] out 填充读取结果
 */
ipc_err_t ipc_get(ipc_ctx_t *ctx, const char *key,
                  void *buf, size_t buf_len, ipc_value_t *out);

/**
 * ipc_delete — 删除一个 key
 */
ipc_err_t ipc_delete(ipc_ctx_t *ctx, const char *key);

/* ===== 订阅 API ===== */

/**
 * ipc_subscribe — 订阅某个 key 的变更通知
 *
 * 库内部会启动一个后台线程阻塞在信号量上，有新事件时过滤并调用 cb。
 *
 * @param ctx       上下文
 * @param key       要监听的键名，"*" 表示监听所有
 * @param cb        回调函数
 * @param userdata  传给 cb 的用户指针
 */
ipc_err_t ipc_subscribe(ipc_ctx_t *ctx, const char *key,
                        ipc_callback_t cb, void *userdata);

/**
 * ipc_unsubscribe — 取消订阅
 */
ipc_err_t ipc_unsubscribe(ipc_ctx_t *ctx, const char *key);

/* ===== 工具 API ===== */

/**
 * ipc_wait_change — 阻塞等待任意 key 发生变更（带超时）
 *
 * @param ctx         上下文
 * @param timeout_ms  超时毫秒，-1 表示永久等待
 * @param[out] event  填充变更事件详情
 */
ipc_err_t ipc_wait_change(ipc_ctx_t *ctx, int timeout_ms,
                          ipc_change_event_t *event);

/**
 * ipc_stat — 查询共享内存统计信息（调试用）
 */
typedef struct {
    uint32_t entry_count;
    uint32_t subscriber_count;
    size_t   shm_used;
    size_t   shm_total;
    uint64_t total_writes;
} ipc_stat_t;

ipc_err_t ipc_stat(ipc_ctx_t *ctx, ipc_stat_t *stat);

/**
 * ipc_strerror — 返回错误码对应的描述字符串
 */
const char *ipc_strerror(ipc_err_t err);

#ifdef __cplusplus
}
#endif

#endif /* IPCSYNC_H */
