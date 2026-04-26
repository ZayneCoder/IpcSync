/**
 * ipcsync_internal.h — 共享内存内部布局（库内部使用，不对外暴露）
 */

#ifndef IPCSYNC_INTERNAL_H
#define IPCSYNC_INTERNAL_H

#include "ipcsync.h"
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>

/* ===== 共享内存 Header ===== */
typedef struct {
    uint32_t magic;           /* IPC_MAGIC                          */
    uint32_t shm_version;     /* IPC_SHM_VERSION，布局不兼容时拒绝   */
    uint64_t shm_size;        /* 总共享内存大小（字节）               */
    uint32_t entry_count;     /* 当前有效条目数                       */
    uint32_t _pad1;
    uint64_t data_offset;     /* 变长数据区起始偏移                   */
    uint64_t data_end;        /* 已用数据区末尾偏移（bump 指针）      */
    uint64_t ring_offset;     /* 环形队列起始偏移                     */
    uint64_t sub_offset;      /* 订阅者表起始偏移                     */
    _Atomic uint64_t total_writes; /* 累计写入次数（统计用）          */
    uint8_t  _pad2[16];       /* 预留，凑满 64 字节                   */
} ipc_header_t;

_Static_assert(sizeof(ipc_header_t) == 80, "ipc_header_t size mismatch");

/* ===== 元数据条目（元数据表中每一项） ===== */
typedef struct {
    char     key[IPC_MAX_KEY_LEN]; /* 键名（\0 结尾）                */
    uint64_t data_offset;          /* 在数据区的起始偏移              */
    uint32_t data_len;             /* 数据字节数                      */
    uint32_t _pad;
    _Atomic uint64_t version;      /* 单调递增版本号（0=空槽）         */
    uint64_t timestamp_ns;         /* 最后写入时间戳                   */
    uint8_t  active;               /* 1=有效 0=已删除                  */
    uint8_t  _pad2[7];
} ipc_entry_t;

_Static_assert(sizeof(ipc_entry_t) == 104, "ipc_entry_t size mismatch");

/* ===== 订阅者记录 ===== */
typedef struct {
    pid_t    pid;                      /* 订阅者进程 pid               */
    char     key_filter[IPC_MAX_KEY_LEN]; /* 过滤键名，"*"=全部        */
    char     sem_name[80];             /* 通知信号量的名称              */
    uint8_t  active;
    uint8_t  _pad[3];
} ipc_subscriber_rec_t;

_Static_assert(sizeof(ipc_subscriber_rec_t) == 152, "ipc_subscriber_rec_t size mismatch");

/* ===== 无锁环形队列 header ===== */
typedef struct {
    _Atomic uint64_t head;            /* 消费者读指针                  */
    _Atomic uint64_t tail;            /* 生产者写指针                  */
    uint32_t         capacity;        /* IPC_RING_SIZE                 */
    uint8_t          _pad[4];
    /* 之后紧跟 capacity 个 ipc_change_event_t */
} ipc_ring_header_t;

/* ===== 共享内存整体视图（辅助指针，映射到同一块内存） ===== */
typedef struct {
    ipc_header_t         *hdr;
    pthread_rwlock_t     *rwlock;      /* 紧跟 header 之后              */
    ipc_entry_t          *entries;    /* 元数据表                       */
    ipc_subscriber_rec_t *subs;       /* 订阅者表                       */
    ipc_ring_header_t    *ring_hdr;   /* 环形队列 header                */
    ipc_change_event_t   *ring_buf;   /* 环形队列数据区                 */
    uint8_t              *data_pool;  /* 变长数据区                     */
    void                 *base;       /* mmap 基址                      */
    size_t                size;       /* mmap 大小                      */
} ipc_shm_view_t;

/* ===== 本地上下文（ipc_ctx_t 实际类型） ===== */
struct ipc_ctx {
    char          shm_name[256];   /* shm_open 名称                    */
    char          sem_name[256];   /* 通知信号量名称（进程独有）        */
    int           shm_fd;
    ipc_shm_view_t shm;

    sem_t        *notify_sem;      /* 当前进程的通知信号量              */

    /* 订阅回调 */
    struct {
        char            key[IPC_MAX_KEY_LEN];
        ipc_callback_t  cb;
        void           *userdata;
        uint8_t         active;
    } sub_slots[IPC_MAX_ENTRIES];
    int sub_count;

    /* 后台通知线程 */
    pthread_t  notify_thread;
    int        notify_running;    /* 1=运行中 */
    int        stop_pipe[2];      /* 用于优雅停止线程的管道 */
};

/* ===== 内部工具函数声明 ===== */
uint64_t ipc_now_ns(void);
ipc_err_t ipc_shm_map(const char *name, int create, size_t size,
                      ipc_shm_view_t *view, int *fd_out);
void ipc_shm_unmap(ipc_shm_view_t *view, int fd);
ipc_entry_t *ipc_find_entry(ipc_shm_view_t *shm, const char *key);
ipc_entry_t *ipc_alloc_entry(ipc_shm_view_t *shm, const char *key);
int ipc_ring_push(ipc_shm_view_t *shm, const ipc_change_event_t *ev);
int ipc_ring_pop(ipc_shm_view_t *shm, ipc_change_event_t *ev);

#endif /* IPCSYNC_INTERNAL_H */
