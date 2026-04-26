/**
 * ipcsync.c — libipcsync 核心实现
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <pthread.h>

#include "ipcsync.h"
#include "ipcsync_internal.h"

static void ipc_strlcpy0(char *dst, const char *src, size_t dst_size) {
    if (!dst || dst_size == 0) return;
    if (!src) { dst[0] = '\0'; return; }
    size_t n = dst_size - 1;
    size_t len = strnlen(src, n);
    memcpy(dst, src, len);
    dst[len] = '\0';
}

/* ======================================================
 * 工具函数
 * ====================================================== */

uint64_t ipc_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

const char *ipc_strerror(ipc_err_t err) {
    switch (err) {
    case IPC_OK:            return "success";
    case IPC_ERR_INVALID:   return "invalid argument";
    case IPC_ERR_NOT_FOUND: return "key not found";
    case IPC_ERR_NO_SPACE:  return "no space in shared memory";
    case IPC_ERR_LOCK:      return "lock operation failed";
    case IPC_ERR_SHM:       return "shared memory operation failed";
    case IPC_ERR_SEM:       return "semaphore operation failed";
    case IPC_ERR_VERSION:   return "incompatible shm version";
    case IPC_ERR_TIMEOUT:   return "operation timed out";
    case IPC_ERR_EXISTS:    return "already exists";
    case IPC_ERR_FULL:      return "subscriber table full";
    default:                return "unknown error";
    }
}

/* ======================================================
 * 共享内存布局计算
 * ====================================================== */

/* 共享内存内部布局（偏移量固定，与 header 中记录的一致）：
 *   [0]               ipc_header_t        (64 B)
 *   [64]              pthread_rwlock_t    (~56 B，对齐到 128 B 边界)
 *   [128+sizeof(rwl)] ipc_entry_t[256]    (256 × 104 B = 26624 B)
 *   [+26624]          ipc_subscriber_rec_t[64]
 *   [+subs_size]      ipc_ring_header_t + ring_buf
 *   [ring_end]        数据池（变长 value 块）
 */

#define SHM_RWLOCK_OFFSET  128u
#define SHM_ENTRIES_OFFSET (SHM_RWLOCK_OFFSET + 128u)  /* 256 B 对齐 */
#define SHM_ENTRIES_SIZE   (IPC_MAX_ENTRIES * sizeof(ipc_entry_t))
#define SHM_SUBS_OFFSET    (SHM_ENTRIES_OFFSET + SHM_ENTRIES_SIZE)
#define SHM_SUBS_SIZE      (IPC_MAX_SUBSCRIBERS * sizeof(ipc_subscriber_rec_t))
#define SHM_RING_OFFSET    (SHM_SUBS_OFFSET + SHM_SUBS_SIZE)
#define SHM_RING_SIZE      (sizeof(ipc_ring_header_t) + \
                            IPC_RING_SIZE * sizeof(ipc_change_event_t))
#define SHM_DATA_OFFSET    (SHM_RING_OFFSET + SHM_RING_SIZE)

static void shm_view_from_base(void *base, size_t size, ipc_shm_view_t *v) {
    v->base      = base;
    v->size      = size;
    v->hdr       = (ipc_header_t *)((uint8_t *)base);
    v->rwlock    = (pthread_rwlock_t *)((uint8_t *)base + SHM_RWLOCK_OFFSET);
    v->entries   = (ipc_entry_t *)((uint8_t *)base + SHM_ENTRIES_OFFSET);
    v->subs      = (ipc_subscriber_rec_t *)((uint8_t *)base + SHM_SUBS_OFFSET);
    v->ring_hdr  = (ipc_ring_header_t *)((uint8_t *)base + SHM_RING_OFFSET);
    v->ring_buf  = (ipc_change_event_t *)((uint8_t *)base + SHM_RING_OFFSET
                    + sizeof(ipc_ring_header_t));
    v->data_pool = (uint8_t *)base + SHM_DATA_OFFSET;
}

/* ======================================================
 * shm 映射 / 取消映射
 * ====================================================== */

ipc_err_t ipc_shm_map(const char *name, int create, size_t size,
                      ipc_shm_view_t *view, int *fd_out) {
    int flags = create ? (O_CREAT | O_EXCL | O_RDWR) : O_RDWR;
    int fd = shm_open(name, flags, 0600);
    if (fd < 0) {
        if (errno == EEXIST) return IPC_ERR_EXISTS;
        return IPC_ERR_SHM;
    }

    if (create) {
        if (ftruncate(fd, (off_t)size) < 0) {
            close(fd);
            shm_unlink(name);
            return IPC_ERR_SHM;
        }
    } else {
        struct stat st;
        if (fstat(fd, &st) < 0) { close(fd); return IPC_ERR_SHM; }
        size = (size_t)st.st_size;
    }

    void *base = mmap(NULL, size, PROT_READ | PROT_WRITE,
                      MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { close(fd); return IPC_ERR_SHM; }

    shm_view_from_base(base, size, view);
    *fd_out = fd;
    return IPC_OK;
}

void ipc_shm_unmap(ipc_shm_view_t *view, int fd) {
    if (view->base) munmap(view->base, view->size);
    if (fd >= 0)    close(fd);
    memset(view, 0, sizeof(*view));
}

/* ======================================================
 * 元数据表操作
 * ====================================================== */

ipc_entry_t *ipc_find_entry(ipc_shm_view_t *shm, const char *key) {
    uint32_t n = shm->hdr->entry_count;
    for (uint32_t i = 0; i < n; i++) {
        ipc_entry_t *e = &shm->entries[i];
        if (e->active && strncmp(e->key, key, IPC_MAX_KEY_LEN) == 0)
            return e;
    }
    return NULL;
}

ipc_entry_t *ipc_alloc_entry(ipc_shm_view_t *shm, const char *key) {
    /* 先找被删除（active=0）的空槽复用 */
    for (int i = 0; i < IPC_MAX_ENTRIES; i++) {
        ipc_entry_t *e = &shm->entries[i];
        if (!e->active) {
            memset(e, 0, sizeof(*e));
            strncpy(e->key, key, IPC_MAX_KEY_LEN - 1);
            e->active = 1;
            /* 确保 entry_count >= 当前最大已用索引 */
            if ((uint32_t)i >= shm->hdr->entry_count)
                shm->hdr->entry_count = (uint32_t)i + 1;
            return e;
        }
    }
    return NULL;
}

/* 在数据池中分配 len 字节（8 字节对齐 bump allocator）*/
static uint64_t data_pool_alloc(ipc_shm_view_t *shm, size_t len) {
    uint64_t aligned = (len + 7) & ~7ULL;
    uint64_t off     = shm->hdr->data_end;
    uint64_t new_end = off + aligned;
    if (new_end > shm->hdr->shm_size) return (uint64_t)-1;
    shm->hdr->data_end = new_end;
    return off;
}

/* ======================================================
 * 无锁环形队列（单生产者多消费者，原子操作）
 * ====================================================== */

int ipc_ring_push(ipc_shm_view_t *shm, const ipc_change_event_t *ev) {
    ipc_ring_header_t *r = shm->ring_hdr;
    uint64_t tail = atomic_load_explicit(&r->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&r->head, memory_order_acquire);
    if (tail - head >= r->capacity) return -1; /* 队列满，丢弃最旧 */

    uint64_t idx = tail % r->capacity;
    shm->ring_buf[idx] = *ev;
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&r->tail, tail + 1, memory_order_release);
    return 0;
}

int ipc_ring_pop(ipc_shm_view_t *shm, ipc_change_event_t *ev) {
    ipc_ring_header_t *r = shm->ring_hdr;
    uint64_t head = atomic_load_explicit(&r->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&r->tail, memory_order_acquire);
    if (head >= tail) return -1; /* 队列空 */

    uint64_t idx = head % r->capacity;
    atomic_thread_fence(memory_order_acquire);
    *ev = shm->ring_buf[idx];
    atomic_store_explicit(&r->head, head + 1, memory_order_release);
    return 0;
}

/* ======================================================
 * 通知订阅者（广播信号量）
 * ====================================================== */

static void notify_subscribers(ipc_ctx_t *ctx, const char *key) {
    ipc_shm_view_t *shm = &ctx->shm;
    for (int i = 0; i < IPC_MAX_SUBSCRIBERS; i++) {
        ipc_subscriber_rec_t *s = &shm->subs[i];
        if (!s->active) continue;
        /* 过滤：key_filter 为 "*" 或与 key 匹配 */
        if (strcmp(s->key_filter, "*") != 0 &&
            strncmp(s->key_filter, key, IPC_MAX_KEY_LEN) != 0)
            continue;
        /* 打开对应进程的通知信号量并 post */
        sem_t *sem = sem_open(s->sem_name, 0);
        if (sem == SEM_FAILED) continue;
        sem_post(sem);
        sem_close(sem);
    }
}

/* ======================================================
 * 创建 / 打开
 * ====================================================== */

ipc_err_t ipc_create(const char *name, size_t shm_size, ipc_ctx_t **out) {
    if (!name || !out) return IPC_ERR_INVALID;
    if (shm_size == 0) shm_size = IPC_DEFAULT_SHM_SIZE;

    ipc_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return IPC_ERR_SHM;

    ipc_strlcpy0(ctx->shm_name, name, sizeof(ctx->shm_name));
    snprintf(ctx->sem_name, sizeof(ctx->sem_name), "%s_sem_%d", name, (int)getpid());

    ipc_err_t rc = ipc_shm_map(name, 1, shm_size, &ctx->shm, &ctx->shm_fd);
    if (rc != IPC_OK) { free(ctx); return rc; }

    /* 初始化 header */
    ipc_header_t *hdr = ctx->shm.hdr;
    hdr->magic       = IPC_MAGIC;
    hdr->shm_version = IPC_SHM_VERSION;
    hdr->shm_size    = shm_size;
    hdr->entry_count = 0;
    hdr->data_offset = SHM_DATA_OFFSET;
    hdr->data_end    = SHM_DATA_OFFSET;
    hdr->ring_offset = SHM_RING_OFFSET;
    hdr->sub_offset  = SHM_SUBS_OFFSET;
    atomic_init(&hdr->total_writes, 0);

    /* 初始化进程共享读写锁 */
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_rwlock_init(ctx->shm.rwlock, &attr);
    pthread_rwlockattr_destroy(&attr);

    /* 初始化环形队列 */
    ipc_ring_header_t *ring = ctx->shm.ring_hdr;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->capacity = IPC_RING_SIZE;

    /* 创建本进程的通知信号量 */
    ctx->notify_sem = sem_open(ctx->sem_name, O_CREAT | O_EXCL, 0600, 0);
    if (ctx->notify_sem == SEM_FAILED) {
        ipc_shm_unmap(&ctx->shm, ctx->shm_fd);
        free(ctx);
        return IPC_ERR_SEM;
    }

    ctx->notify_running = 0;
    *out = ctx;
    return IPC_OK;
}

ipc_err_t ipc_open(const char *name, ipc_ctx_t **out) {
    if (!name || !out) return IPC_ERR_INVALID;

    ipc_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return IPC_ERR_SHM;

    ipc_strlcpy0(ctx->shm_name, name, sizeof(ctx->shm_name));
    snprintf(ctx->sem_name, sizeof(ctx->sem_name), "%s_sem_%d", name, (int)getpid());

    ipc_err_t rc = ipc_shm_map(name, 0, 0, &ctx->shm, &ctx->shm_fd);
    if (rc != IPC_OK) { free(ctx); return rc; }

    /* 校验 header */
    ipc_header_t *hdr = ctx->shm.hdr;
    if (hdr->magic != IPC_MAGIC || hdr->shm_version != IPC_SHM_VERSION) {
        ipc_shm_unmap(&ctx->shm, ctx->shm_fd);
        free(ctx);
        return IPC_ERR_VERSION;
    }

    /* 创建本进程通知信号量 */
    ctx->notify_sem = sem_open(ctx->sem_name, O_CREAT, 0600, 0);
    if (ctx->notify_sem == SEM_FAILED) {
        ipc_shm_unmap(&ctx->shm, ctx->shm_fd);
        free(ctx);
        return IPC_ERR_SEM;
    }

    ctx->notify_running = 0;
    *out = ctx;
    return IPC_OK;
}

void ipc_close(ipc_ctx_t *ctx) {
    if (!ctx) return;
    /* 停止通知线程 */
    if (ctx->notify_running) {
        ctx->notify_running = 0;
        sem_post(ctx->notify_sem); /* 唤醒线程让其退出 */
        pthread_join(ctx->notify_thread, NULL);
        close(ctx->stop_pipe[0]);
        close(ctx->stop_pipe[1]);
    }
    /* 从订阅者表中注销自己 */
    ipc_shm_view_t *shm = &ctx->shm;
    if (shm->base) {
        for (int i = 0; i < IPC_MAX_SUBSCRIBERS; i++) {
            if (shm->subs[i].active && shm->subs[i].pid == getpid()) {
                shm->subs[i].active = 0;
            }
        }
    }
    sem_close(ctx->notify_sem);
    sem_unlink(ctx->sem_name);
    ipc_shm_unmap(&ctx->shm, ctx->shm_fd);
    free(ctx);
}

ipc_err_t ipc_destroy(const char *name) {
    if (!name) return IPC_ERR_INVALID;
    shm_unlink(name);
    /* 信号量由各进程 close 时清理，这里不强制删 */
    return IPC_OK;
}

/* ======================================================
 * 写入
 * ====================================================== */

ipc_err_t ipc_publish(ipc_ctx_t *ctx, const char *key,
                      const void *data, size_t len) {
    if (!ctx || !key || (!data && len > 0)) return IPC_ERR_INVALID;
    if (strlen(key) >= IPC_MAX_KEY_LEN)    return IPC_ERR_INVALID;

    ipc_shm_view_t *shm = &ctx->shm;

    if (pthread_rwlock_wrlock(shm->rwlock) != 0) return IPC_ERR_LOCK;

    ipc_entry_t *e = ipc_find_entry(shm, key);
    if (!e) {
        e = ipc_alloc_entry(shm, key);
        if (!e) {
            pthread_rwlock_unlock(shm->rwlock);
            return (shm->hdr->entry_count >= IPC_MAX_ENTRIES)
                   ? IPC_ERR_FULL : IPC_ERR_NO_SPACE;
        }
    }

    /* 为新值分配数据池空间（旧数据不回收，bump allocator 简单安全） */
    uint64_t new_off = (uint64_t)-1;
    if (len > 0) {
        new_off = data_pool_alloc(shm, len);
        if (new_off == (uint64_t)-1) {
            pthread_rwlock_unlock(shm->rwlock);
            return IPC_ERR_NO_SPACE;
        }
        memcpy(shm->data_pool + (new_off - SHM_DATA_OFFSET), data, len);
    }

    uint64_t new_ver = atomic_load_explicit(&e->version, memory_order_relaxed) + 1;
    e->data_offset  = new_off;
    e->data_len     = (uint32_t)len;
    e->timestamp_ns = ipc_now_ns();
    /* 版本号最后写，读者用它做双检锁 */
    atomic_store_explicit(&e->version, new_ver, memory_order_release);
    atomic_fetch_add_explicit(&shm->hdr->total_writes, 1, memory_order_relaxed);

    /* 推送变更事件到环形队列 */
    ipc_change_event_t ev;
    strncpy(ev.key, key, IPC_MAX_KEY_LEN - 1);
    ev.key[IPC_MAX_KEY_LEN-1] = '\0';
    ev.version      = new_ver;
    ev.timestamp_ns = e->timestamp_ns;
    ev.op           = IPC_OP_SET;
    ipc_ring_push(shm, &ev);

    pthread_rwlock_unlock(shm->rwlock);

    /* 通知所有订阅该 key 的进程 */
    notify_subscribers(ctx, key);

    return IPC_OK;
}

/* ======================================================
 * 读取
 * ====================================================== */

ipc_err_t ipc_get(ipc_ctx_t *ctx, const char *key,
                  void *buf, size_t buf_len, ipc_value_t *out) {
    if (!ctx || !key || !buf || !out) return IPC_ERR_INVALID;

    ipc_shm_view_t *shm = &ctx->shm;

    if (pthread_rwlock_rdlock(shm->rwlock) != 0) return IPC_ERR_LOCK;

    ipc_entry_t *e = ipc_find_entry(shm, key);
    if (!e) {
        pthread_rwlock_unlock(shm->rwlock);
        return IPC_ERR_NOT_FOUND;
    }

    /* 双检版本：读前记录版本，读后确认版本未变（防止并发写撕裂） */
    uint64_t ver_before = atomic_load_explicit(&e->version, memory_order_acquire);

    if (e->data_len > buf_len) {
        pthread_rwlock_unlock(shm->rwlock);
        return IPC_ERR_NO_SPACE;
    }

    size_t copy_len = e->data_len;
    if (copy_len > 0) {
        memcpy(buf, shm->data_pool + (e->data_offset - SHM_DATA_OFFSET), copy_len);
    }
    uint64_t ts  = e->timestamp_ns;

    atomic_thread_fence(memory_order_acquire);
    uint64_t ver_after = atomic_load_explicit(&e->version, memory_order_acquire);

    pthread_rwlock_unlock(shm->rwlock);

    if (ver_before != ver_after) {
        /* 数据被并发覆写，版本不一致，请调用者重试 */
        return IPC_ERR_LOCK;
    }

    out->data        = buf;
    out->data_len    = copy_len;
    out->version     = ver_before;
    out->timestamp_ns = ts;
    return IPC_OK;
}

/* ======================================================
 * 删除
 * ====================================================== */

ipc_err_t ipc_delete(ipc_ctx_t *ctx, const char *key) {
    if (!ctx || !key) return IPC_ERR_INVALID;

    ipc_shm_view_t *shm = &ctx->shm;
    if (pthread_rwlock_wrlock(shm->rwlock) != 0) return IPC_ERR_LOCK;

    ipc_entry_t *e = ipc_find_entry(shm, key);
    if (!e) { pthread_rwlock_unlock(shm->rwlock); return IPC_ERR_NOT_FOUND; }

    e->active = 0;
    uint64_t new_ver = atomic_load_explicit(&e->version, memory_order_relaxed) + 1;
    atomic_store_explicit(&e->version, new_ver, memory_order_release);

    ipc_change_event_t ev;
    strncpy(ev.key, key, IPC_MAX_KEY_LEN - 1);
    ev.key[IPC_MAX_KEY_LEN-1] = '\0';
    ev.version      = new_ver;
    ev.timestamp_ns = ipc_now_ns();
    ev.op           = IPC_OP_DEL;
    ipc_ring_push(shm, &ev);

    pthread_rwlock_unlock(shm->rwlock);
    notify_subscribers(ctx, key);
    return IPC_OK;
}

/* ======================================================
 * 等待变更（阻塞）
 * ====================================================== */

ipc_err_t ipc_wait_change(ipc_ctx_t *ctx, int timeout_ms,
                          ipc_change_event_t *event) {
    if (!ctx || !event) return IPC_ERR_INVALID;

    if (timeout_ms < 0) {
        if (sem_wait(ctx->notify_sem) != 0) return IPC_ERR_SEM;
    } else {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec  += timeout_ms / 1000;
        ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        int rc = sem_timedwait(ctx->notify_sem, &ts);
        if (rc != 0) {
            return (errno == ETIMEDOUT) ? IPC_ERR_TIMEOUT : IPC_ERR_SEM;
        }
    }

    /* 从环形队列弹出事件 */
    if (ipc_ring_pop(&ctx->shm, event) != 0) {
        /* 队列空（可能是 spurious wakeup），返回空事件 */
        memset(event, 0, sizeof(*event));
    }
    return IPC_OK;
}

/* ======================================================
 * 订阅 — 后台通知线程
 * ====================================================== */

static void *notify_thread_func(void *arg) {
    ipc_ctx_t *ctx = (ipc_ctx_t *)arg;

    while (ctx->notify_running) {
        /* 阻塞等待通知，超时 200ms 重新检查退出标志 */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 200000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }

        int rc = sem_timedwait(ctx->notify_sem, &ts);
        if (rc != 0 && errno == ETIMEDOUT) continue;
        if (!ctx->notify_running) break;

        /* 弹出所有待处理事件 */
        ipc_change_event_t ev;
        while (ipc_ring_pop(&ctx->shm, &ev) == 0) {
            /* 匹配本进程注册的回调 */
            for (int i = 0; i < ctx->sub_count; i++) {
                if (!ctx->sub_slots[i].active) continue;
                const char *filter = ctx->sub_slots[i].key;
                if (strcmp(filter, "*") != 0 &&
                    strncmp(filter, ev.key, IPC_MAX_KEY_LEN) != 0)
                    continue;

                /* 读取最新值传给回调 */
                uint8_t  tmp_buf[4096];
                ipc_value_t val;
                ipc_err_t err = ipc_get(ctx, ev.key, tmp_buf, sizeof(tmp_buf), &val);
                if (err == IPC_OK) {
                    ctx->sub_slots[i].cb(ev.key, &val,
                                         ctx->sub_slots[i].userdata);
                }
            }
        }
    }
    return NULL;
}

ipc_err_t ipc_subscribe(ipc_ctx_t *ctx, const char *key,
                        ipc_callback_t cb, void *userdata) {
    if (!ctx || !key || !cb) return IPC_ERR_INVALID;

    /* 注册到本地回调槽 */
    if (ctx->sub_count >= IPC_MAX_ENTRIES) return IPC_ERR_FULL;
    int slot = ctx->sub_count++;
    ipc_strlcpy0(ctx->sub_slots[slot].key, key, sizeof(ctx->sub_slots[slot].key));
    ctx->sub_slots[slot].cb       = cb;
    ctx->sub_slots[slot].userdata = userdata;
    ctx->sub_slots[slot].active   = 1;

    /* 在共享内存订阅者表中注册（其他进程需要知道本进程关心什么 key） */
    ipc_shm_view_t *shm = &ctx->shm;
    pthread_rwlock_wrlock(shm->rwlock);
    int found = 0;
    for (int i = 0; i < IPC_MAX_SUBSCRIBERS; i++) {
        if (!shm->subs[i].active) {
            shm->subs[i].pid = getpid();
            ipc_strlcpy0(shm->subs[i].key_filter, key, sizeof(shm->subs[i].key_filter));
            ipc_strlcpy0(shm->subs[i].sem_name, ctx->sem_name, sizeof(shm->subs[i].sem_name));
            shm->subs[i].active = 1;
            found = 1;
            break;
        }
    }
    pthread_rwlock_unlock(shm->rwlock);
    if (!found) return IPC_ERR_FULL;

    /* 首次订阅时启动后台通知线程 */
    if (!ctx->notify_running) {
        ctx->notify_running = 1;
        if (pthread_create(&ctx->notify_thread, NULL,
                           notify_thread_func, ctx) != 0) {
            ctx->notify_running = 0;
            return IPC_ERR_SEM;
        }
    }

    return IPC_OK;
}

ipc_err_t ipc_unsubscribe(ipc_ctx_t *ctx, const char *key) {
    if (!ctx || !key) return IPC_ERR_INVALID;

    /* 禁用本地回调槽 */
    for (int i = 0; i < ctx->sub_count; i++) {
        if (ctx->sub_slots[i].active &&
            strncmp(ctx->sub_slots[i].key, key, IPC_MAX_KEY_LEN) == 0) {
            ctx->sub_slots[i].active = 0;
        }
    }

    /* 从共享内存订阅者表移除 */
    ipc_shm_view_t *shm = &ctx->shm;
    pthread_rwlock_wrlock(shm->rwlock);
    for (int i = 0; i < IPC_MAX_SUBSCRIBERS; i++) {
        if (shm->subs[i].active && shm->subs[i].pid == getpid() &&
            strncmp(shm->subs[i].key_filter, key, IPC_MAX_KEY_LEN) == 0) {
            shm->subs[i].active = 0;
        }
    }
    pthread_rwlock_unlock(shm->rwlock);

    return IPC_OK;
}

/* ======================================================
 * 统计
 * ====================================================== */

ipc_err_t ipc_stat(ipc_ctx_t *ctx, ipc_stat_t *stat) {
    if (!ctx || !stat) return IPC_ERR_INVALID;
    ipc_shm_view_t *shm = &ctx->shm;

    pthread_rwlock_rdlock(shm->rwlock);
    stat->entry_count      = shm->hdr->entry_count;
    stat->shm_used         = (size_t)(shm->hdr->data_end - SHM_DATA_OFFSET);
    stat->shm_total        = shm->hdr->shm_size;
    stat->total_writes     = atomic_load(&shm->hdr->total_writes);

    uint32_t subs = 0;
    for (int i = 0; i < IPC_MAX_SUBSCRIBERS; i++)
        if (shm->subs[i].active) subs++;
    stat->subscriber_count = subs;
    pthread_rwlock_unlock(shm->rwlock);

    return IPC_OK;
}
