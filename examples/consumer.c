/**
 * consumer.c — 消费者示例：订阅变更回调 + 轮询读取
 *
 * 用法：
 *   ./consumer          （在 producer 运行后启动）
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <inttypes.h>
#include "ipcsync.h"

static volatile int running = 1;
static void sig_handler(int sig) { (void)sig; running = 0; }

/* 变更回调：在后台线程中被调用 */
static void on_change(const char *key, const ipc_value_t *val, void *userdata) {
    (void)userdata;
    printf("[callback] key='%s' version=%"PRIu64" len=%zu  data=",
           key, val->version, val->data_len);

    /* 尝试作为字符串打印；否则打印十六进制 */
    const uint8_t *d = (const uint8_t *)val->data;
    int printable = 1;
    for (size_t i = 0; i < val->data_len; i++) {
        if (d[i] != 0 && (d[i] < 0x20 || d[i] > 0x7e)) { printable = 0; break; }
    }
    if (printable && val->data_len > 0) {
        printf("\"%.*s\"", (int)val->data_len, (const char *)val->data);
    } else {
        for (size_t i = 0; i < val->data_len && i < 8; i++)
            printf("%02x ", d[i]);
    }
    printf("\n");
}

int main(void) {
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    ipc_ctx_t *ctx = NULL;
    ipc_err_t rc = ipc_open("/demo_sync", &ctx);
    if (rc != IPC_OK) {
        fprintf(stderr, "ipc_open failed: %s\n", ipc_strerror(rc));
        fprintf(stderr, "请先启动 producer\n");
        return 1;
    }

    printf("[consumer] pid=%d，连接成功\n", (int)getpid());

    /* 订阅所有 key 的变更 */
    rc = ipc_subscribe(ctx, "*", on_change, NULL);
    if (rc != IPC_OK) {
        fprintf(stderr, "subscribe failed: %s\n", ipc_strerror(rc));
        ipc_close(ctx);
        return 1;
    }

    printf("[consumer] 已订阅所有 key，等待变更通知...\n");

    /* 主循环：每 3 秒主动 poll 一次并打印统计 */
    int poll_cnt = 0;
    while (running) {
        sleep(3);
        if (!running) break;
        poll_cnt++;

        /* 主动读取 cpu_usage */
        char buf[128];
        ipc_value_t val;
        rc = ipc_get(ctx, "cpu_usage", buf, sizeof(buf), &val);
        if (rc == IPC_OK) {
            printf("[poll #%d] cpu_usage = \"%s\" (ver=%"PRIu64")\n",
                   poll_cnt, buf, val.version);
        }

        /* 主动读取 mem_free_mb */
        uint64_t mem;
        rc = ipc_get(ctx, "mem_free_mb", &mem, sizeof(mem), &val);
        if (rc == IPC_OK) {
            printf("[poll #%d] mem_free_mb = %"PRIu64" MB (ver=%"PRIu64")\n",
                   poll_cnt, mem, val.version);
        }

        /* 打印统计 */
        ipc_stat_t stat;
        if (ipc_stat(ctx, &stat) == IPC_OK) {
            printf("[stat] entries=%u subs=%u shm_used=%zu/%zu total_writes=%"PRIu64"\n",
                   stat.entry_count, stat.subscriber_count,
                   stat.shm_used, stat.shm_total, stat.total_writes);
        }
        printf("---\n");
    }

    printf("[consumer] 退出\n");
    ipc_close(ctx);
    return 0;
}
