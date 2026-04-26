/**
 * producer.c — 生产者示例：每秒写入 CPU 使用率和内存数据
 *
 * 用法：
 *   ./producer          （先运行，创建共享内存）
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include "ipcsync.h"

static volatile int running = 1;
static void sig_handler(int sig) { (void)sig; running = 0; }

int main(void) {
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    ipc_ctx_t *ctx = NULL;
    ipc_err_t rc = ipc_create("/demo_sync", 0, &ctx);
    if (rc == IPC_ERR_EXISTS) {
        /* 已有其他生产者，直接 open */
        rc = ipc_open("/demo_sync", &ctx);
    }
    if (rc != IPC_OK) {
        fprintf(stderr, "ipc_create/open failed: %s\n", ipc_strerror(rc));
        return 1;
    }

    printf("[producer] pid=%d, 开始写入数据，按 Ctrl+C 停止\n", (int)getpid());

    int tick = 0;
    while (running) {
        /* 模拟 CPU 使用率（浮点，5 字节字符串）*/
        char cpu_str[32];
        double cpu = 20.0 + (tick % 60);
        snprintf(cpu_str, sizeof(cpu_str), "%.1f%%", cpu);
        rc = ipc_publish(ctx, "cpu_usage", cpu_str, strlen(cpu_str) + 1);
        if (rc != IPC_OK)
            fprintf(stderr, "publish cpu_usage: %s\n", ipc_strerror(rc));

        /* 模拟内存剩余（整型）*/
        uint64_t mem_free = 4096 - (uint64_t)(tick % 1024);
        rc = ipc_publish(ctx, "mem_free_mb", &mem_free, sizeof(mem_free));
        if (rc != IPC_OK)
            fprintf(stderr, "publish mem_free_mb: %s\n", ipc_strerror(rc));

        /* 每 5 次写入一次状态字符串 */
        if (tick % 5 == 0) {
            const char *status = (tick % 10 == 0) ? "healthy" : "warn";
            ipc_publish(ctx, "system_status", status, strlen(status) + 1);
        }

        printf("[producer] tick=%d  cpu=%s  mem_free=%llu MB\n",
               tick, cpu_str, (unsigned long long)mem_free);

        tick++;
        sleep(1);
    }

    printf("[producer] 退出，清理共享内存\n");
    ipc_close(ctx);
    ipc_destroy("/demo_sync");
    return 0;
}
