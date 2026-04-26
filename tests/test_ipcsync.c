/**
 * test_ipcsync.c — 基本功能单元测试
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <inttypes.h>
#include <sys/wait.h>
#include "../include/ipcsync.h"

#define SHM_NAME "/test_ipcsync_tmp"
#define PASS "\033[32mPASS\033[0m"
#define FAIL "\033[31mFAIL\033[0m"

static int test_count = 0, fail_count = 0;

#define CHECK(cond, msg) do { \
    test_count++; \
    if (!(cond)) { fail_count++; printf("[" FAIL "] %s\n", msg); } \
    else { printf("[" PASS "] %s\n", msg); } \
} while(0)

/* ===== 测试 1：创建与打开 ===== */
static void test_create_open(void) {
    printf("\n--- test_create_open ---\n");
    ipc_destroy(SHM_NAME); /* 清理残留 */

    ipc_ctx_t *ctx = NULL;
    ipc_err_t rc = ipc_create(SHM_NAME, 0, &ctx);
    CHECK(rc == IPC_OK, "ipc_create returns IPC_OK");
    CHECK(ctx != NULL, "ipc_create sets ctx pointer");

    /* 重复创建应失败 */
    ipc_ctx_t *ctx2 = NULL;
    rc = ipc_create(SHM_NAME, 0, &ctx2);
    CHECK(rc == IPC_ERR_EXISTS, "duplicate ipc_create returns IPC_ERR_EXISTS");

    /* open 成功 */
    ipc_ctx_t *ctx3 = NULL;
    rc = ipc_open(SHM_NAME, &ctx3);
    CHECK(rc == IPC_OK, "ipc_open returns IPC_OK");

    ipc_close(ctx3);
    ipc_close(ctx);
    ipc_destroy(SHM_NAME);
}

/* ===== 测试 2：publish / get ===== */
static void test_publish_get(void) {
    printf("\n--- test_publish_get ---\n");
    ipc_destroy(SHM_NAME);

    ipc_ctx_t *ctx = NULL;
    ipc_create(SHM_NAME, 0, &ctx);

    /* 写入字符串 */
    const char *val1 = "hello, world";
    ipc_err_t rc = ipc_publish(ctx, "greeting", val1, strlen(val1) + 1);
    CHECK(rc == IPC_OK, "publish string");

    char buf[256];
    ipc_value_t res;
    rc = ipc_get(ctx, "greeting", buf, sizeof(buf), &res);
    CHECK(rc == IPC_OK, "get string");
    CHECK(strcmp(buf, "hello, world") == 0, "get string value matches");
    CHECK(res.version == 1, "version == 1 after first publish");

    /* 更新同一 key */
    const char *val2 = "updated value";
    ipc_publish(ctx, "greeting", val2, strlen(val2) + 1);
    rc = ipc_get(ctx, "greeting", buf, sizeof(buf), &res);
    CHECK(rc == IPC_OK && strcmp(buf, "updated value") == 0, "get after update");
    CHECK(res.version == 2, "version == 2 after update");

    /* 写入二进制数据 */
    uint64_t num = 0xDEADBEEFCAFEBABEULL;
    ipc_publish(ctx, "binary_key", &num, sizeof(num));
    uint64_t out_num;
    rc = ipc_get(ctx, "binary_key", &out_num, sizeof(out_num), &res);
    CHECK(rc == IPC_OK && out_num == num, "binary get matches");

    /* 查询不存在的 key */
    rc = ipc_get(ctx, "nonexistent", buf, sizeof(buf), &res);
    CHECK(rc == IPC_ERR_NOT_FOUND, "get nonexistent key returns NOT_FOUND");

    ipc_close(ctx);
    ipc_destroy(SHM_NAME);
}

/* ===== 测试 3：delete ===== */
static void test_delete(void) {
    printf("\n--- test_delete ---\n");
    ipc_destroy(SHM_NAME);

    ipc_ctx_t *ctx = NULL;
    ipc_create(SHM_NAME, 0, &ctx);

    ipc_publish(ctx, "to_delete", "abc", 4);
    ipc_err_t rc = ipc_delete(ctx, "to_delete");
    CHECK(rc == IPC_OK, "delete existing key");

    char buf[64]; ipc_value_t res;
    rc = ipc_get(ctx, "to_delete", buf, sizeof(buf), &res);
    CHECK(rc == IPC_ERR_NOT_FOUND, "get deleted key returns NOT_FOUND");

    rc = ipc_delete(ctx, "ghost_key");
    CHECK(rc == IPC_ERR_NOT_FOUND, "delete nonexistent returns NOT_FOUND");

    ipc_close(ctx);
    ipc_destroy(SHM_NAME);
}

/* ===== 测试 4：多 key ===== */
static void test_multi_key(void) {
    printf("\n--- test_multi_key ---\n");
    ipc_destroy(SHM_NAME);

    ipc_ctx_t *ctx = NULL;
    ipc_create(SHM_NAME, 0, &ctx);

    char key[32], val[32];
    for (int i = 0; i < 10; i++) {
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(val, sizeof(val), "value_%03d", i);
        ipc_publish(ctx, key, val, strlen(val) + 1);
    }

    int ok = 1;
    char buf[64]; ipc_value_t res;
    for (int i = 0; i < 10; i++) {
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(val, sizeof(val), "value_%03d", i);
        if (ipc_get(ctx, key, buf, sizeof(buf), &res) != IPC_OK ||
            strcmp(buf, val) != 0) { ok = 0; break; }
    }
    CHECK(ok, "10 keys all match");

    ipc_stat_t stat;
    ipc_stat(ctx, &stat);
    CHECK(stat.entry_count >= 10, "entry_count >= 10");
    CHECK(stat.total_writes == 10, "total_writes == 10");

    ipc_close(ctx);
    ipc_destroy(SHM_NAME);
}

/* ===== 测试 5：跨进程读写（fork）===== */
static void test_cross_process(void) {
    printf("\n--- test_cross_process (fork) ---\n");
    ipc_destroy(SHM_NAME);

    ipc_ctx_t *ctx = NULL;
    ipc_create(SHM_NAME, 0, &ctx);

    pid_t pid = fork();
    if (pid == 0) {
        /* 子进程：open + publish */
        ipc_ctx_t *child_ctx = NULL;
        ipc_err_t rc = ipc_open(SHM_NAME, &child_ctx);
        if (rc != IPC_OK) exit(1);
        const char *msg = "from_child_process";
        rc = ipc_publish(child_ctx, "cross_key", msg, strlen(msg) + 1);
        ipc_close(child_ctx);
        exit(rc == IPC_OK ? 0 : 2);
    }

    /* 父进程：等子进程写完，再读 */
    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0,
          "child process publish succeeded");

    char buf[64]; ipc_value_t res;
    ipc_err_t rc = ipc_get(ctx, "cross_key", buf, sizeof(buf), &res);
    CHECK(rc == IPC_OK, "parent can read child-written key");
    CHECK(strcmp(buf, "from_child_process") == 0, "cross-process value matches");

    ipc_close(ctx);
    ipc_destroy(SHM_NAME);
}

/* ===== 测试 6：wait_change ===== */
static void test_wait_change(void) {
    printf("\n--- test_wait_change ---\n");
    ipc_destroy(SHM_NAME);

    ipc_ctx_t *ctx_w = NULL, *ctx_r = NULL;
    ipc_create(SHM_NAME, 0, &ctx_w);
    ipc_open(SHM_NAME, &ctx_r);

    /* 先写入触发通知 */
    ipc_publish(ctx_w, "trigger", "go", 3);

    ipc_change_event_t ev;
    ipc_err_t rc = ipc_wait_change(ctx_r, 1000, &ev);
    /* 注：ctx_r 没有订阅，通知打到 ctx_w 的 sem；
     * 这里测超时路径 */
    CHECK(rc == IPC_ERR_TIMEOUT || rc == IPC_OK,
          "wait_change returns OK or TIMEOUT");

    /* 测超时：等 100ms 肯定超时 */
    rc = ipc_wait_change(ctx_r, 100, &ev);
    CHECK(rc == IPC_ERR_TIMEOUT, "wait_change 100ms timeout");

    ipc_close(ctx_r);
    ipc_close(ctx_w);
    ipc_destroy(SHM_NAME);
}

/* ===== 汇总 ===== */
int main(void) {
    printf("====== libipcsync 单元测试 ======\n");

    test_create_open();
    test_publish_get();
    test_delete();
    test_multi_key();
    test_cross_process();
    test_wait_change();

    printf("\n====== 结果: %d/%d 通过 ======\n",
           test_count - fail_count, test_count);
    return fail_count > 0 ? 1 : 0;
}
