CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -std=c11 -D_GNU_SOURCE \
           -I$(CURDIR)/include
LDFLAGS = -lpthread -lrt

SRC     = src/ipcsync.c
OBJ     = build/ipcsync.o
LIB     = build/libipcsync.a

EXAMPLES     = build/producer build/consumer
TEST_BIN     = build/test_ipcsync

.PHONY: all clean test examples

all: $(LIB) examples

$(LIB): $(OBJ)
	@mkdir -p build
	ar rcs $@ $^

build/ipcsync.o: src/ipcsync.c include/ipcsync.h src/ipcsync_internal.h
	@mkdir -p build
	$(CC) $(CFLAGS) -c $< -o $@

# ===== 示例 =====
examples: $(EXAMPLES)

build/producer: examples/producer.c $(LIB)
	$(CC) $(CFLAGS) $< -L./build -lipcsync $(LDFLAGS) -o $@

build/consumer: examples/consumer.c $(LIB)
	$(CC) $(CFLAGS) $< -L./build -lipcsync $(LDFLAGS) -o $@

# ===== 测试 =====
build/test_ipcsync: tests/test_ipcsync.c $(LIB)
	$(CC) $(CFLAGS) $< -L./build -lipcsync $(LDFLAGS) -o $@

test: build/test_ipcsync
	@echo "--- 清理残留 shm ---"
	@rm -f /dev/shm/test_ipcsync_tmp* 2>/dev/null; true
	./build/test_ipcsync

clean:
	rm -rf build/
