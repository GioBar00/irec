//
// Created by jelte on 26-6-23.
//
#define _GNU_SOURCE
#include "irec_vm_fns.h"
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "ubpf_int.h"
#include <ubpf_config.h>
#include "shared.h"
#if defined(UBPF_HAS_ELF_H)
#include <time.h>
#include <stdarg.h>

#endif

void assert_fail(ubpf_ctx_t* ctx, const char *__assertion, const char *__file,
                 unsigned int __line, const char *__function){
    __assert_fail(__assertion, __file, __line, __function);

}

int print(ubpf_ctx_t* ctx, const char * fmt, ...){
// This is a security risk, only allow printing in debug mode.
#ifdef IREC_DEBUG
    va_list ap;
    int res = 0;
    va_start(ap, fmt);
    res = vprintf(fmt, ap);
    va_end(ap);
    return res;
#endif
}

//#ifndef __GLIBC__
void*
memfrob(void* s, size_t n)
{
    for (int i = 0; i < n; i++) {
        ((char*)s)[i] ^= 42;
    }
    return s;
}
//#endif

uint64_t
gather_bytes(ubpf_ctx_t * ctx UNUSED, uint8_t a, uint8_t b, uint8_t c, uint8_t d, uint8_t e)
{
    return ((uint64_t)a << 32) | ((uint32_t)b << 24) | ((uint32_t)c << 16) | ((uint16_t)d << 8) | e;
}


void
trash_registers(void)
{
    /* Overwrite all caller-save registers */
#if __x86_64__
    asm("mov $0xf0, %rax;"
        "mov $0xf1, %rcx;"
        "mov $0xf2, %rdx;"
        "mov $0xf3, %rsi;"
        "mov $0xf4, %rdi;"
        "mov $0xf5, %r8;"
        "mov $0xf6, %r9;"
        "mov $0xf7, %r10;"
        "mov $0xf8, %r11;");
#elif __aarch64__
    asm("mov w0, #0xf0;"
        "mov w1, #0xf1;"
        "mov w2, #0xf2;"
        "mov w3, #0xf3;"
        "mov w4, #0xf4;"
        "mov w5, #0xf5;"
        "mov w6, #0xf6;"
        "mov w7, #0xf7;"
        "mov w8, #0xf8;"
        "mov w9, #0xf9;"
        "mov w10, #0xfa;"
        "mov w11, #0xfb;"
        "mov w12, #0xfc;"
        "mov w13, #0xfd;"
        "mov w14, #0xfe;"
        "mov w15, #0xff;" ::
            : "w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9", "w10", "w11", "w12", "w13", "w14", "w15");
#else
    fprintf(stderr, "trash_registers not implemented for this architecture.\n");
    exit(1);
#endif
}

uint32_t
sqrti(ubpf_ctx_t* ctx UNUSED, uint32_t x)
{
    return sqrt(x);
}

uint64_t
unwind(ubpf_ctx_t* ctx UNUSED, uint64_t i)
{
    return i;
}

void*
bpf_map_lookup_elem_impl(ubpf_ctx_t* ctx UNUSED, struct bpf_map* map, const void* key)
{
    map_entry_t* map_entry = (map_entry_t*)map;
    if (map_entry->map_definition.type == BPF_MAP_TYPE_ARRAY) {
        uint32_t index = *(uint32_t*)key;
        if (index >= map_entry->map_definition.max_entries) {
            return NULL;
        }
        return map_entry->array + index * map_entry->map_definition.value_size;
    } else {
        fprintf(stderr, "bpf_map_lookup_elem not implemented for this map type.\n");
        exit(1);
    }
    return NULL;
}

int
bpf_map_update_elem_impl(ubpf_ctx_t* ctx UNUSED, struct bpf_map* map, const void* key, const void* value, uint64_t flags)
{
    map_entry_t* map_entry = (map_entry_t*)map;
    (void)flags; // unused
    if (map_entry->map_definition.type == BPF_MAP_TYPE_ARRAY) {
        uint32_t index = *(uint32_t*)key;
        if (index >= map_entry->map_definition.max_entries) {
            return -1;
        }
        memcpy(
                map_entry->array + index * map_entry->map_definition.value_size,
                value,
                map_entry->map_definition.value_size);
        return 0;
    } else {
        fprintf(stderr, "bpf_map_update_elem not implemented for this map type.\n");
        exit(1);
    }
    return 0;
}

int
bpf_map_delete_elem_impl(ubpf_ctx_t* ctx UNUSED, struct bpf_map* map, const void* key)
{
    map_entry_t* map_entry = (map_entry_t*)map;
    if (map_entry->map_definition.type == BPF_MAP_TYPE_ARRAY) {
        uint32_t index = *(uint32_t*)key;
        if (index >= map_entry->map_definition.max_entries) {
            return -1;
        }
        memset(
                map_entry->array + index * map_entry->map_definition.value_size, 0, map_entry->map_definition.value_size);
        return 0;
    } else {
        fprintf(stderr, "bpf_map_delete_elem not implemented for this map type.\n");
        exit(1);
    }
}
int test (ubpf_ctx_t* ctx UNUSED, int x)
{
    printf("PRINTING FROM UBPF: %d, num insts: %d\n", x,ctx->vm->num_insts);
    return 50;
}

void *get_arg(ubpf_ctx_t* ctx , int type) {
    int i;
    uint8_t *ret_arg;
    // fprintf(stderr, "Ptr ctx at %s call --> %p\n", __FUNCTION__, vm_ctx);

    args_t *check_args = ctx->args;
    if (!check_args) {
        return NULL;
    }

    for (i = 0; i < check_args->nargs; i++) {
        if (check_args->args[i].type == type) {
            ret_arg = mem_alloc(&ctx->mem.mgr_heap, check_args->args[i].len);
            if (!ret_arg) return NULL;
            memcpy(ret_arg, check_args->args[i].arg, check_args->args[i].len);
            return ret_arg;
        }
    }
    return NULL;
}

//void *ctx_malloc(ubpf_ctx_t *ctx, size_t size);
void *ctx_malloc(ubpf_ctx_t *ctx, size_t size) {
    return mem_alloc(&ctx->mem.mgr_heap, size);
}
//void *ctx_calloc(ubpf_ctx_t *ctx, uint64_t nmemb, uint64_t size);
void *ctx_calloc(ubpf_ctx_t *ctx, uint64_t nmemb, uint64_t size) {
    void *ptr;
    ptr = ctx_malloc(ctx, nmemb * size);

    if (!ptr) return NULL;
    memset(ptr, 0, nmemb * size);
    return ptr;
}

//void *ctx_realloc(ubpf_ctx_t *ctx, void *ptr, uint64_t size);
void *ctx_realloc(ubpf_ctx_t *ctx, void *ptr, uint64_t size) {
    return mem_realloc(&ctx->mem.mgr_heap, ptr, size);
}
//void ctx_free(ubpf_ctx_t *ctx, UNUSED void *ptr);
void ctx_free(ubpf_ctx_t *ctx, UNUSED void *ptr) {
    mem_free(&ctx->mem.mgr_heap, ptr);
}
//void *ctx_shmnew(ubpf_ctx_t *ctx, key_t key, uint64_t size);
void *ctx_shmnew(ubpf_ctx_t *ctx, key_t key, uint64_t size) {
    void *addr;
    addr = shared_new(&ctx->mem.mgr_shared_heap,
                      &ctx->mem.shared_blocks, key, size);
    if (addr)
        memset(addr, 0, size);
    return addr;
}
//void *ctx_shmget(ubpf_ctx_t *ctx, key_t key);
void *ctx_shmget(ubpf_ctx_t *ctx, key_t key) {
    return shared_get(&ctx->mem.mgr_shared_heap,
                      &ctx->mem.shared_blocks, key);
}
//void ctx_shmrm(ubpf_ctx_t *ctx, key_t key);
void ctx_shmrm(ubpf_ctx_t *ctx, key_t key) {
    shared_rm(&ctx->mem.mgr_shared_heap,
              &ctx->mem.shared_blocks, key);
}
//int get_time(ubpf_ctx_t *ctx UNUSED, struct timespec *spec);
int get_time(ubpf_ctx_t *ctx UNUSED, struct timespec *spec) {

    memset(spec, 0, sizeof(*spec));

    if (clock_gettime(CLOCK_MONOTONIC, spec) != 0) {
        perror("Clock gettime");
        return -1;
    }

    return 0;
}

//int get_realtime(ubpf_ctx_t *ctx UNUSED, struct timespec *spec);
int get_realtime(ubpf_ctx_t *ctx UNUSED, struct timespec *spec) {

    if (!spec) return -1;

    if (clock_gettime(CLOCK_REALTIME, spec) != 0) {
        perror("Clock gettime");
        return -1;
    }
    return 0;
}
