//
// Created by jelte on 26-6-23.
//

#ifndef IREC_VM_FN_H
#define IREC_VM_FN_H
#include "ubpf.h"
#include "context.h"
#include "bpf.h"

typedef struct _map_entry
{
    struct bpf_map_def map_definition;
    const char* map_name;
    union
    {
        uint8_t* array;
    };
} map_entry_t;

void assert_fail(ubpf_ctx_t* ctx, const char *__assertion, const char *__file,
                        unsigned int __line, const char *__function);

int print(ubpf_ctx_t* ctx, const char * fmt, ...);

//#ifndef __GLIBC__
void* memfrob(void* s, size_t n);
//#endif

uint64_t gather_bytes(ubpf_ctx_t * ctx UNUSED, uint8_t a, uint8_t b, uint8_t c, uint8_t d, uint8_t e);


void trash_registers(void);

uint32_t sqrti(ubpf_ctx_t* ctx UNUSED, uint32_t x);

uint64_t unwind(ubpf_ctx_t* ctx UNUSED, uint64_t i);

void* bpf_map_lookup_elem_impl(ubpf_ctx_t* ctx UNUSED, struct bpf_map* map, const void* key);

int bpf_map_update_elem_impl(ubpf_ctx_t* ctx UNUSED, struct bpf_map* map, const void* key, const void* value, uint64_t flags);

int bpf_map_delete_elem_impl(ubpf_ctx_t* ctx UNUSED, struct bpf_map* map, const void* key);

int test (ubpf_ctx_t* ctx UNUSED, int x);

void *get_arg(ubpf_ctx_t* ctx , int type);

void *ctx_malloc(ubpf_ctx_t *ctx, size_t size);

void *ctx_calloc(ubpf_ctx_t *ctx, uint64_t nmemb, uint64_t size);

void *ctx_realloc(ubpf_ctx_t *ctx, void *ptr, uint64_t size);

void ctx_free(ubpf_ctx_t *ctx, UNUSED void *ptr);

void *ctx_shmnew(ubpf_ctx_t *ctx, key_t key, uint64_t size);

void *ctx_shmget(ubpf_ctx_t *ctx, key_t key);

void ctx_shmrm(ubpf_ctx_t *ctx, key_t key);

int get_time(ubpf_ctx_t *ctx UNUSED, struct timespec *spec);

int get_realtime(ubpf_ctx_t *ctx UNUSED, struct timespec *spec);
#endif