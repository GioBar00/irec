//
// Created by jelte on 9-6-23.
//
#ifndef UBPF_CONTEXT
#define UBPF_CONTEXT
#include <stdint.h>
#include <stdio.h>
#include "memory.h"
#include "tommy.h"

#define kind_null 0
#define kind_ptr 1
#define kind_primitive 2
#define kind_hidden 3

#define entry_arg_null {.arg = NULL, .len = 0, .kind = 0, .type = 0}
#define entry_is_null(entry) (((entry)->arg == NULL) && ((entry)->len == 0) && ((entry)->kind == 0) && ((entry)->type == 0))


struct entry_arg {
    void *arg;
    size_t len;
    short kind;
    int type; // custom type defined by the protocol insertion point
};
typedef struct entry_arg entry_arg_t;

typedef struct {
    entry_arg_t *args;
    int nargs;
} args_t;





struct ubpf_ctx {
    struct ubpf_vm* vm;
    struct {
        uint8_t has_heap: 1;
        uint8_t has_shared_heap: 1;
        size_t len;
        uint8_t *master_block;

        struct memory_manager mgr_heap;
        struct memory_manager mgr_shared_heap;
        tommy_hashdyn shared_blocks;
    } mem;
    void * bufVM;
    args_t *args;
};
typedef struct ubpf_ctx ubpf_ctx_t;

#endif

