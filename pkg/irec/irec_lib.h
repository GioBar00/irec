//
// Created by jelte on 28-6-23.
//

#ifndef LIBIREC_IREC_MAIN_H
#define LIBIREC_IREC_MAIN_H
#include <inttypes.h>

#include <stdlib.h>

#include <stdbool.h>

#include "context.h"

#include "shared.h"
struct buffer_info {
    void *buffer;
    size_t buffer_len;
};

int prepare_mem(struct ubpf_ctx *ctx, struct ubpf_vm *vm, void *data_buffer, size_t buffer_len);

struct ubpf_ctx* new_ctx();

int destroy_ctx(struct ubpf_ctx *ctx);

struct ubpf_vm* create_vm(void* code, size_t code_len, struct ubpf_ctx *ctx, bool jit);

struct execution_result exec_vm(struct ubpf_vm *vm, struct ubpf_ctx *ctx, bool jit);

int destroy_mem(ubpf_ctx_t *ctx);

void destroy_vm(struct ubpf_vm* vm);
#endif //LIBIREC_IREC_MAIN_H
