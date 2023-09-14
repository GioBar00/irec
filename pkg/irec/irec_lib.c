// Copyright (c) 2015 Big Switch Networks, Inc
// SPDX-License-Identifier: Apache-2.0

/*
 * Copyright 2015 Big Switch Networks, Inc
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ubpf_config.h>

#define _GNU_SOURCE
#include <inttypes.h>
#include "irec_lib.h"
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <errno.h>
#include <math.h>
#include "ubpf.h"
#include "ubpf_int.h"
#include "context.h"
#include "bpf.h"
#include "shared.h"
#include "irec_vm_fns.h"

#if defined(UBPF_HAS_ELF_H)
#include <elf.h>
#include <time.h>
#include <stdarg.h>

#endif

void
ubpf_set_register_offset(int x);
static void*
readfile(const char* path, size_t maxlen, size_t* len);
static void
register_functions(struct ubpf_vm* vm);

struct beacon_result *_beacon_result;
unsigned int _beacon_len;



#define MAX_SIZE_ARGS 512
#define MAX_HEAP 268435456

static map_entry_t* _map_entries = NULL;
static int _map_entries_count = 0;
static int _map_entries_capacity = 0;
uint64_t
do_map_relocation(
        void* user_context,
        const uint8_t* map_data,
        uint64_t map_data_size,
        const char* symbol_name,
        uint64_t symbol_offset,
        uint64_t symbol_size)
{
    struct bpf_map_def map_definition = *(struct bpf_map_def*)(map_data + symbol_offset);
    (void)user_context;  // unused
    (void)symbol_offset; // unused
    (void)map_data_size; // unused

    if (symbol_size < sizeof(struct bpf_map_def)) {
        fprintf(stderr, "Invalid map size: %d\n", (int)symbol_size);
        return 0;
    }

    if (map_definition.type != BPF_MAP_TYPE_ARRAY) {
        fprintf(stderr, "Unsupported map type %d\n", map_definition.type);
        return 0;
    }

    if (map_definition.key_size != sizeof(uint32_t)) {
        fprintf(stderr, "Unsupported key size %d\n", map_definition.key_size);
        return 0;
    }

    for (int index = 0; index < _map_entries_count; index++) {
        if (strcmp(_map_entries[index].map_name, symbol_name) == 0) {
            return (uint64_t)&_map_entries[index];
        }
    }

    if (_map_entries_count == _map_entries_capacity) {
        _map_entries_capacity = _map_entries_capacity ? _map_entries_capacity * 2 : 4;
        _map_entries = realloc(_map_entries, _map_entries_capacity * sizeof(map_entry_t));
    }

    _map_entries[_map_entries_count].map_definition = map_definition;
    _map_entries[_map_entries_count].map_name = strdup(symbol_name);
    _map_entries[_map_entries_count].array = calloc(map_definition.max_entries, map_definition.value_size);

    return (uint64_t)&_map_entries[_map_entries_count++];
}

bool
map_relocation_bounds_check_function(void* user_context, uint64_t addr, uint64_t size)
{
    (void)user_context;
    for (int index = 0; index < _map_entries_count; index++) {
        if (addr >= (uint64_t)_map_entries[index].array &&
            addr + size <= (uint64_t)_map_entries[index].array + _map_entries[index].map_definition.max_entries *
                                                                 _map_entries[index].map_definition.value_size) {
            return true;
        }
    }
    return false;
}

//int alloc_fcc(struct ubpf_ctx *alloc_context,
//              flatcc_iovec_t *b, size_t request, int zero_fill, int alloc_type){
//
//    void *bufVM = mem_alloc(&alloc_context->mem.mgr_heap, request*sizeof(char));
//}


#define ADDED_CTX_CALL 12
#define MAX_CALL 2048

static bool rewrite_with_ctx(struct ubpf_vm *vm, const struct ebpf_inst *insts, uint32_t num_inst, char **errmsg,
                             uint64_t ctx_id) {

    int pc = 0;
    uint32_t i;
//
    struct ebpf_inst inst;
    uint32_t num_call = 0;
    uint32_t rewrite_pcs[MAX_CALL];

    uint16_t new_offset;
    uint32_t new_num_insts;

    /* 1st pass : locate EBPF_OP_CALL */
    for(i = 0; i < num_inst; i++) {
        inst = insts[i];

        switch (inst.opcode) {
            case EBPF_OP_CALL:
                rewrite_pcs[num_call] = i;
                num_call++;

                if(num_call >= MAX_CALL) {
                    *errmsg = ubpf_error("Too many calls (EBPF_OP_CALL)");
                    return false;
                }
                break;
            default:
                break;
        }


    }

    new_num_insts = vm->num_insts + (num_call * ADDED_CTX_CALL);

    vm->insts = malloc(new_num_insts * 8);
    if(!vm->num_insts) {
        *errmsg = ubpf_error("Cannot allocate space for rewritten eBPF instructions");
        return false;
    }
    vm->num_insts = new_num_insts;

    /* 2nd pass : rewrite ebpf assembly accordingly */
    for(i = 0; i < num_inst; i++) {
//
        inst = insts[i];
//
        switch (inst.opcode) {
            case EBPF_OP_CALL:
                /* 12 extra eBPF instructions to pass ctx for helper functions */

                /* copy R5 to R13 (tmp register) */
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 12, .src = 5, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 5, .src = 4, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 4, .src = 3, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 3, .src = 2, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 2, .src = 1, .offset = 0, .imm = 0};
                /* copy ctx_id to R1 */
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_LDDW, .dst = 1, .src = 0, .offset = 0, .imm = ctx_id & UINT32_MAX};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = 0, .dst = 0, .src = 0, .offset = 0, .imm = ctx_id >> 32u};

                /* call instruction */
                vm->insts[pc++] = inst;

                /* call has been executed, revert back registers */
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 1, .src = 2, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 2, .src = 3, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 3, .src = 4, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 4, .src = 5, .offset = 0, .imm = 0};
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = EBPF_OP_MOV64_REG, .dst = 5, .src = 12, .offset = 0, .imm = 0};
                break;

            case EBPF_OP_JA:
            case EBPF_OP_JEQ_REG:
            case EBPF_OP_JEQ_IMM:
            case EBPF_OP_JGT_REG:
            case EBPF_OP_JGT_IMM:
            case EBPF_OP_JGE_REG:
            case EBPF_OP_JGE_IMM:
            case EBPF_OP_JLT_REG:
            case EBPF_OP_JLT_IMM:
            case EBPF_OP_JLE_REG:
            case EBPF_OP_JLE_IMM:
            case EBPF_OP_JSET_REG:
            case EBPF_OP_JSET_IMM:
            case EBPF_OP_JNE_REG:
            case EBPF_OP_JNE_IMM:
            case EBPF_OP_JSGT_IMM:
            case EBPF_OP_JSGT_REG:
            case EBPF_OP_JSGE_IMM:
            case EBPF_OP_JSGE_REG:
            case EBPF_OP_JSLT_IMM:
            case EBPF_OP_JSLT_REG:
            case EBPF_OP_JSLE_IMM:
            case EBPF_OP_JSLE_REG:
                /* rewriting jumps according to the number of new instructions added */

                new_offset = inst.offset;
                if (inst.offset > 0) {
                    for (uint32_t j = 0; j < num_call && rewrite_pcs[j] < i + 1 + inst.offset; j++) {
                        /* We should jump all loads/stores in range [ next_pc ; next_pc + offset [ */
                        if (rewrite_pcs[j] >= i + 1 && rewrite_pcs[j] < i + 1 + inst.offset) {
                            new_offset += ADDED_CTX_CALL;
                        }
                    }
                }
                else if (inst.offset < 0) {
                    for (uint32_t j = 0; j < num_call && rewrite_pcs[j] < i + 1; j++) {
                        /* We should jump all loads/stores in range [ next_pc + offset ; next_pc [ */
                        /* Notice that here, offset is negative */
                        if (rewrite_pcs[j] >= i + 1 + inst.offset && rewrite_pcs[j] < i + 1) {
                            new_offset -= ADDED_CTX_CALL;
                        }
                    }
                }
                /* And put the jump with the new offset */
                vm->insts[pc++] = (struct ebpf_inst) {.opcode = inst.opcode, .dst = inst.dst, .src = inst.src, .offset = new_offset, .imm = inst.imm};
                break;
            default:
                vm->insts[pc++] = inst;
        }

    }
    return true;
}

struct ubpf_ctx* new_ctx(){
    struct ubpf_ctx *ctx = calloc(1, sizeof(*ctx));
    return ctx;
}

int destroy_ctx(struct ubpf_ctx *ctx){
    free(ctx);
    return 1;
}


int prepare_mem(struct ubpf_ctx *ctx, struct ubpf_vm *vm, void *data_buffer, size_t buffer_len) {
     ctx->vm = vm;
    size_t heap_size = 100217728;
    size_t sheap_size = 0;
    size_t total_allowed_mem;
    uint8_t *super_block;
    heap_size = MEM_ALIGN(heap_size);
    sheap_size = MEM_ALIGN(sheap_size);

    total_allowed_mem = sheap_size + heap_size + MAX_SIZE_ARGS;

    if (total_allowed_mem > MAX_HEAP) {
        return 0;
    }
    ctx->mem.len = total_allowed_mem;
    super_block = malloc(total_allowed_mem);
    if (!super_block) {
        perror("Can't alloc mem for execution");
        free(ctx);
        return 0;
    }
    ctx->mem.master_block = super_block;

    if (heap_size > 0) {
        ctx->mem.has_heap = 1;

        if (init_memory_manager(&ctx->mem.mgr_heap, MICHELFRA_MEM) != 0) {
            free(ctx);
            return 0;
        }

        mem_init(&ctx->mem.mgr_heap, super_block + MAX_SIZE_ARGS, heap_size);

    }
    if (sheap_size > 0) {
        ctx->mem.has_shared_heap = 1;

        init_shared_hash(&ctx->mem.shared_blocks);

        if (init_memory_manager(&ctx->mem.mgr_shared_heap, MICHELFRA_MEM) != 0) {
            free(ctx);
            return 0;
        }

        mem_init(&ctx->mem.mgr_shared_heap, super_block + MAX_SIZE_ARGS + heap_size, sheap_size);
    }
    vm->extra_mem_start = (void *) ctx->mem.master_block;
    vm->extra_mem_size = (uint32_t) ctx->mem.len;
    ctx->bufVM = mem_alloc(&ctx->mem.mgr_heap, buffer_len*sizeof(char));

    memcpy(ctx->bufVM, data_buffer, buffer_len*sizeof(char));

    entry_arg_t *args = malloc(sizeof(entry_arg_t) * 3);
    args[0] = (entry_arg_t){.arg = &data_buffer, .len = sizeof(buffer_len), .kind=kind_primitive, .type=1};
    args[1] = (entry_arg_t){.arg = &ctx->bufVM, .len = sizeof(ctx->bufVM), .kind=kind_ptr, .type=2};
    args[2] = (entry_arg_t) entry_arg_null;

    args_t *arg = malloc(sizeof(args_t));
    arg->args=args;
    arg->nargs=2;

    ctx->args = arg;

    return 1;
}

struct ubpf_vm* create_vm(void* code, size_t code_len, struct ubpf_ctx *ctx, bool jit)
{
    uint64_t secret = (uint64_t)rand() << 32 | (uint64_t)rand();

    struct ubpf_vm* vm = ubpf_create();
    if (!vm) {
        fprintf(stderr, "Failed to create VM\n");
        return NULL;
    }
    // TODO; Are these necessary? deprecate.
    ubpf_register_data_relocation(vm, NULL, do_map_relocation);
    ubpf_register_data_bounds_check(vm, NULL, map_relocation_bounds_check_function);

    if (ubpf_set_pointer_secret(vm, secret) != 0) {
        fprintf(stderr, "Failed to set pointer secret\n");
        return NULL;
    }

    register_functions(vm);
//
    /*
     * The ELF magic corresponds to an RSH instruction with an offset,
     * which is invalid.
     */
#if defined(UBPF_HAS_ELF_H)
    bool elf = code_len >= SELFMAG && !memcmp(code, ELFMAG, SELFMAG);
#endif

    char* errmsg;
    int rv;
    load:
#if defined(UBPF_HAS_ELF_H)
    if (elf) {
        rv = ubpf_load_elf(vm, code, code_len, &errmsg);
    } else {
#endif
        rv = ubpf_load(vm, code, code_len, &errmsg);
#if defined(UBPF_HAS_ELF_H)
    }
#endif

    if (rv < 0) {
        fprintf(stderr, "Failed to load code: %s\n", errmsg);
        free(errmsg);
        ubpf_destroy(vm);
        return NULL;
    }

    if (jit) {
        struct ebpf_inst * code_ptr = vm->insts;
        if (!rewrite_with_ctx(vm, code_ptr, vm->num_insts, &errmsg,  ((uintptr_t) ctx))) {
            return NULL;
        }
        ubpf_compile(vm, &errmsg);
    }
    return vm;
}

static void*
readfile(const char* path, size_t maxlen, size_t* len)
{
    FILE* file;
    if (!strcmp(path, "-")) {
        file = fdopen(STDIN_FILENO, "r");
    } else {
        file = fopen(path, "r");
    }

    if (file == NULL) {
        fprintf(stderr, "Failed to open %s: %s\n", path, strerror(errno));
        return NULL;
    }

    char* data = calloc(maxlen, 1);
    size_t offset = 0;
    size_t rv;
    while ((rv = fread(data + offset, 1, maxlen - offset, file)) > 0) {
        offset += rv;
    }

    if (ferror(file)) {
        fprintf(stderr, "Failed to read %s: %s\n", path, strerror(errno));
        fclose(file);
        free(data);
        return NULL;
    }

    if (!feof(file)) {
        fprintf(stderr, "Failed to read %s because it is too large (max %u bytes)\n", path, (unsigned)maxlen);
        fclose(file);
        free(data);
        return NULL;
    }

    fclose(file);
    if (len) {
        *len = offset;
    }
    return (void*)data;
}


int submit_beacons(ubpf_ctx_t* ctx, unsigned int beacon_len, struct beacon_result *result) {
    _beacon_len = beacon_len;
    _beacon_result = result;

//    printf(" [env] beacon result submitted; %d beacons; %d\n", beacon_len, result[1].beacon_id);
    return 50;
}

static void
register_functions(struct ubpf_vm* vm)
{
    ubpf_register(vm, 0, "gather_bytes", gather_bytes);
    ubpf_register(vm, 1, "memfrob", memfrob);
    ubpf_register(vm, 2, "trash_registers", trash_registers);
    ubpf_register(vm, 3, "sqrti", sqrti);
    ubpf_register(vm, 4, "strcmp_ext", strcmp);
    ubpf_register(vm, 5, "unwind", unwind);
    ubpf_set_unwind_function_index(vm, 5);
    ubpf_register(vm, (unsigned int)(uintptr_t)bpf_map_lookup_elem, "bpf_map_lookup_elem", bpf_map_lookup_elem_impl);
    ubpf_register(vm, (unsigned int)(uintptr_t)bpf_map_update_elem, "bpf_map_update_elem", bpf_map_update_elem_impl);
    ubpf_register(vm, (unsigned int)(uintptr_t)bpf_map_delete_elem, "bpf_map_delete_elem", bpf_map_delete_elem_impl);
    ubpf_register(vm, 30, "test", test);
    ubpf_register(vm, 31, "ctx_malloc", ctx_malloc);
    ubpf_register(vm, 32, "ctx_calloc", ctx_calloc);
    ubpf_register(vm, 33, "ctx_realloc", ctx_realloc);
    ubpf_register(vm, 34, "ctx_free", ctx_free);
    ubpf_register(vm, 35, "ctx_shmnew", ctx_shmnew);
    ubpf_register(vm, 36, "ctx_shmget", ctx_shmget);
    ubpf_register(vm, 37, "ctx_shmrm", ctx_shmrm);
    ubpf_register(vm, 38, "get_time", get_time);
    ubpf_register(vm, 39, "get_realtime", get_realtime);
    ubpf_register(vm, 40, "get_arg", get_arg);
    ubpf_register(vm, 41, "print", print);
    ubpf_register(vm, 43, "__assert_fail", assert_fail);
    ubpf_register(vm, 45, "submit_beacons", submit_beacons);
}

struct execution_result exec_vm(struct ubpf_vm *vm, struct ubpf_ctx *ctx, bool jit){
    struct execution_result res;
    char* errmsg;
    uint64_t ret;
//    clock_t start_time = clock();

    if (jit) {
        ubpf_jit_fn fn = ubpf_compile(vm, &errmsg);
        if (fn == NULL) {
            fprintf(stderr, "Failed to compile: %s\n", errmsg);
            free(errmsg);
            return res;
        }
        ret = fn(NULL, 0);
    } else {
        if (ubpf_exec(vm, ctx, NULL, 0, &ret) < 0)
            ret = UINT64_MAX;
    }

//    double elapsed_time = (double)(clock() - start_time) / CLOCKS_PER_SEC;
//    printf("Done in %f seconds\n", elapsed_time);
    res.beacon=_beacon_result;
    res.result_len=_beacon_len;
    return res;
//    return ret;
}
int destroy_mem(ubpf_ctx_t *ctx){
    destroy_shared_map(&ctx->mem.mgr_shared_heap, &ctx->mem.shared_blocks);
    free(ctx->mem.master_block);
    free(ctx->args->args);
    free(ctx->args);
    return 1;
}
void destroy_vm(struct ubpf_vm* vm)
{
    ubpf_destroy(vm);
}
