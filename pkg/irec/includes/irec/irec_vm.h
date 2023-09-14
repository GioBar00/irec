//
// Created by jelte on 9-6-23.
//
#include "shared.h"
#include <inttypes.h>
#include <stdlib.h>
extern int print(const char *fmt, ...);
extern void *get_arg(unsigned int arg_type);
extern void *ctx_malloc(size_t size);
extern void *ctx_calloc(uint64_t nmemb, uint64_t size);
extern void *ctx_realloc(void *ptr, uint64_t size);
extern void ctx_free(void *ptr);

extern void submit_beacons(unsigned int beacon_len, struct beacon_result *result);