#include "uthash.h"

#ifndef CPARTITION_H
#define CPARTITION_H

typedef struct inthash {
    int id;
    int value;
    UT_hash_handle hh;
} inthash_t, *inthash_ptr;

typedef struct cpartition {
    int length;
    int* values;
    inthash_ptr* value_positions;
    int* parent;
    int* rank;
} cpartition_t, *cpartition_ptr;

cpartition_ptr create_cpartition(int* values, int length);
void free_cpartition(cpartition_ptr part);

static inline int cpartition_find(cpartition_ptr part, int x);
static inline int cpartition_find_by_value(cpartition_ptr part, int x);
void cpartition_merge(cpartition_ptr part, int x, int y);

#endif