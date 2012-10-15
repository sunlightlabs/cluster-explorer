#include <stdlib.h>
#include <stdio.h>

#include "cpartition.h"

cpartition_ptr create_cpartition(int* values, int length) {
    int byte_length, i;
    inthash_ptr item, tmp;

    cpartition_ptr part = (cpartition_ptr)(malloc(sizeof(cpartition_t)));
    part->length = length;
    part->values = values;

    byte_length = sizeof(int) * length;

    part->rank = (int*)(malloc(byte_length));
    memset(part->rank, 0, byte_length);

    part->parent = (int*)(malloc(byte_length));
    part->value_positions = (inthash_ptr*)(malloc(sizeof(inthash_ptr)));
    *(part->value_positions) = NULL;

    for (i = 0; i < length; i++) {
        part->parent[i] = i;

        item = (inthash_ptr)(malloc(sizeof(inthash_t)));
        item->id = values[i];
        item->value = i;
        HASH_ADD_INT(*(part->value_positions), id, item); 
    }

    return part;
}

void free_cpartition(cpartition_ptr part) {
    /* free the value_positions has first */
    inthash_t *current, *tmp;

    HASH_ITER(hh, *(part->value_positions), current, tmp) {
        HASH_DEL(*(part->value_positions), current);
        free(current);
    }
    free(*(part->value_positions));

    /* free the rest of it */
    free(part->rank);
    free(part->parent);

    free(part);
}

int cpartition_find(cpartition_ptr part, int x) {
    if (part->parent[x] != x) {
        part->parent[x] = cpartition_find(part, part->parent[x]);
    }
    return part->parent[x];
}

int cpartition_find_by_value(cpartition_ptr part, int x) {
    inthash_ptr s, tmp;

    HASH_FIND_INT(*(part->value_positions), &x, s );
    return cpartition_find(part, s->value);
}

void cpartition_merge(cpartition_ptr part, int x, int y) {
    int x_root, y_root;
    x_root = cpartition_find_by_value(part, x);
    y_root = cpartition_find_by_value(part, y);

    if (x_root == y_root) {
        return;
    }

    if (part->rank[x_root] < part->rank[y_root]) {
        part->parent[x_root] = y_root;
    } else if (part->rank[x_root] > part->rank[y_root]) {
        part->parent[y_root] = x_root;
    } else {
        part->parent[y_root] = x_root;
        part->rank[x_root] += 1;
    }
}