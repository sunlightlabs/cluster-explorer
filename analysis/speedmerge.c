#include <stdlib.h>
#include <stdio.h>

#include "cpartition.h"
#include "lz4.h"

/* from python-lz4 */
static inline unsigned int load_le32(const char *c) {
    const uint8_t *d = (const uint8_t *)c;
    return d[0] | (d[1] << 8) | (d[2] << 16) | (d[3] << 24);
}
static const int hdr_size = sizeof(uint32_t);

static inline char* read_file(char* file_name) {
    FILE* infile;
    char* buffer;
    long numbytes;
    
    infile = fopen(file_name, "r");
    
    if (infile == NULL) {
        return NULL;
    }
    
    /* Get the number of bytes */
    fseek(infile, 0L, SEEK_END);
    numbytes = ftell(infile);
    
    /* reset the file position indicator to the beginning of the file */
    fseek(infile, 0L, SEEK_SET);
    
    buffer = (char*)(malloc(numbytes * sizeof(char)));
    
    /* memory error */
    if (buffer == NULL) {
        return NULL;
    }
    
    /* copy all the text into the buffer */
    fread(buffer, sizeof(char), numbytes, infile);
    fclose(infile);
    
    return buffer;
}

static inline int decompress_lz4_file(char* file_name, char** dest_ptr) {
    char* compressed;
    char* dest;
    unsigned int dest_size;
    int out;

    compressed = read_file(file_name);
    dest_size = load_le32(compressed);
    dest = (char*)(malloc(dest_size * sizeof(char)));

    *dest_ptr = dest;

    LZ4_uncompress(compressed + hdr_size, dest, dest_size);
    free(compressed);

    return dest_size;
}

void merge_lz4_file(cpartition_ptr part, char* file_name) {
    char* buffer;
    int size, i;

    size = decompress_lz4_file(file_name, &buffer);

    for (i = 0; i < size; i += 2 * sizeof(char)) {
        cpartition_merge(part, *(int*)(buffer + i), *(int*)(buffer + i + sizeof(int)));
    }
}