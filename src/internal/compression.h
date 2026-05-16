#ifndef ES_INTERNAL_COMPRESSION_H
#define ES_INTERNAL_COMPRESSION_H

#include "edgestorage/edgestorage.h"

#include <stdint.h>
#include <stdio.h>

typedef struct {
    uint64_t last_timestamp_ns;
    int has_last_timestamp;
} es_compression_context_t;

typedef enum {
    ES_TIMESTAMP_ENCODING_FULL_U64 = 1,
    ES_TIMESTAMP_ENCODING_DELTA_U32 = 2
} es_timestamp_encoding_t;

typedef enum {
    ES_PAYLOAD_ENCODING_RAW = 0
} es_payload_encoding_t;

typedef struct {
    uint8_t timestamp_encoding;
    uint8_t payload_encoding;
    uint16_t record_type_id;
    uint16_t flags;
    uint16_t reserved;
    uint32_t uncompressed_payload_size;
    uint32_t encoded_payload_size;
} es_encoded_record_header_t;

void es_compression_context_init(es_compression_context_t* ctx);

void es_compression_context_reset(es_compression_context_t* ctx);

size_t es_compression_encoded_record_size(
    const es_compression_context_t* ctx,
    uint32_t compression_mode,
    const es_record_t* record
);

es_status_t es_compression_write_record(
    FILE* file,
    es_compression_context_t* ctx,
    uint32_t compression_mode,
    const es_record_t* record
);

es_status_t es_compression_read_record(
    FILE* file,
    es_compression_context_t* ctx,
    uint32_t compression_mode,
    es_record_t* out_record
);

#endif