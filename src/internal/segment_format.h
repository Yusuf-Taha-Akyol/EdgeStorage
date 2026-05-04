#ifndef ES_INTERNAL_SEGMENT_FORMAT_H
#define ES_INTERNAL_SEGMENT_FORMAT_H

#include "edgestorage/edgestorage.h"

#include <stdint.h>
#include <stdio.h>

#define ES_SEGMENT_MAGIC "ESG1"
#define ES_SEGMENT_FORMAT_VERSION 1
#define ES_RECORD_FORMAT_VERSION 1

typedef enum {
    ES_SEGMENT_COMPRESSION_NONE = 0,
    ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA = 1
} es_segment_compression_mode_t;

typedef struct {
    char magic[4];
    uint16_t version;
    uint16_t header_size;
    uint32_t stream_id;
    uint32_t segment_index;
    uint32_t compression_mode;
    uint32_t record_format_version;
    uint32_t flags;
    uint32_t reserved;
} es_segment_header_t;

es_status_t es_segment_format_write_header(
    FILE* file,
    const es_segment_header_t* header
);

es_status_t es_segment_format_read_header(
    FILE* file,
    es_segment_header_t* out_header
);

es_status_t es_segment_format_validate_header(
    const es_segment_header_t* header
);

es_segment_compression_mode_t es_segment_format_compression_from_config(
    int compression_enabled
);

#endif