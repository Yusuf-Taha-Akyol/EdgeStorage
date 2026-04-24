#ifndef ES_INTERNAL_STORAGE_WRITER_H
#define ES_INTERNAL_STORAGE_WRITER_H

#include "edgestorage/edgestorage.h"

#include "stdio.h"
#include "stddef.h"
#include "stdint.h"

#define ES_STORAGE_PATH_MAX 512

typedef struct {
    uint32_t stream_id;
    uint32_t active_segment_index;
    size_t active_segment_size_bytes;

    uint64_t last_timestamp_ns;
    int has_last_timestamp;
    
    FILE* active_segment_file;
    char stream_dir_path[ES_STORAGE_PATH_MAX];
    char active_segment_path[ES_STORAGE_PATH_MAX];
} es_stream_storage_state_t;

es_status_t es_storage_writer_init(es_engine_t* engine);
void es_storage_writer_shutdown(es_engine_t* engine);

es_status_t es_storage_writer_register_stream(
    es_engine_t* engine,
    uint32_t stream_id
);

es_stream_storage_state_t* es_storage_writer_find_stream_state(
    es_engine_t* engine,
    uint32_t stream_id
);

es_status_t es_storage_writer_append_record(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* record
);

es_status_t es_storage_writer_append_batch(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* records,
    size_t count
);

#endif