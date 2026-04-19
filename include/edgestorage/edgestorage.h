#ifndef EDGESTORAGE_EDGESTORAGE_H
#define EDGESTORAGE_EDGESTORAGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct es_engine es_engine_t;

typedef enum {
    ES_OK = 0,
    ES_ERR_INVALID_ARG = 1,
    ES_ERR_IO = 2,
    ES_ERR_OOM = 3,
    ES_ERR_NOT_FOUND = 4,
    ES_ERR_INTERNAL = 5,
} es_status_t;

typedef enum {
    ES_TYPE_I64 = 0,
    ES_TYPE_U64 = 1,
    ES_TYPE_F32 = 2,
    ES_TYPE_F64 = 3,
    ES_TYPE_BOOL = 4
} es_value_type_t;

typedef struct {
    const char* storage_path;
    size_t segment_size_bytes;
    size_t write_buffer_size_bytes;
    int compression_enabled;
} es_config_t;

typedef struct {
    uint16_t field_id;
    const char* name;
    es_value_type_t type;
} es_field_def_t;

typedef struct {
    const char* stream_name;
    uint16_t field_count;
    es_field_def_t* fields;
} es_stream_def_t;

typedef struct {
    uint16_t field_id;
    es_value_type_t type;
    union {
        int64_t i64;
        uint64_t u64;
        float f32;
        double f64;
        uint8_t boolean;
    } value;
} es_field_t;

typedef struct {
    uint64_t timestamp;
    uint32_t stream_id;
    uint16_t field_count;
    es_field_t* fields;
} es_record_t;

typedef struct {
    uint64_t stream_id;
    uint64_t start_ts;
    uint64_t end_ts;
    size_t limit;
} es_query_t;

typedef struct {
    es_record_t* records;
    size_t count;
} es_result_t;

es_engine_t* es_open(const es_config_t* config);
void es_close(es_engine_t* engine);

es_status_t es_register_stream(
    es_engine_t* engine,
    const es_stream_def_t* def,
    uint32_t* out_stream_id
);

es_status_t es_write_record(
    es_engine_t* engine, 
    const es_record_t* record
);

es_status_t es_write_batch(
    es_engine_t* engine,
    const es_record_t* records,
    size_t count
);

es_status_t es_query_range(
    es_engine_t* engine,
    const es_query_t* query,
    es_result_t* out_result
);

void es_result_free(es_result_t* result);

#ifdef __cplusplus
}
#endif

#endif