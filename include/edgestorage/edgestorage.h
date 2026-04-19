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
    ES_TYPE_U8 = 1,
    ES_TYPE_U16 = 2,
    ES_TYPE_U32 = 3,
    ES_TYPE_U64 = 4,
    ES_TYPE_I32 = 5,
    ES_TYPE_I64 = 6,
    ES_TYPE_F32 = 7,
    ES_TYPE_F64 = 8,
    ES_TYPE_BOOL = 9,
} es_value_type_t;

typedef enum {
    ES_COMP_NONE = 0,
    ES_COMP_DELTA = 1,
    ES_COMP_DELTA_DELTA = 2,
    ES_COMP_XOR = 3,
    ES_COMP_FOR = 4,
} es_comp_hint_t;

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
    es_comp_hint_t comp_hint;
} es_field_def_t;

typedef struct {
    uint16_t record_type_id;
    const char* record_name;
    uint16_t field_count;
    const es_field_def_t* fields;
    uint32_t payload_size;
} es_record_type_def_t;

typedef struct {
    const char* stream_name;
    uint32_t schema_version;
    uint16_t record_type_count;
    const es_record_type_def_t* record_types;
} es_stream_schema_t;

typedef struct {
    uint64_t timestamp_ns;
    uint16_t record_type_id;
    uint16_t flags;
    uint32_t payload_size;
    const void* payload;
} es_record_t;

/*
 * record_type_id == 0 -> all record types
 * record_type_id != 0 -> filter by that record type
 */
typedef struct {
    uint32_t stream_id;
    uint64_t start_ts_ns;
    uint64_t end_ts_ns;
    uint16_t record_type_id;
    size_t limit;
} es_query_t;

typedef struct {
    es_record_t* records;
    size_t count;
} es_result_t;

es_engine_t* es_open(const es_config_t* config);
void es_close(es_engine_t* engine);

es_status_t es_register_stream_schema(
    es_engine_t* engine,
    const es_stream_schema_t* schema,
    uint32_t* out_stream_id
);

es_status_t es_write_record(
    es_engine_t* engine, 
    uint32_t stream_id,
    const es_record_t* record
);

es_status_t es_write_batch(
    es_engine_t* engine,
    uint32_t stream_id, 
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