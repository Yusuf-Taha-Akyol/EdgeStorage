#include "edgestorage/edgestorage.h"
#include "runtime.h"
#include "stream_registry.h"
#include "storage_writer.h"
#include "storage_reader.h"

#include <stdlib.h>

es_engine_t* es_open(const es_config_t* config) {
    return es_runtime_create(config);
}

void es_close(es_engine_t* engine) {
    es_runtime_destroy(engine);
}

es_status_t es_register_stream_schema(
    es_engine_t* engine,
    const es_stream_schema_t* schema,
    uint32_t* out_stream_id
) {
    if(!engine || !out_stream_id) {
        return ES_ERR_INVALID_ARG;
    }

    if(!es_runtime_is_open(engine)) {
        return ES_ERR_INTERNAL;
    }

    return es_stream_registry_register(engine, schema, out_stream_id);
}

es_status_t es_write_record(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* record
) {
    if(!engine || !record || stream_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(!es_runtime_is_open(engine)) {
        return ES_ERR_INTERNAL;
    }

    if(!es_stream_registry_contains(engine, stream_id)) {
        return ES_ERR_NOT_FOUND;
    }

    return es_storage_writer_append_record(engine, stream_id, record);
}

es_status_t es_write_batch(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* records,
    size_t count
) {
    if(!engine || !records || stream_id == 0 || count == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(!es_runtime_is_open(engine)) {
        return ES_ERR_INTERNAL;
    }

    if(!es_stream_registry_contains(engine, stream_id)) {
        return ES_ERR_NOT_FOUND;
    }

    return es_storage_writer_append_batch(engine, stream_id, records, count);
}

es_status_t es_query_range(
    es_engine_t* engine,
    const es_query_t* query,
    es_result_t* out_result
) {
    if(!engine || !query || !out_result) {
        return ES_ERR_INVALID_ARG;
    }

    if(!es_runtime_is_open(engine)) {
        return ES_ERR_INTERNAL;
    }

    if(query->stream_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(!es_stream_registry_contains(engine, query->stream_id)) {
        return ES_ERR_NOT_FOUND;
    }

    return es_storage_reader_query_range(engine, query, out_result);
}

void es_result_free(es_result_t* result) {
    if(!result) {
        return;
    }

    free(result->records);
    result->records = NULL;
    result->count = 0;
}