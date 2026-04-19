#include "edgestorage/edgestorage.h"

#include <stdlib.h>

struct es_engine {
    es_config_t config;
};

es_engine_t* es_open(const es_config_t* config) {
    if(!config || !config->storage_path) {
        return NULL;
    }

    es_engine_t* engine = (es_engine_t*)malloc(sizeof(es_engine_t));
    if(!engine) {
        return NULL;
    }

    engine->config = *config;
    return engine;
}

void es_close(es_engine_t* engine) {
    free(engine);
}

es_status_t es_register_stream_schema(
    es_engine_t* engine,
    const es_stream_schema_t* schema,
    uint32_t* out_stream_id
) {
    (void)engine;
    (void)schema;

    if(!out_stream_id) {
        return ES_ERR_INVALID_ARG;
    }

    *out_stream_id = 1;
    return ES_OK;
}

es_status_t es_write_record(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* record
) {
    (void)engine;
    (void)stream_id;
    (void)record;

    return ES_OK;
}

es_status_t es_write_batch (
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* records,
    size_t count
) {
    (void) engine;
    (void) stream_id;
    (void) records;
    (void) count;

    return ES_OK;
}

es_status_t es_query_range (
    es_engine_t* engine,
    const es_query_t* query,
    es_result_t* out_result
) {
    (void) engine;
    (void) query;

    if(!out_result) {
        return ES_ERR_INVALID_ARG;
    }

    out_result->records = NULL;
    out_result->count = 0;
    return ES_OK;
}

void es_result_free(es_result_t* result) {
    if(!result) {
        return;
    }

    free(result->records);
    result->records = NULL;
    result->count = 0;
}