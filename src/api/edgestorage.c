#include "edgestorage/edgestorage.h"

#include <stdlib.h>

typedef enum {
    ES_ENGINE_STATE_OPEN = 1,
    ES_ENGINE_STATE_CLOSED = 2
} es_engine_state_t;

struct es_engine {
    es_config_t config;
    es_engine_state_t state;
    uint32_t next_stream_id;
    size_t registered_stream_count;
};

static int es_is_valid_schema(const es_stream_schema_t* schema) {
    if(!schema || !schema->stream_name || !schema->record_types) {
        return 0;
    }

    if(schema->record_type_count == 0) {
        return 0;
    }

    for(uint16_t i = 0; i < schema->record_type_count; ++i) {
        const es_record_type_def_t* record_type = &schema->record_types[i];

        if(record_type->record_type_id == 0) {
            return 0;
        }

        if(!record_type->record_name || !record_type->fields) {
            return 0;
        }

        if(record_type->field_count == 0) {
            return 0;
        }
    }

    return 1;
}

es_engine_t* es_open(const es_config_t* config) {
    if(!config || !config->storage_path) {
        return NULL;
    }

    if(config->segment_size_bytes == 0 || config->write_buffer_size_bytes == 0) {
        return NULL;
    }

    es_engine_t* engine = (es_engine_t*)malloc(sizeof(es_engine_t));
    if(!engine) {
        return NULL;
    }

    engine->config = *config;
    engine->state = ES_ENGINE_STATE_OPEN;
    engine->next_stream_id = 1;
    engine->registered_stream_count = 0;
    return engine;
}

void es_close(es_engine_t* engine) {
    if(!engine) {
        return;
    }

    engine->state = ES_ENGINE_STATE_CLOSED;
    free(engine);
}

es_status_t es_register_stream_schema(
    es_engine_t* engine,
    const es_stream_schema_t* schema,
    uint32_t* out_stream_id
) {
    if(!engine || !out_stream_id) {
        return ES_ERR_INVALID_ARG;
    }

    if(engine->state != ES_ENGINE_STATE_OPEN) {
        return ES_ERR_INTERNAL;
    }

    if(!es_is_valid_schema(schema)) {
        return ES_ERR_INVALID_ARG;
    }

    *out_stream_id = engine->next_stream_id;
    engine->next_stream_id++;
    engine->registered_stream_count++;

    return ES_OK;
}

es_status_t es_write_record(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* record
) {
    if(!engine || !record || stream_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(engine->state != ES_ENGINE_STATE_OPEN) {
        return ES_ERR_INTERNAL;
    }

    return ES_OK;
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

    if(engine->state != ES_ENGINE_STATE_OPEN) {
        return ES_ERR_INTERNAL;
    }

    return ES_OK;
}

es_status_t es_query_range(
    es_engine_t* engine,
    const es_query_t* query,
    es_result_t* out_result
) {
    if(!engine || !query || !out_result) {
        return ES_ERR_INVALID_ARG;
    }

    if(engine->state != ES_ENGINE_STATE_OPEN) {
        return ES_ERR_INTERNAL;
    }

    if(query->stream_id == 0) {
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