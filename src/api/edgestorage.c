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
    uint32_t* registered_stream_ids;
    size_t registered_stream_count;
    size_t registered_stream_capacity;
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

static int es_is_registered_stream_id(const es_engine_t* engine, uint32_t stream_id) {
    if(!engine || stream_id == 0) {
        return 0;
    }

    for(size_t i = 0; i < engine->registered_stream_count; ++i) {
        if(engine->registered_stream_ids[i] == stream_id) {
            return 1;
        }
    }

    return 0;
}

static es_status_t es_ensure_stream_capacity(es_engine_t* engine) {
    if(!engine) {
        return ES_ERR_INVALID_ARG;
    }

    if(engine->registered_stream_count < engine->registered_stream_capacity) {
        return ES_OK;
    }

    size_t new_capacity = engine->registered_stream_capacity == 0 ? 4 : engine->registered_stream_capacity * 2;
    uint32_t* new_ids = (uint32_t*)realloc(
        engine->registered_stream_ids,
        new_capacity * sizeof(uint32_t)
    );

    if(!new_ids) {
        return ES_ERR_OOM;
    } 

    engine->registered_stream_ids = new_ids;
    engine->registered_stream_capacity = new_capacity;

    return ES_OK;
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
    engine->registered_stream_ids = NULL;
    engine->registered_stream_count = 0;
    engine->registered_stream_capacity = 0;
    return engine;
}

void es_close(es_engine_t* engine) {
    if(!engine) {
        return;
    }

    engine->state = ES_ENGINE_STATE_CLOSED;
    free(engine->registered_stream_ids);
    engine->registered_stream_ids = NULL;
    engine->registered_stream_count = 0;
    engine->registered_stream_capacity = 0;
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

    es_status_t status = es_ensure_stream_capacity(engine);
    if(status != ES_OK) {
        return status;
    }

    *out_stream_id = engine->next_stream_id;
    engine->registered_stream_ids[engine->registered_stream_count] = *out_stream_id;
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

    if(!es_is_registered_stream_id(engine, stream_id)) {
        return ES_ERR_NOT_FOUND;
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

    if(!es_is_registered_stream_id(engine, stream_id)) {
        return ES_ERR_NOT_FOUND;
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

    if(!es_is_registered_stream_id(engine, query->stream_id)) {
        return ES_ERR_NOT_FOUND;
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