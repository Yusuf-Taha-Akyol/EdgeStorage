#include "stream_registry.h"
#include "runtime.h"

#include <stdlib.h>

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

int es_stream_registry_is_valid_schema(const es_stream_schema_t* schema) {
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

es_status_t es_stream_registry_register(
    es_engine_t* engine,
    const es_stream_schema_t* schema,
    uint32_t* out_stream_id
) {
    if(!engine || !out_stream_id) {
        return ES_ERR_INVALID_ARG;
    }

    if(!es_stream_registry_is_valid_schema(schema)) {
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

int es_stream_registry_contains(const es_engine_t* engine, uint32_t stream_id) {
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