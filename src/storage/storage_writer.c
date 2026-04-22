#include "storage_writer.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>

static es_status_t es_storage_writer_ensure_capacity(es_engine_t* engine) {
    if(!engine) {
        return ES_ERR_INVALID_ARG;
    }

    if(engine->stream_storage_count < engine->stream_storage_capacity) {
        return ES_OK;
    }

    size_t new_capacity = engine->stream_storage_capacity == 0
        ? 4
        : engine->stream_storage_capacity * 2;

    es_stream_storage_state_t* new_states = (es_stream_storage_state_t*)realloc(
        engine->stream_storage_states,
        new_capacity * sizeof(es_stream_storage_state_t)
    );

    if(!new_states) {
        return ES_ERR_OOM;
    }

    engine->stream_storage_states = new_states;
    engine->stream_storage_capacity = new_capacity;

    return ES_OK;
}

es_status_t es_storage_writer_init(es_engine_t* engine) {
    if(!engine) {
        return ES_ERR_INVALID_ARG;
    }

    engine->stream_storage_states = NULL;
    engine->stream_storage_count = 0;
    engine->stream_storage_capacity = 0;

    return ES_OK;
}

void es_storage_writer_shutdown(es_engine_t* engine) {
    if(!engine) {
        return;
    }

    for(size_t i = 0; i < engine->stream_storage_count; ++i) {
        if(engine->stream_storage_states[i].active_segment_file) {
            fclose(engine->stream_storage_states[i].active_segment_file);
            engine->stream_storage_states[i].active_segment_file = NULL;
        }
    }

    free(engine->stream_storage_states);
    engine->stream_storage_states = NULL;
    engine->stream_storage_count = 0;
    engine->stream_storage_capacity = 0;
}

es_stream_storage_state_t* es_storage_writer_find_stream_state(
    es_engine_t* engine,
    uint32_t stream_id
) {
    if(!engine || stream_id == 0) {
        return NULL;
    }

    for(size_t i = 0; i < engine->stream_storage_count; ++i) {
        if(engine->stream_storage_states[i].stream_id == stream_id) {
            return &engine->stream_storage_states[i];
        }
    }

    return NULL;
}

es_status_t es_storage_writer_register_stream(
    es_engine_t* engine,
    uint32_t stream_id
) {
    if(!engine || stream_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(es_storage_writer_find_stream_state(engine, stream_id)) {
        return ES_OK;
    }

    es_status_t status = es_storage_writer_ensure_capacity(engine);
    if(status != ES_OK) {
        return status;
    }

    es_stream_storage_state_t* state = 
        &engine->stream_storage_states[engine->stream_storage_count];

    state->stream_id = stream_id;
    state->active_segment_index = 1;
    state->active_segment_size_bytes = 0;
    state->active_segment_file = NULL;

    engine->stream_storage_count++;

    return ES_OK;
}

es_status_t es_storage_writer_append_record(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* record
) {
    (void) engine;
    (void) stream_id;
    (void) record;

    return ES_OK;
}

es_status_t es_storage_writer_append_batch(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* records,
    size_t count
) {
    (void)engine;
    (void)stream_id;
    (void)records;
    (void)count;

    return ES_OK;
}