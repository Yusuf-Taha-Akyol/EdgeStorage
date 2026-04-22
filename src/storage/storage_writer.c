#include "storage_writer.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>
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

es_status_t es_storage_writer_register_stream(
    es_engine_t* engine,
    uint32_t stream_id
) {
    (void) engine;
    (void) stream_id;

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