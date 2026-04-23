#include "runtime.h"
#include "storage_writer.h"


#include <stdlib.h>

es_engine_t* es_runtime_create(const es_config_t* config) {
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

    engine->stream_storage_states = NULL;
    engine->stream_storage_count = 0;
    engine->stream_storage_capacity = 0;

    if(es_storage_writer_init(engine) != ES_OK) {
        free(engine);
        return NULL;
    }

    return engine;
}

void es_runtime_destroy(es_engine_t* engine) {
    if(!engine) {
        return;
    }

    engine->state = ES_ENGINE_STATE_CLOSED;
    es_storage_writer_shutdown(engine);
    free(engine->registered_stream_ids);
    engine->registered_stream_ids = NULL;
    engine->registered_stream_count = 0;
    engine->registered_stream_capacity = 0;
    free(engine);
}

int es_runtime_is_open(const es_engine_t* engine) {
    return engine && engine->state == ES_ENGINE_STATE_OPEN;
}