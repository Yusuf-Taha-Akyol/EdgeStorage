#ifndef ES_INTERNAL_RUNTIME_H
#define ES_INTERNAL_RUNTIME_H

#include "edgestorage/edgestorage.h"

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

es_engine_t* es_runtime_create(const es_config_t* config);
void es_runtime_destroy(es_engine_t* engine);
int es_runtime_is_open(const es_engine_t* engine);

#endif