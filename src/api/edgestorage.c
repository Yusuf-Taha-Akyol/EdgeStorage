#include "/Users/yusuf_taha/projects/EdgeStorage/include/edgestorage/edgestorage.h"

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

es_status_t es_register_stream(
    es_engine_t* engine,
    const es_stream_def_t* def,
    uint32_t* out_stream_id
) {
    (void)engine;
    (void)record;
    return ES_OK;
}