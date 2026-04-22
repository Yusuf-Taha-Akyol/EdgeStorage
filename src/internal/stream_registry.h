#ifndef ES_INTERNAL_STREAM_REGISTRY_H
#define ES_INTERNAL_STREAM_REGISTRY_H

#include "edgestorage/edgestorage.h"

int es_stream_registry_is_valid_schema(const es_stream_schema_t* schema);

es_status_t es_stream_registry_register(
    es_engine_t* engine,
    const es_stream_schema_t* schema,
    uint32_t* out_stream_id
);

int es_stream_registry_contains(
    const es_engine_t* engine,
    uint32_t stream_id
);

#endif