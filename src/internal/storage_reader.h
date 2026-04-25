#ifndef ES_INTERNAL_STORAGE_READER_H
#define ES_INTERNAL_STORAGE_READER_H

#include "edgestorage/edgestorage.h"

es_status_t es_storage_reader_query_range(
    es_engine_t* engine,
    const es_query_t* query,
    es_result_t* out_result
);

#endif