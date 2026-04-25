#include "storage_reader.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

static es_status_t es_storage_reader_build_stream_dir_path(
    const es_engine_t* engine,
    uint32_t stream_id,
    char* out_path,
    size_t out_path_size
) {
    if(!engine || !out_path || out_path_size == 0 || stream_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    int written = snprintf(
        out_path,
        out_path_size,
        "%s/stream_%u",
        engine->config.storage_path,
        stream_id
    );

    if(written < 0 || (size_t)written >= out_path_size) {
        return ES_ERR_INTERNAL;
    }

    return ES_OK;
}

static es_status_t es_storage_reader_build_segment_path(
    const char* stream_dir_path,
    uint32_t segment_index,
    char* out_path,
    size_t out_path_size
) {
    if(!stream_dir_path || !out_path || out_path_size == 0 || segment_index == 0) {
        return ES_ERR_INVALID_ARG;
    }

    int written = snprintf(
        out_path,
        out_path_size,
        "%s/segment_%06u.seg",
        stream_dir_path,
        segment_index
    );

    if(written < 0 || (size_t)written >= out_path_size) {
        return ES_ERR_INTERNAL;
    }

    return ES_OK;
}

static int es_storage_reader_file_exists(const char*path) {
    struct stat st;
    return path && stat(path, &st) == 0;
}

es_status_t es_storage_reader_query_range(
    es_engine_t* engine,
    const es_query_t* query,
    es_result_t* out_result
) {
    if(!engine || !query || !out_result) {
        return ES_ERR_INVALID_ARG;
    }

    out_result->records = NULL;
    out_result->count = 0;

    char stream_dir_path[512];
    es_status_t status = es_storage_reader_build_stream_dir_path(
        engine,
        query->stream_id,
        stream_dir_path,
        sizeof(stream_dir_path)
    );

    if(status != ES_OK) {
        return status;
    }

    for(uint32_t segment_index = 1; ; ++segment_index) {
        char segment_path[512];

        status = es_storage_reader_build_segment_path(
            stream_dir_path,
            segment_index,
            segment_path,
            sizeof(segment_path)
        );

        if(status != ES_OK) {
            return status;
        }

        if(!es_storage_reader_file_exists(segment_path)) {
            break;
        }

        FILE* file = fopen(segment_path, "rb");
        if(!file) {
            return ES_ERR_IO;
        }

        fclose(file);
    }

    return ES_OK;
}