#include "storage_reader.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

static es_status_t es_storage_reader_read_uncompressed_record(
    FILE* file,
    es_record_t* out_record
) {
    if(!file || !out_record) {
        return ES_ERR_INVALID_ARG;
    }

    memset(out_record, 0, sizeof(*out_record));

    if(fread(&out_record->timestamp_ns, sizeof(out_record->timestamp_ns), 1, file) != 1) {
        if(feof(file)) {
            return ES_ERR_NOT_FOUND;
        }

        return ES_ERR_IO;
    }

    if(fread(&out_record->record_type_id, sizeof(out_record->record_type_id), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(fread(&out_record->flags, sizeof(out_record->flags), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(fread(&out_record->payload_size, sizeof(out_record->payload_size), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(out_record->payload_size > 0) {
        void* payload = malloc(out_record->payload_size);
        if(!payload) {
            return ES_ERR_OOM;
        }

        if(fread(payload, 1, out_record->payload_size, file) != out_record->payload_size) {
            free(payload);
            return ES_ERR_IO;
        }

        out_record->payload = payload;
    }

    return ES_OK;
}

static es_status_t es_storage_reader_append_result_record(
    es_result_t* result,
    const es_record_t* record
) {
    if(!result || !record) {
        return ES_ERR_INVALID_ARG;
    }

    size_t new_count = result->count + 1;

    es_record_t* new_records = (es_record_t*)realloc(
        result->records,
        new_count * sizeof(es_record_t)
    );

    if(!new_records) {
        return ES_ERR_OOM;
    }

    result->records = new_records;
    result->records[result->count] = *record;
    result->count = new_count;

    return ES_OK;
}

static int es_storage_reader_record_matches_query(
    const es_record_t* record,
    const es_query_t* query
) {
    if(!record || !query) {
        return 0;
    }

    if(record->timestamp_ns < query->start_ts_ns) {
        return 0;
    }

    if(record->timestamp_ns > query->end_ts_ns) {
        return 0;
    }

    if(query->record_type_id != 0 && record->record_type_id != query->record_type_id) {
        return 0;
    }

    return 1;
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

        while(1) {
            es_record_t record;
            es_status_t read_status = es_storage_reader_read_uncompressed_record(
                file,
                &record
            );

            if(read_status == ES_ERR_NOT_FOUND) {
                break;
            }

            if(read_status != ES_OK) {
                fclose(file);
                es_result_free(out_result);
                return read_status;
            }

            if(es_storage_reader_record_matches_query(&record, query)) {
                es_status_t append_status = es_storage_reader_append_result_record(
                    out_result,
                    &record
                );

                if(append_status != ES_OK) {
                    free((void*)record.payload);
                    fclose(file);
                    es_result_free(out_result);
                    return append_status;
                }
            } else {
                free((void*)record.payload);
            }

            if(query->limit > 0 && out_result->count >= query->limit) {
                fclose(file);
                return ES_OK;
            }
        }

        fclose(file);
    }

    return ES_OK;
}