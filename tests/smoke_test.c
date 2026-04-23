#include "edgestorage/edgestorage.h"
#include <sys/stat.h>
#include <stdio.h>

static int fail_at(const char* label, es_engine_t* engine) {
    printf("FAILED AT: %s\n", label);
    return 1;
}

static int expect_status(es_status_t actual, es_status_t expected) {
    return actual == expected ? 0 : 1;
}

static int path_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static long path_file_size(const char* path) {
    struct stat st;
    if(stat(path, &st) != 0) {
        return -1;
    }

    return (long)st.st_size;
}

static es_field_def_t test_fields[] = {
    {
        .field_id = 1,
        .name = "value",
        .type = ES_TYPE_F32,
        .comp_hint = ES_COMP_NONE
    }
};

static es_record_type_def_t test_record_types[] = {
    {
        .record_type_id = 1,
        .record_name = "imu",
        .field_count = 1,
        .fields = test_fields,
        .payload_size = sizeof(float)
    }
};

static es_stream_schema_t test_schema = {
    .stream_name = "telemetry",
    .schema_version = 1,
    .record_type_count = 1,
    .record_types = test_record_types
};

static es_stream_schema_t invalid_schema = {
  .stream_name = "invalid",
  .schema_version = 1,
  .record_type_count = 0,
  .record_types = test_record_types  
};

static float test_value = 42.0f;
static es_record_t test_record = {
    .timestamp_ns = 0,
    .record_type_id = 1,
    .flags = 0,
    .payload_size = sizeof(float),
    .payload = &test_value
};

static es_record_t invalid_record_type_record = {
    .timestamp_ns = 0,
    .record_type_id = 0,
    .flags = 0,
    .payload_size = sizeof(float),
    .payload = &test_value
};

static es_record_t invalid_payload_record = {
    .timestamp_ns = 0,
    .record_type_id = 1,
    .flags = 0,
    .payload_size = sizeof(float),
    .payload = NULL
};

int main(void) {
    es_config_t config = {
        .storage_path = "./testdata",
        .segment_size_bytes = 32,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_config_t invalid_config = {
        .storage_path = NULL,
        .segment_size_bytes = 0,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    if(es_open(&invalid_config) != NULL) {
        return fail_at("open with invalid config should return NULL", NULL);
    }

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        return fail_at("open with valid config should return engine", NULL);
    }

    uint32_t stream_id = 0;
    if(expect_status(es_register_stream_schema(engine, &test_schema, &stream_id), ES_OK) != 0) {
        es_close(engine);
        return fail_at("register_stream_schema failed", engine);
    }

    if(!path_exists("./testdata")) {
        es_close(engine);
        return fail_at("testdata directory does not exist", engine);
    }

    if(!path_exists("./testdata/stream_1")) {
        es_close(engine);
        return fail_at("stream directory does not exist", engine);
    }

    if(expect_status(es_write_record(engine, 0, &test_record), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_record with invalid stream ID", engine);
    }

    if(expect_status(es_write_record(engine, 9999, &test_record), ES_ERR_NOT_FOUND) != 0) {
        es_close(engine);
        return fail_at("write_record with non-existent stream ID", engine);
    }

    if(expect_status(es_write_record(engine, stream_id, NULL), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_record with NULL payload", engine);
    }

    if(expect_status(es_write_record(engine, stream_id, &invalid_record_type_record), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_record with invalid record type", engine);
    }

    if(expect_status(es_write_record(engine, stream_id, &invalid_payload_record), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_record with invalid payload", engine);
    }

    if(expect_status(es_write_batch(engine, stream_id, &test_record, 0), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_batch with zero record count", engine);
    }

    if(expect_status(es_write_batch(engine, 0, &test_record, 1), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_batch with invalid stream ID", engine);
    }

    if(expect_status(es_write_batch(engine, 9999, &test_record, 1), ES_ERR_NOT_FOUND) != 0) {
        es_close(engine);
        return fail_at("write_batch with non-existent stream ID", engine);
    }

    if(expect_status(es_write_batch(engine, stream_id, &invalid_record_type_record, 1), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_batch with invalid record type", engine);
    }

    if(expect_status(es_write_batch(engine, stream_id, &invalid_payload_record, 1), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("write_batch with invalid payload", engine);
    }

    if(expect_status(es_write_record(engine, stream_id, &test_record), ES_OK) != 0) {
        es_close(engine);
        return fail_at("write_record failed", engine);
    }

    if(!path_exists("./testdata/stream_1/segment_000001.seg")) {
        es_close(engine);
        return fail_at("segment file does not exist", engine);
    }

    if(path_file_size("./testdata/stream_1/segment_000001.seg") <= 0) {
        es_close(engine);
        return fail_at("segment file is empty", engine);
    }

    if(expect_status(es_write_batch(engine, stream_id, &test_record, 1), ES_OK) != 0) {
        es_close(engine);
        return fail_at("write_batch failed", engine);
    }

    if(!path_exists("./testdata/stream_1/segment_000002.seg")) {    
        es_close(engine);
        return fail_at("segment file does not exist", engine);
    }

    if((path_file_size("./testdata/stream_1/segment_000002.seg")) <= 0) {
        es_close(engine);
        return fail_at("segment file is empty", engine);
    }

    es_query_t invalid_query = {
        .stream_id = 0,
        .start_ts_ns = 0,
        .end_ts_ns = 1000,
        .record_type_id = 0,
        .limit = 10
    };

    es_result_t result = {0};

    if(expect_status(es_query_range(engine, &invalid_query, &result), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("query_range with invalid query should return ES_ERR_INVALID_ARG", engine);
    }

    if(expect_status(es_query_range(engine, NULL, &result), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("query_range with NULL query should return ES_ERR_INVALID_ARG", engine);
    }

    if(expect_status(es_query_range(engine, &invalid_query, NULL), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("query_range with NULL result should return ES_ERR_INVALID_ARG", engine);
    }

    es_query_t missing_stream_query = {
        .stream_id = 9999,
        .start_ts_ns = 0,
        .end_ts_ns = 1000,
        .record_type_id = 0,
        .limit = 10
    };

    if(expect_status(es_query_range(engine, &missing_stream_query, &result), ES_ERR_NOT_FOUND) != 0) {
        es_close(engine);
        return fail_at("query_range with non-existent stream ID should return ES_ERR_NOT_FOUND", engine);
    }

    es_query_t valid_query = {
        .stream_id = stream_id,
        .start_ts_ns = 0,
        .end_ts_ns = 1000,
        .record_type_id = 0,
        .limit = 10
    };

    if(expect_status(es_query_range(engine, &valid_query, &result), ES_OK) != 0) {
        es_close(engine);
        return fail_at("query_range with valid query should return ES_OK", engine);
    }

    if(stream_id == 0) {
        es_close(engine);
        return fail_at("stream ID is zero", engine);
    }

    if(expect_status(es_register_stream_schema(engine, &invalid_schema, &stream_id), ES_ERR_INVALID_ARG) != 0) {
        es_close(engine);
        return fail_at("register_stream_schema with invalid schema should return ES_ERR_INVALID_ARG", engine);
    }

    es_close(engine);
    return 0;
}