#include "edgestorage/edgestorage.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    uint32_t value;
} counter_payload_t;

static int expect_status(es_status_t actual, es_status_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%d actual=%d\n", label, expected, actual);
        return 1;
    }

    return 0;
}

static int expect_size(size_t actual, size_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%zu actual=%zu\n", label, expected, actual);
        return 1;
    }

    return 0;
}

static int expect_u32(uint32_t actual, uint32_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%u actual=%u\n", label, expected, actual);
        return 1;
    }

    return 0;
}

int main(void) {
    system("rm -rf ./read_query_testdata");

    es_field_def_t fields[] = {
        {
            .field_id = 1,
            .name = "value",
            .type = ES_TYPE_U32,
            .comp_hint = ES_COMP_NONE
        }
    };

    es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "counter",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(counter_payload_t)
        },
        {
            .record_type_id = 2,
            .record_name = "event_counter",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(counter_payload_t)
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "query_stream",
        .schema_version = 1,
        .record_type_count = 2,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./read_query_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 0
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(
        es_register_stream_schema(engine, &schema, &stream_id),
        ES_OK,
        "register stream"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    counter_payload_t payloads[3] = {
        { .value = 10 },
        { .value = 20 },
        { .value = 30 }
    };

    es_record_t records[3] = {
        {
            .timestamp_ns = 1000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 2000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[1]
        },
        {
            .timestamp_ns = 3000,
            .record_type_id = 2,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[2]
        }
    };

    if(expect_status(
        es_write_batch(engine, stream_id, records, 3),
        ES_OK,
        "write batch"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    es_query_t query = {
        .stream_id = stream_id,
        .start_ts_ns = 1500,
        .end_ts_ns = 3000,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t result = {0};

    if(expect_status(
        es_query_range(engine, &query, &result),
        ES_OK,
        "query range"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    if(expect_size(result.count, 1, "query result count") != 0) {
        es_result_free(&result);
        es_close(engine);
        return 1;
    }

    counter_payload_t* first_payload = (counter_payload_t*)result.records[0].payload;

    if(expect_u32(first_payload->value, 20, "first result payload") != 0) {
        es_result_free(&result);
        es_close(engine);
        return 1;
    }

    es_result_free(&result);
    es_close(engine);

    printf("read_query_test passed\n");
    return 0;
}