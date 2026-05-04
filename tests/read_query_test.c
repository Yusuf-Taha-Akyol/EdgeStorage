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
    es_query_t all_types_query = {
        .stream_id = stream_id,
        .start_ts_ns = 1500,
        .end_ts_ns = 3000,
        .record_type_id = 0,
        .limit = 0
    };

    es_result_t all_types_result = {0};

    if(expect_status(
        es_query_range(engine, &all_types_query, &all_types_result),
        ES_OK,
        "all types query range"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    if(expect_size(all_types_result.count, 2, "all types query result count") != 0) {
        es_result_free(&all_types_result);
        es_close(engine);
        return 1;
    }

    counter_payload_t* all_first_payload =
        (counter_payload_t*)all_types_result.records[0].payload;
    counter_payload_t* all_second_payload =
        (counter_payload_t*)all_types_result.records[1].payload;

    if(expect_u32(all_first_payload->value, 20, "all types first payload") != 0) {
        es_result_free(&all_types_result);
        es_close(engine);
        return 1;
    }

    if(expect_u32(all_second_payload->value, 30, "all types second payload") != 0) {
        es_result_free(&all_types_result);
        es_close(engine);
        return 1;
    }

    es_result_free(&all_types_result);
    es_close(engine);

        system("rm -rf ./read_query_multisegment_testdata");

    es_config_t multisegment_config = {
        .storage_path = "./read_query_multisegment_testdata",
        .segment_size_bytes = 71,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 0
    };

    es_engine_t* multisegment_engine = es_open(&multisegment_config);
    if(!multisegment_engine) {
        printf("FAILED: multisegment es_open returned NULL\n");
        return 1;
    }

    uint32_t multisegment_stream_id = 0;
    if(expect_status(
        es_register_stream_schema(multisegment_engine, &schema, &multisegment_stream_id),
        ES_OK,
        "register multisegment stream"
    ) != 0) {
        es_close(multisegment_engine);
        return 1;
    }

    counter_payload_t multisegment_payloads[3] = {
        { .value = 100 },
        { .value = 200 },
        { .value = 300 }
    };

    es_record_t multisegment_records[3] = {
        {
            .timestamp_ns = 1000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &multisegment_payloads[0]
        },
        {
            .timestamp_ns = 2000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &multisegment_payloads[1]
        },
        {
            .timestamp_ns = 3000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &multisegment_payloads[2]
        }
    };

    if(expect_status(
        es_write_batch(multisegment_engine, multisegment_stream_id, multisegment_records, 3),
        ES_OK,
        "write multisegment batch"
    ) != 0) {
        es_close(multisegment_engine);
        return 1;
    }

    es_query_t multisegment_query = {
        .stream_id = multisegment_stream_id,
        .start_ts_ns = 1000,
        .end_ts_ns = 3000,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t multisegment_result = {0};

    if(expect_status(
        es_query_range(multisegment_engine, &multisegment_query, &multisegment_result),
        ES_OK,
        "multisegment query range"
    ) != 0) {
        es_close(multisegment_engine);
        return 1;
    }

    if(expect_size(multisegment_result.count, 3, "multisegment query result count") != 0) {
        es_result_free(&multisegment_result);
        es_close(multisegment_engine);
        return 1;
    }

    counter_payload_t* multisegment_first_payload =
        (counter_payload_t*)multisegment_result.records[0].payload;
    counter_payload_t* multisegment_second_payload =
        (counter_payload_t*)multisegment_result.records[1].payload;
    counter_payload_t* multisegment_third_payload =
        (counter_payload_t*)multisegment_result.records[2].payload;

    if(expect_u32(multisegment_first_payload->value, 100, "multisegment first payload") != 0) {
        es_result_free(&multisegment_result);
        es_close(multisegment_engine);
        return 1;
    }

    if(expect_u32(multisegment_second_payload->value, 200, "multisegment second payload") != 0) {
        es_result_free(&multisegment_result);
        es_close(multisegment_engine);
        return 1;
    }

    if(expect_u32(multisegment_third_payload->value, 300, "multisegment third payload") != 0) {
        es_result_free(&multisegment_result);
        es_close(multisegment_engine);
        return 1;
    }

    es_result_free(&multisegment_result);

        es_query_t limited_query = {
        .stream_id = multisegment_stream_id,
        .start_ts_ns = 1000,
        .end_ts_ns = 3000,
        .record_type_id = 1,
        .limit = 2
    };

    es_result_t limited_result = {0};

    if(expect_status(
        es_query_range(multisegment_engine, &limited_query, &limited_result),
        ES_OK,
        "limited query range"
    ) != 0) {
        es_close(multisegment_engine);
        return 1;
    }

    if(expect_size(limited_result.count, 2, "limited query result count") != 0) {
        es_result_free(&limited_result);
        es_close(multisegment_engine);
        return 1;
    }

    counter_payload_t* limited_first_payload =
        (counter_payload_t*)limited_result.records[0].payload;
    counter_payload_t* limited_second_payload =
        (counter_payload_t*)limited_result.records[1].payload;

    if(expect_u32(limited_first_payload->value, 100, "limited first payload") != 0) {
        es_result_free(&limited_result);
        es_close(multisegment_engine);
        return 1;
    }

    if(expect_u32(limited_second_payload->value, 200, "limited second payload") != 0) {
        es_result_free(&limited_result);
        es_close(multisegment_engine);
        return 1;
    }

    es_result_free(&limited_result);
    es_close(multisegment_engine);

        system("rm -rf ./read_query_compressed_testdata");

    es_config_t compressed_config = {
        .storage_path = "./read_query_compressed_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* compressed_engine = es_open(&compressed_config);
    if(!compressed_engine) {
        printf("FAILED: compressed es_open returned NULL\n");
        return 1;
    }

    uint32_t compressed_stream_id = 0;
    if(expect_status(
        es_register_stream_schema(compressed_engine, &schema, &compressed_stream_id),
        ES_OK,
        "register compressed stream"
    ) != 0) {
        es_close(compressed_engine);
        return 1;
    }

    counter_payload_t compressed_payloads[3] = {
        { .value = 1000 },
        { .value = 2000 },
        { .value = 3000 }
    };

    es_record_t compressed_records[3] = {
        {
            .timestamp_ns = 1000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &compressed_payloads[0]
        },
        {
            .timestamp_ns = 1001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &compressed_payloads[1]
        },
        {
            .timestamp_ns = 1002000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &compressed_payloads[2]
        }
    };

    if(expect_status(
        es_write_batch(compressed_engine, compressed_stream_id, compressed_records, 3),
        ES_OK,
        "write compressed batch"
    ) != 0) {
        es_close(compressed_engine);
        return 1;
    }

    es_query_t compressed_query = {
        .stream_id = compressed_stream_id,
        .start_ts_ns = 1000500000ULL,
        .end_ts_ns = 1002000000ULL,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t compressed_result = {0};

    if(expect_status(
        es_query_range(compressed_engine, &compressed_query, &compressed_result),
        ES_OK,
        "compressed query range"
    ) != 0) {
        es_close(compressed_engine);
        return 1;
    }

    if(expect_size(compressed_result.count, 2, "compressed query result count") != 0) {
        es_result_free(&compressed_result);
        es_close(compressed_engine);
        return 1;
    }

    if(compressed_result.records[0].timestamp_ns != 1001000000ULL) {
        printf(
            "FAILED: compressed first timestamp expected=%llu actual=%llu\n",
            1001000000ULL,
            (unsigned long long)compressed_result.records[0].timestamp_ns
        );
        es_result_free(&compressed_result);
        es_close(compressed_engine);
        return 1;
    }

    if(compressed_result.records[1].timestamp_ns != 1002000000ULL) {
        printf(
            "FAILED: compressed second timestamp expected=%llu actual=%llu\n",
            1002000000ULL,
            (unsigned long long)compressed_result.records[1].timestamp_ns
        );
        es_result_free(&compressed_result);
        es_close(compressed_engine);
        return 1;
    }

    counter_payload_t* compressed_first_payload =
        (counter_payload_t*)compressed_result.records[0].payload;
    counter_payload_t* compressed_second_payload =
        (counter_payload_t*)compressed_result.records[1].payload;

    if(expect_u32(compressed_first_payload->value, 2000, "compressed first payload") != 0) {
        es_result_free(&compressed_result);
        es_close(compressed_engine);
        return 1;
    }

    if(expect_u32(compressed_second_payload->value, 3000, "compressed second payload") != 0) {
        es_result_free(&compressed_result);
        es_close(compressed_engine);
        return 1;
    }

    es_result_free(&compressed_result);
    es_close(compressed_engine);

    system("rm -rf ./read_query_compressed_rollover_testdata");

    es_config_t compressed_rollover_config = {
        .storage_path = "./read_query_compressed_rollover_testdata",
        .segment_size_bytes = 71,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* compressed_rollover_engine = es_open(&compressed_rollover_config);
    if(!compressed_rollover_engine) {
        printf("FAILED: compressed rollover es_open returned NULL\n");
        return 1;
    }

    uint32_t compressed_rollover_stream_id = 0;
    if(expect_status(
        es_register_stream_schema(
            compressed_rollover_engine,
            &schema,
            &compressed_rollover_stream_id
        ),
        ES_OK,
        "register compressed rollover stream"
    ) != 0) {
        es_close(compressed_rollover_engine);
        return 1;
    }

    counter_payload_t compressed_rollover_payloads[3] = {
        { .value = 4000},
        { .value = 5000},
        { .value = 6000}
    };

    es_record_t compressed_rollover_records[3] = {
        {
            .timestamp_ns = 2000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &compressed_rollover_payloads[0]
        },
        {
            .timestamp_ns = 2001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &compressed_rollover_payloads[1]
        },
        {
            .timestamp_ns = 2002000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &compressed_rollover_payloads[2]
        }
    };

    if(expect_status(
        es_write_batch(
            compressed_rollover_engine,
            compressed_rollover_stream_id,
            compressed_rollover_records,
            3
        ),
        ES_OK,
        "write compressed rollover batch"
    ) != 0) {
        es_close(compressed_rollover_engine);
        return 1;
    }

    es_query_t compressed_rollover_query = {
        .stream_id = compressed_rollover_stream_id,
        .start_ts_ns = 2000000000ULL,
        .end_ts_ns = 2002000000ULL,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t compressed_rollover_result = {0};

    if(expect_status(
        es_query_range(
            compressed_rollover_engine,
            &compressed_rollover_query,
            &compressed_rollover_result
        ),
        ES_OK,
        "compressed rollover query range"
    ) != 0) {
        es_close(compressed_rollover_engine);
        return 1;
    }

    if(expect_size(
        compressed_rollover_result.count,
        3,
        "compressed rollover query result count"
    ) != 0) {
        es_result_free(&compressed_rollover_result);
        es_close(compressed_rollover_engine);
        return 1;
    }

    if(compressed_rollover_result.records[0].timestamp_ns != 2000000000ULL) {
        printf(
            "FAILED: compressed rollover first timestamp expected=%llu actual=%llu\n",
            2000000000ULL,
            (unsigned long long)compressed_rollover_result.records[0].timestamp_ns
        );
        es_result_free(&compressed_rollover_result);
        es_close(compressed_rollover_engine);

        return 1;
    }

    if(compressed_rollover_result.records[1].timestamp_ns != 2001000000ULL) {
        printf(
            "FAILED: compressed rollover second timestamp expected=%llu actual=%llu\n",
            2001000000ULL,
            (unsigned long long)compressed_rollover_result.records[1].timestamp_ns
        );
        es_result_free(&compressed_rollover_result);
        es_close(compressed_rollover_engine);
        return 1;
    }

    if(compressed_rollover_result.records[2].timestamp_ns != 2002000000ULL) {
        printf(
            "FAILED: compressed rollover third timestamp expected=%llu actual=%llu\n",
            2002000000ULL,
            (unsigned long long)compressed_rollover_result.records[2].timestamp_ns
        );
        es_result_free(&compressed_rollover_result);
        es_close(compressed_rollover_engine);
        return 1;
    }

    counter_payload_t* compressed_rollover_first_payload = 
        (counter_payload_t*)compressed_rollover_result.records[0].payload;
    counter_payload_t* compressed_rollover_second_payload = 
        (counter_payload_t*)compressed_rollover_result.records[1].payload;
    counter_payload_t* compressed_rollover_third_payload =
        (counter_payload_t*)compressed_rollover_result.records[2].payload;

    if(expect_u32(
        compressed_rollover_first_payload->value,
        4000,
        "compressed rollover first payload"
    ) != 0) {
        es_result_free(&compressed_rollover_result);
        es_close(compressed_rollover_engine);
        return 1;
    }

    if(expect_u32(
        compressed_rollover_second_payload->value,
        5000,
        "compressed rollover second payload"
    ) != 0) {
        es_result_free(&compressed_rollover_result);
        es_close(compressed_rollover_engine);
        return 1;
    }

    if(expect_u32(
        compressed_rollover_third_payload->value,
        6000,
        "compressed rollover third payload"
    ) != 0) {
        es_result_free(&compressed_rollover_result);
        es_close(compressed_rollover_engine);
        return 1;
    }

    es_result_free(&compressed_rollover_result);
    es_close(compressed_rollover_engine);

    printf("read_query_test passed\n");
    return 0;
}