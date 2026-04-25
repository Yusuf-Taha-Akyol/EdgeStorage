#include "edgestorage/edgestorage.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

typedef struct {
    uint32_t value;
} counter_payload_t;

static long path_file_size(const char* path) {
    struct stat st;
    if(stat(path, &st) != 0) {
        return -1;
    }

    return (long)st.st_size;
}

static int expect_status(es_status_t actual, es_status_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%d actual=%d\n", label, expected, actual);
        return 1;
    }

    return 0;
}

int main(void) {
    system("rm -rf ./compression_delta_testdata");

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
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "delta_stream",
        .schema_version = 1,
        .record_type_count = 1,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./compression_delta_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(es_register_stream_schema(engine, &schema, &stream_id), ES_OK, "register stream") != 0) {
        es_close(engine);
        return 1;
    }

    counter_payload_t payloads[3] = {
        { .value = 10 },
        { .value = 11 },
        { .value = 12 }
    };

    es_record_t records[3] = {
        {
            .timestamp_ns = 1000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 1001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[1]
        },
        {
            .timestamp_ns = 1002000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[2]
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, records, 3), ES_OK, "write compressed batch") != 0) {
        es_close(engine);
        return 1;
    }

    es_close(engine);

    const char* segment_path = "./compression_delta_testdata/stream_1/segment_000001.seg";
    long actual_size = path_file_size(segment_path);

    /*
     * Uncompressed per record:
     * timestamp_ns 8 + record_type_id 2 + flags 2 + payload_size 4 + payload 4 = 20 bytes
     * 3 records = 60 bytes
     *
     * Delta timestamp encoded:
     * first record: 8 + 2 + 2 + 4 + 4 = 20 bytes
     * next records: 4 + 2 + 2 + 4 + 4 = 16 bytes each
     * total = 52 bytes
     */
    long expected_compressed_size = 52;
    long uncompressed_size = 60;

    if(actual_size != expected_compressed_size) {
        printf(
            "FAILED: compressed size mismatch expected=%ld actual=%ld\n",
            expected_compressed_size,
            actual_size
        );
        return 1;
    }

    if(actual_size >= uncompressed_size) {
        printf(
            "FAILED: compressed size should be smaller than uncompressed size compressed=%ld uncompressed=%ld\n",
            actual_size,
            uncompressed_size
        );
        return 1;
    }

        system("rm -rf ./compression_delta_invalid_testdata");

    es_config_t invalid_config = {
        .storage_path = "./compression_delta_invalid_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* invalid_engine = es_open(&invalid_config);
    if(!invalid_engine) {
        printf("FAILED: invalid_engine es_open returned NULL\n");
        return 1;
    }

    uint32_t invalid_stream_id = 0;
    if(expect_status(
        es_register_stream_schema(invalid_engine, &schema, &invalid_stream_id),
        ES_OK,
        "register invalid timestamp stream"
    ) != 0) {
        es_close(invalid_engine);
        return 1;
    }

    counter_payload_t invalid_payloads[2] = {
        { .value = 20 },
        { .value = 21 }
    };

    es_record_t invalid_records[2] = {
        {
            .timestamp_ns = 2000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &invalid_payloads[0]
        },
        {
            .timestamp_ns = 1000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &invalid_payloads[1]
        }
    };

    if(expect_status(
        es_write_record(invalid_engine, invalid_stream_id, &invalid_records[0]),
        ES_OK,
        "write first invalid timestamp record"
    ) != 0) {
        es_close(invalid_engine);
        return 1;
    }

    if(expect_status(
        es_write_record(invalid_engine, invalid_stream_id, &invalid_records[1]),
        ES_ERR_INVALID_ARG,
        "reject non-monotonic timestamp"
    ) != 0) {
        es_close(invalid_engine);
        return 1;
    }

    es_close(invalid_engine);

        system("rm -rf ./compression_delta_overflow_testdata");

    es_config_t overflow_config = {
        .storage_path = "./compression_delta_overflow_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* overflow_engine = es_open(&overflow_config);
    if(!overflow_engine) {
        printf("FAILED: overflow_engine es_open returned NULL\n");
        return 1;
    }

    uint32_t overflow_stream_id = 0;
    if(expect_status(
        es_register_stream_schema(overflow_engine, &schema, &overflow_stream_id),
        ES_OK,
        "register overflow timestamp stream"
    ) != 0) {
        es_close(overflow_engine);
        return 1;
    }

    counter_payload_t overflow_payloads[2] = {
        { .value = 30 },
        { .value = 31 }
    };

    es_record_t overflow_records[2] = {
        {
            .timestamp_ns = 1000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &overflow_payloads[0]
        },
        {
            .timestamp_ns = 1000000000ULL + 4294967296ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &overflow_payloads[1]
        }
    };

    if(expect_status(
        es_write_record(overflow_engine, overflow_stream_id, &overflow_records[0]),
        ES_OK,
        "write first overflow timestamp record"
    ) != 0) {
        es_close(overflow_engine);
        return 1;
    }

    if(expect_status(
        es_write_record(overflow_engine, overflow_stream_id, &overflow_records[1]),
        ES_ERR_INVALID_ARG,
        "reject timestamp delta overflow"
    ) != 0) {
        es_close(overflow_engine);
        return 1;
    }

    es_close(overflow_engine);

        system("rm -rf ./compression_delta_rollover_testdata");

    es_config_t rollover_config = {
        .storage_path = "./compression_delta_rollover_testdata",
        .segment_size_bytes = 40,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* rollover_engine = es_open(&rollover_config);
    if(!rollover_engine) {
        printf("FAILED: rollover_engine es_open returned NULL\n");
        return 1;
    }

    uint32_t rollover_stream_id = 0;
    if(expect_status(
        es_register_stream_schema(rollover_engine, &schema, &rollover_stream_id),
        ES_OK,
        "register rollover stream"
    ) != 0) {
        es_close(rollover_engine);
        return 1;
    }

    counter_payload_t rollover_payloads[3] = {
        { .value = 40 },
        { .value = 41 },
        { .value = 42 }
    };

    es_record_t rollover_records[3] = {
        {
            .timestamp_ns = 3000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &rollover_payloads[0]
        },
        {
            .timestamp_ns = 3001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &rollover_payloads[1]
        },
        {
            .timestamp_ns = 3002000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &rollover_payloads[2]
        }
    };

    if(expect_status(
        es_write_batch(rollover_engine, rollover_stream_id, rollover_records, 3),
        ES_OK,
        "write rollover compressed batch"
    ) != 0) {
        es_close(rollover_engine);
        return 1;
    }

    es_close(rollover_engine);

    long rollover_segment_1_size = path_file_size(
        "./compression_delta_rollover_testdata/stream_1/segment_000001.seg"
    );
    long rollover_segment_2_size = path_file_size(
        "./compression_delta_rollover_testdata/stream_1/segment_000002.seg"
    );

    /*
     * segment_size_bytes = 40
     *
     * First segment:
     * first record full timestamp = 20 bytes
     * second record delta timestamp = 16 bytes
     * total = 36 bytes
     *
     * Third record would exceed 40 bytes, so it rolls over.
     *
     * New segment must reset timestamp state:
     * third record must be written with full timestamp = 20 bytes
     */
    if(rollover_segment_1_size != 36) {
        printf(
            "FAILED: rollover segment 1 size mismatch expected=36 actual=%ld\n",
            rollover_segment_1_size
        );
        return 1;
    }

    if(rollover_segment_2_size != 20) {
        printf(
            "FAILED: rollover segment 2 size mismatch expected=20 actual=%ld\n",
            rollover_segment_2_size
        );
        return 1;
    }

    printf("compression_delta_test passed\n");
    return 0;
}