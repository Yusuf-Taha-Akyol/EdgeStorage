#include "edgestorage/edgestorage.h"
#include "compression.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

typedef struct {
    int32_t a;
    int32_t b;
    int32_t c;
    int32_t d;
    int32_t e;
    int32_t f;
    int32_t g;
    int32_t h;
} int32_payload_t;

static int expect_status(es_status_t actual, es_status_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%d actual=%d\n", label, expected, actual);
        return 1;
    }

    return 0;
}

static int expect_u8(uint8_t actual, uint8_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%u actual=%u\n", label, expected, actual);
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

static int expect_payload_equal(
    const void* actual,
    const void* expected,
    size_t size,
    const char* label
) {
    if(!actual || !expected || memcmp(actual, expected, size) != 0) {
        printf("FAILED: %s payload mismatch\n", label);
        return 1;
    }

    return 0;
}

static int expect_encoded_header(
    FILE* file,
    uint8_t expected_timestamp_encoding,
    uint8_t expected_payload_encoding,
    uint32_t expected_uncompressed_payload_size,
    uint32_t expected_encoded_payload_size,
    const char* label
) {
    es_encoded_record_header_t header;

    if(fread(&header, sizeof(header), 1, file) != 1) {
        printf("FAILED: %s could not read encoded header\n", label);
        return 1;
    }

    if(expect_u8(header.timestamp_encoding, expected_timestamp_encoding, label) != 0) {
        return 1;
    }

    if(expect_u8(header.payload_encoding, expected_payload_encoding, label) != 0) {
        return 1;
    }

    if(expect_u32(header.uncompressed_payload_size, expected_uncompressed_payload_size, label) != 0) {
        return 1;
    }

    if(expect_u32(header.encoded_payload_size, expected_encoded_payload_size, label) != 0) {
        return 1;
    }

    return 0;
}

static int test_smooth_int32_payload_uses_delta_i8(void) {
    system("rm -rf ./compression_payload_delta_testdata");

    es_field_def_t fields[] = {
        { .field_id = 1, .name = "a", .type = ES_TYPE_I32, .comp_hint = ES_COMP_NONE }
    };

    es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "smooth_i32",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(int32_payload_t)
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "payload_delta_stream",
        .schema_version = 1,
        .record_type_count = 1,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./compression_payload_delta_testdata",
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

    int32_payload_t payloads[3] = {
        { 100, 200, 300, 400, 500, 600, 700, 800 },
        { 101, 201, 301, 401, 501, 601, 701, 801 },
        { 102, 202, 302, 402, 502, 602, 702, 802 }
    };

    es_record_t records[3] = {
        {
            .timestamp_ns = 1000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 1001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[1]
        },
        {
            .timestamp_ns = 1002000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[2]
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, records, 3), ES_OK, "write smooth i32 batch") != 0) {
        es_close(engine);
        return 1;
    }

    es_query_t query = {
        .stream_id = stream_id,
        .start_ts_ns = 0,
        .end_ts_ns = 2000000000ULL,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t result;
    if(expect_status(es_query_range(engine, &query, &result), ES_OK, "query smooth i32 records") != 0) {
        es_close(engine);
        return 1;
    }

    if(result.count != 3) {
        printf("FAILED: query result count expected=3 actual=%zu\n", result.count);
        es_result_free(&result);
        es_close(engine);
        return 1;
    }

    for(size_t i = 0; i < result.count; ++i) {
        if(expect_payload_equal(
            result.records[i].payload,
            &payloads[i],
            sizeof(int32_payload_t),
            "roundtrip payload"
        ) != 0) {
            es_result_free(&result);
            es_close(engine);
            return 1;
        }
    }

    es_result_free(&result);
    es_close(engine);

    const char* segment_path = "./compression_payload_delta_testdata/stream_1/segment_000001.seg";
    FILE* file = fopen(segment_path, "rb");
    if(!file) {
        printf("FAILED: could not open segment\n");
        return 1;
    }

    long segment_header_size = 32;
    if(fseek(file, segment_header_size, SEEK_SET) != 0) {
        fclose(file);
        printf("FAILED: could not seek segment header\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_FULL_U64,
        ES_PAYLOAD_ENCODING_RAW,
        sizeof(int32_payload_t),
        sizeof(int32_payload_t),
        "record 1 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    if(fseek(file, sizeof(uint64_t) + sizeof(int32_payload_t), SEEK_CUR) != 0) {
        fclose(file);
        printf("FAILED: could not seek record 1 body\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_DELTA_U32,
        ES_PAYLOAD_ENCODING_DELTA_I32_I8,
        sizeof(int32_payload_t),
        8 * (uint32_t)sizeof(int8_t),
        "record 2 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    if(fseek(file, sizeof(uint32_t) + 8 * (long)sizeof(int8_t), SEEK_CUR) != 0) {
        fclose(file);
        printf("FAILED: could not seek record 2 body\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_DELTA_U32,
        ES_PAYLOAD_ENCODING_DELTA_I32_I8,
        sizeof(int32_payload_t),
        8 * (uint32_t)sizeof(int8_t),
        "record 3 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    fclose(file);
    return 0;
}

static int test_int32_payload_uses_delta_i16_when_i8_overflows(void) {
    system("rm -rf ./compression_payload_delta_i16_testdata");

    es_field_def_t fields[] = {
        { .field_id = 1, .name = "a", .type = ES_TYPE_I32, .comp_hint = ES_COMP_NONE }
    };

    es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "i16_delta",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(int32_payload_t)
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "payload_delta_i16_stream",
        .schema_version = 1,
        .record_type_count = 1,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./compression_payload_delta_i16_testdata",
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
    if(expect_status(es_register_stream_schema(engine, &schema, &stream_id), ES_OK, "register i16 stream") != 0) {
        es_close(engine);
        return 1;
    }

    int32_payload_t payloads[2] = {
        { 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000 },
        { 1200, 2200, 3200, 4200, 5200, 6200, 7200, 8200 }
    };

    es_record_t records[2] = {
        {
            .timestamp_ns = 2000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 2001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[1]
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, records, 2), ES_OK, "write i16 delta batch") != 0) {
        es_close(engine);
        return 1;
    }

    es_query_t query = {
        .stream_id = stream_id,
        .start_ts_ns = 0,
        .end_ts_ns = 3000000000ULL,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t result;
    if(expect_status(es_query_range(engine, &query, &result), ES_OK, "query i16 delta records") != 0) {
        es_close(engine);
        return 1;
    }

    if(result.count != 2) {
        printf("FAILED: i16 query result count expected=2 actual=%zu\n", result.count);
        es_result_free(&result);
        es_close(engine);
        return 1;
    }

    for(size_t i = 0; i < result.count; ++i) {
        if(expect_payload_equal(
            result.records[i].payload,
            &payloads[i],
            sizeof(int32_payload_t),
            "i16 roundtrip payload"
        ) != 0) {
            es_result_free(&result);
            es_close(engine);
            return 1;
        }
    }

    es_result_free(&result);
    es_close(engine);

    const char* segment_path = "./compression_payload_delta_i16_testdata/stream_1/segment_000001.seg";
    FILE* file = fopen(segment_path, "rb");
    if(!file) {
        printf("FAILED: could not open i16 segment\n");
        return 1;
    }

    long segment_header_size = 32;
    if(fseek(file, segment_header_size, SEEK_SET) != 0) {
        fclose(file);
        printf("FAILED: could not seek i16 segment header\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_FULL_U64,
        ES_PAYLOAD_ENCODING_RAW,
        sizeof(int32_payload_t),
        sizeof(int32_payload_t),
        "i16 record 1 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    if(fseek(file, sizeof(uint64_t) + sizeof(int32_payload_t), SEEK_CUR) != 0) {
        fclose(file);
        printf("FAILED: could not seek i16 record 1 body\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_DELTA_U32,
        ES_PAYLOAD_ENCODING_DELTA_I32_I16,
        sizeof(int32_payload_t),
        8 * (uint32_t)sizeof(int16_t),
        "i16 record 2 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    fclose(file);
    return 0;
}

typedef struct {
    unsigned char bytes[6];
} six_byte_payload_t;

static int test_incompatible_payload_size_falls_back_to_raw(void) {
    system("rm -rf ./compression_payload_delta_incompatible_testdata");

    es_field_def_t fields[] = {
        { .field_id = 1, .name = "bytes", .type = ES_TYPE_U32, .comp_hint = ES_COMP_NONE }
    };

    es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "six_byte_payload",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(six_byte_payload_t)
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "payload_delta_incompatible_stream",
        .schema_version = 1,
        .record_type_count = 1,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./compression_payload_delta_incompatible_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: incompatible es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(es_register_stream_schema(engine, &schema, &stream_id), ES_OK, "register incompatible stream") != 0) {
        es_close(engine);
        return 1;
    }

    six_byte_payload_t payloads[2] = {
        { .bytes = { 1, 2, 3, 4, 5, 6 } },
        { .bytes = { 2, 3, 4, 5, 6, 7 } }
    };

    es_record_t records[2] = {
        {
            .timestamp_ns = 3000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(six_byte_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 3001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(six_byte_payload_t),
            .payload = &payloads[1]
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, records, 2), ES_OK, "write incompatible batch") != 0) {
        es_close(engine);
        return 1;
    }

    es_query_t query = {
        .stream_id = stream_id,
        .start_ts_ns = 0,
        .end_ts_ns = 4000000000ULL,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t result;
    if(expect_status(es_query_range(engine, &query, &result), ES_OK, "query incompatible records") != 0) {
        es_close(engine);
        return 1;
    }

    if(result.count != 2) {
        printf("FAILED: incompatible query result count expected=2 actual=%zu\n", result.count);
        es_result_free(&result);
        es_close(engine);
        return 1;
    }

    for(size_t i = 0; i < result.count; ++i) {
        if(expect_payload_equal(
            result.records[i].payload,
            &payloads[i],
            sizeof(six_byte_payload_t),
            "incompatible roundtrip payload"
        ) != 0) {
            es_result_free(&result);
            es_close(engine);
            return 1;
        }
    }

    es_result_free(&result);
    es_close(engine);

    const char* segment_path = "./compression_payload_delta_incompatible_testdata/stream_1/segment_000001.seg";
    FILE* file = fopen(segment_path, "rb");
    if(!file) {
        printf("FAILED: could not open incompatible segment\n");
        return 1;
    }

    long segment_header_size = 32;
    if(fseek(file, segment_header_size, SEEK_SET) != 0) {
        fclose(file);
        printf("FAILED: could not seek incompatible segment header\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_FULL_U64,
        ES_PAYLOAD_ENCODING_RAW,
        sizeof(six_byte_payload_t),
        sizeof(six_byte_payload_t),
        "incompatible record 1 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    if(fseek(file, sizeof(uint64_t) + sizeof(six_byte_payload_t), SEEK_CUR) != 0) {
        fclose(file);
        printf("FAILED: could not seek incompatible record 1 body\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_DELTA_U32,
        ES_PAYLOAD_ENCODING_RAW,
        sizeof(six_byte_payload_t),
        sizeof(six_byte_payload_t),
        "incompatible record 2 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    fclose(file);
    return 0;
}

static int test_large_int32_delta_falls_back_to_raw(void) {
    system("rm -rf ./compression_payload_delta_large_delta_testdata");

    es_field_def_t fields[] = {
        { .field_id = 1, .name = "a", .type = ES_TYPE_I32, .comp_hint = ES_COMP_NONE }
    };

    es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "large_delta",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(int32_payload_t)
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "payload_delta_large_delta_stream",
        .schema_version = 1,
        .record_type_count = 1,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./compression_payload_delta_large_delta_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: large delta es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(es_register_stream_schema(engine, &schema, &stream_id), ES_OK, "register large delta stream") != 0) {
        es_close(engine);
        return 1;
    }

    int32_payload_t payloads[2] = {
        { 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000 },
        { 100000, 101000, 102000, 103000, 104000, 105000, 106000, 107000 }
    };

    es_record_t records[2] = {
        {
            .timestamp_ns = 4000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 4001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[1]
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, records, 2), ES_OK, "write large delta batch") != 0) {
        es_close(engine);
        return 1;
    }

    es_query_t query = {
        .stream_id = stream_id,
        .start_ts_ns = 0,
        .end_ts_ns = 5000000000ULL,
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t result;
    if(expect_status(es_query_range(engine, &query, &result), ES_OK, "query large delta records") != 0) {
        es_close(engine);
        return 1;
    }

    if(result.count != 2) {
        printf("FAILED: large delta query result count expected=2 actual=%zu\n", result.count);
        es_result_free(&result);
        es_close(engine);
        return 1;
    }

    for(size_t i = 0; i < result.count; ++i) {
        if(expect_payload_equal(
            result.records[i].payload,
            &payloads[i],
            sizeof(int32_payload_t),
            "large delta roundtrip payload"
        ) != 0) {
            es_result_free(&result);
            es_close(engine);
            return 1;
        }
    }

    es_result_free(&result);
    es_close(engine);

    const char* segment_path = "./compression_payload_delta_large_delta_testdata/stream_1/segment_000001.seg";
    FILE* file = fopen(segment_path, "rb");
    if(!file) {
        printf("FAILED: could not open large delta segment\n");
        return 1;
    }

    long segment_header_size = 32;
    if(fseek(file, segment_header_size, SEEK_SET) != 0) {
        fclose(file);
        printf("FAILED: could not seek large delta segment header\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_FULL_U64,
        ES_PAYLOAD_ENCODING_RAW,
        sizeof(int32_payload_t),
        sizeof(int32_payload_t),
        "large delta record 1 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    if(fseek(file, sizeof(uint64_t) + sizeof(int32_payload_t), SEEK_CUR) != 0) {
        fclose(file);
        printf("FAILED: could not seek large delta record 1 body\n");
        return 1;
    }

    if(expect_encoded_header(
        file,
        ES_TIMESTAMP_ENCODING_DELTA_U32,
        ES_PAYLOAD_ENCODING_RAW,
        sizeof(int32_payload_t),
        sizeof(int32_payload_t),
        "large delta record 2 header"
    ) != 0) {
        fclose(file);
        return 1;
    }

    fclose(file);
    return 0;
}

static int test_rollover_resets_previous_payload_state(void) {
    system("rm -rf ./compression_payload_delta_rollover_testdata");

    es_field_def_t fields[] = {
        { .field_id = 1, .name = "a", .type = ES_TYPE_I32, .comp_hint = ES_COMP_NONE }
    };

    es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "rollover_i32",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(int32_payload_t)
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "payload_delta_rollover_stream",
        .schema_version = 1,
        .record_type_count = 1,
        .record_types = record_types
    };

    /*
     * Header 32 + encoded header 16 + full timestamp 8 + payload 32 = 88.
     * Segment size 100 allows exactly one first/full record per segment.
     * A second record would require at least 16 + 4 + 8 = 28 more bytes,
     * so rollover must happen before writing it.
     */
    es_config_t config = {
        .storage_path = "./compression_payload_delta_rollover_testdata",
        .segment_size_bytes = 100,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: rollover payload es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(es_register_stream_schema(engine, &schema, &stream_id), ES_OK, "register payload rollover stream") != 0) {
        es_close(engine);
        return 1;
    }

    int32_payload_t payloads[3] = {
        { 10, 20, 30, 40, 50, 60, 70, 80 },
        { 11, 21, 31, 41, 51, 61, 71, 81 },
        { 12, 22, 32, 42, 52, 62, 72, 82 }
    };

    es_record_t records[3] = {
        {
            .timestamp_ns = 5000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 5001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[1]
        },
        {
            .timestamp_ns = 5002000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(int32_payload_t),
            .payload = &payloads[2]
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, records, 3), ES_OK, "write payload rollover batch") != 0) {
        es_close(engine);
        return 1;
    }

    es_close(engine);

    const char* segment_paths[3] = {
        "./compression_payload_delta_rollover_testdata/stream_1/segment_000001.seg",
        "./compression_payload_delta_rollover_testdata/stream_1/segment_000002.seg",
        "./compression_payload_delta_rollover_testdata/stream_1/segment_000003.seg"
    };

    for(size_t i = 0; i < 3; ++i) {
        FILE* file = fopen(segment_paths[i], "rb");
        if(!file) {
            printf("FAILED: could not open rollover segment %zu\n", i + 1);
            return 1;
        }

        long segment_header_size = 32;
        if(fseek(file, segment_header_size, SEEK_SET) != 0) {
            fclose(file);
            printf("FAILED: could not seek rollover segment %zu header\n", i + 1);
            return 1;
        }

        if(expect_encoded_header(
            file,
            ES_TIMESTAMP_ENCODING_FULL_U64,
            ES_PAYLOAD_ENCODING_RAW,
            sizeof(int32_payload_t),
            sizeof(int32_payload_t),
            "rollover segment first record header"
        ) != 0) {
            fclose(file);
            return 1;
        }

        fclose(file);
    }

    return 0;
}

int main(void) {
    if(test_smooth_int32_payload_uses_delta_i8() != 0) {
        return 1;
    }

    if(test_int32_payload_uses_delta_i16_when_i8_overflows() != 0) {
        return 1;
    }

    if(test_incompatible_payload_size_falls_back_to_raw() != 0) {
        return 1;
    }

    if(test_large_int32_delta_falls_back_to_raw() != 0) {
        return 1;
    }

    if(test_rollover_resets_previous_payload_state() != 0) {
        return 1;
    }

    printf("compression_payload_delta_test passed\n");
    return 0;
}