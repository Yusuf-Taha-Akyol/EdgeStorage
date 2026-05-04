#include "edgestorage/edgestorage.h"
#include "segment_format.h"

#include <stdio.h>
#include <stdlib.h>

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

static int expect_u32(uint32_t actual, uint32_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%u actual=%u\n", label, expected, actual);
        return 1;
    }

    return 0;
}

static int read_segment_header(const char* path, es_segment_header_t* out_header) {
    FILE* file = fopen(path, "rb");
    if(!file) {
        printf("FAILED: could not open segment file: %s\n", path);
        return 1;
    }

    es_status_t status = es_segment_format_read_header(file, out_header);
    fclose(file);

    if(status != ES_OK) {
        printf("FAILED: could not read valid segment header: %s status=%d\n", path, status);
        return 1;
    }

    return 0;
}

static int create_schema(es_stream_schema_t* out_schema) {
    static es_field_def_t fields[] = {
        {
            .field_id = 1,
            .name = "value",
            .type = ES_TYPE_U32,
            .comp_hint = ES_COMP_NONE
        }
    };

    static es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "counter",
            .field_count = 1,
            .fields = fields,
            .payload_size = sizeof(counter_payload_t)
        }
    };

    out_schema->stream_name = "header_stream";
    out_schema->schema_version = 1;
    out_schema->record_type_count = 1;
    out_schema->record_types = record_types;

    return 0;
}

static int test_uncompressed_header(void) {
    system("rm -rf ./segment_header_uncompressed_testdata");

    es_stream_schema_t schema;
    create_schema(&schema);

    es_config_t config = {
        .storage_path = "./segment_header_uncompressed_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 0
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: uncompressed es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(
        es_register_stream_schema(engine, &schema, &stream_id),
        ES_OK,
        "register uncompressed stream"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    counter_payload_t payload = { .value = 10 };
    es_record_t record = {
        .timestamp_ns = 1000,
        .record_type_id = 1,
        .flags = 0,
        .payload_size = sizeof(counter_payload_t),
        .payload = &payload
    };

    if(expect_status(
        es_write_record(engine, stream_id, &record),
        ES_OK,
        "write uncompressed record"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    es_close(engine);

    es_segment_header_t header;
    if(read_segment_header(
        "./segment_header_uncompressed_testdata/stream_1/segment_000001.seg",
        &header
    ) != 0) {
        return 1;
    }

    if(expect_u32(header.stream_id, 1, "uncompressed header stream_id") != 0) {
        return 1;
    }

    if(expect_u32(header.segment_index, 1, "uncompressed header segment_index") != 0) {
        return 1;
    }

    if(expect_u32(
        header.compression_mode,
        ES_SEGMENT_COMPRESSION_NONE,
        "uncompressed header compression_mode"
    ) != 0) {
        return 1;
    }

    if(expect_u32(
        header.record_format_version,
        ES_RECORD_FORMAT_VERSION,
        "uncompressed header record_format_version"
    ) != 0) {
        return 1;
    }

    return 0;
}

static int test_compressed_header(void) {
    system("rm -rf ./segment_header_compressed_testdata");

    es_stream_schema_t schema;
    create_schema(&schema);

    es_config_t config = {
        .storage_path = "./segment_header_compressed_testdata",
        .segment_size_bytes = 4096,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: compressed es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(
        es_register_stream_schema(engine, &schema, &stream_id),
        ES_OK,
        "register compressed stream"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    counter_payload_t payload = { .value = 20 };
    es_record_t record = {
        .timestamp_ns = 1000000000ULL,
        .record_type_id = 1,
        .flags = 0,
        .payload_size = sizeof(counter_payload_t),
        .payload = &payload
    };

    if(expect_status(
        es_write_record(engine, stream_id, &record),
        ES_OK,
        "write compressed record"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    es_close(engine);

    es_segment_header_t header;
    if(read_segment_header(
        "./segment_header_compressed_testdata/stream_1/segment_000001.seg",
        &header
    ) != 0) {
        return 1;
    }

    if(expect_u32(header.stream_id, 1, "compressed header stream_id") != 0) {
        return 1;
    }

    if(expect_u32(header.segment_index, 1, "compressed header segment_index") != 0) {
        return 1;
    }

    if(expect_u32(
        header.compression_mode,
        ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA,
        "compressed header compression_mode"
    ) != 0) {
        return 1;
    }

    return 0;
}

static int test_rollover_headers(void) {
    system("rm -rf ./segment_header_rollover_testdata");

    es_stream_schema_t schema;
    create_schema(&schema);

    es_config_t config = {
        .storage_path = "./segment_header_rollover_testdata",
        .segment_size_bytes = 71,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: rollover es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    if(expect_status(
        es_register_stream_schema(engine, &schema, &stream_id),
        ES_OK,
        "register rollover stream"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    counter_payload_t payloads[3] = {
        { .value = 100 },
        { .value = 200 },
        { .value = 300 }
    };

    es_record_t records[3] = {
        {
            .timestamp_ns = 2000000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[0]
        },
        {
            .timestamp_ns = 2001000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[1]
        },
        {
            .timestamp_ns = 2002000000ULL,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(counter_payload_t),
            .payload = &payloads[2]
        }
    };

    if(expect_status(
        es_write_batch(engine, stream_id, records, 3),
        ES_OK,
        "write rollover records"
    ) != 0) {
        es_close(engine);
        return 1;
    }

    es_close(engine);

    es_segment_header_t header_1;
    if(read_segment_header(
        "./segment_header_rollover_testdata/stream_1/segment_000001.seg",
        &header_1
    ) != 0) {
        return 1;
    }

    es_segment_header_t header_2;
    if(read_segment_header(
        "./segment_header_rollover_testdata/stream_1/segment_000002.seg",
        &header_2
    ) != 0) {
        return 1;
    }

    if(expect_u32(header_1.stream_id, 1, "rollover header 1 stream_id") != 0) {
        return 1;
    }

    if(expect_u32(header_1.segment_index, 1, "rollover header 1 segment_index") != 0) {
        return 1;
    }

    if(expect_u32(
        header_1.compression_mode,
        ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA,
        "rollover header 1 compression_mode"
    ) != 0) {
        return 1;
    }

    if(expect_u32(header_2.stream_id, 1, "rollover header 2 stream_id") != 0) {
        return 1;
    }

    if(expect_u32(header_2.segment_index, 2, "rollover header 2 segment_index") != 0) {
        return 1;
    }

    if(expect_u32(
        header_2.compression_mode,
        ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA,
        "rollover header 2 compression_mode"
    ) != 0) {
        return 1;
    }

    return 0;
}

int main(void) {
    if(test_uncompressed_header() != 0) {
        return 1;
    }

    if(test_compressed_header() != 0) {
        return 1;
    }

    if(test_rollover_headers() != 0) {
        return 1;
    }

    printf("segment_header_test passed\n");
    return 0;
}