#include "edgestorage/edgestorage.h"

#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>

typedef struct {
    float ax, ay, az;
    float gx, gy, gz;
} imu_payload_t;

typedef struct {
    double lat;
    double lon;
    float alt;
    float speed;
} gps_payload_t;

typedef struct {
    float voltage;
    float current;
    float percentage;
} battery_payload_t;

typedef struct {
    float board_temp;
    float motor_temp;
} temperature_payload_t;

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

static int expect_status(es_status_t actual, es_status_t expected, const char* label) {
    if(actual != expected) {
        printf("FAILED: %s expected=%d actual=%d\n", label, expected, actual);
        return 1;
    }

    return 0;
}

int main(void) {
    system("rm -rf ./storage_flow_testdata");

    es_field_def_t imu_fields[] = {
        { .field_id = 1, .name = "ax", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 2, .name = "ay", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 3, .name = "az", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 4, .name = "gx", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 5, .name = "gy", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 6, .name = "gz", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
    };

        es_field_def_t gps_fields[] = {
        { .field_id = 1, .name = "lat", .type = ES_TYPE_F64, .comp_hint = ES_COMP_NONE },
        { .field_id = 2, .name = "lon", .type = ES_TYPE_F64, .comp_hint = ES_COMP_NONE },
        { .field_id = 3, .name = "alt", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 4, .name = "speed", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
    };

    es_field_def_t battery_fields[] = {
        { .field_id = 1, .name = "voltage", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 2, .name = "current", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 3, .name = "percentage", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
    };

    es_field_def_t temperature_fields[] = {
        { .field_id = 1, .name = "board_temp", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
        { .field_id = 2, .name = "motor_temp", .type = ES_TYPE_F32, .comp_hint = ES_COMP_NONE },
    };

    es_record_type_def_t record_types[] = {
        {
            .record_type_id = 1,
            .record_name = "imu",
            .field_count = 6,
            .fields = imu_fields,
            .payload_size = sizeof(imu_payload_t)
        },
        {
            .record_type_id = 2,
            .record_name = "gps",
            .field_count = 4,
            .fields = gps_fields,
            .payload_size = sizeof(gps_payload_t)
        },
        {
            .record_type_id = 3,
            .record_name = "battery",
            .field_count = 3,
            .fields = battery_fields,
            .payload_size = sizeof(battery_payload_t)
        },
        {
            .record_type_id = 4,
            .record_name = "temperature",
            .field_count = 2,
            .fields = temperature_fields,
            .payload_size = sizeof(temperature_payload_t)
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "drone_telemetry",
        .schema_version = 1,
        .record_type_count = 4,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./storage_flow_testdata",
        .segment_size_bytes = 128,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 0
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

    imu_payload_t imu = {
        .ax = 0.1f, .ay = 0.2f, .az = 9.8f,
        .gx = 1.0f, .gy = 2.0f, .gz = 3.0f
    };

    es_record_t record = {
        .timestamp_ns = 1000000,
        .record_type_id = 1,
        .flags = 0,
        .payload_size = sizeof(imu_payload_t),
        .payload = &imu
    };

    if(expect_status(es_write_record(engine, stream_id, &record), ES_OK, "write single IMU record") != 0) {
        es_close(engine);
        return 1;
    }

    const char* segment_path = "./storage_flow_testdata/stream_1/segment_000001.seg";

    if(!path_exists(segment_path)) {
        printf("FAILED: segment file does not exist\n");
        es_close(engine);
        return 1;
    }

    long size = path_file_size(segment_path);
    long expected_size = 16 + (long)sizeof(imu_payload_t);

    if(size != expected_size) {
        printf("FAILED: segment size mismatch expected=%ld actual=%ld\n", expected_size, size);
        es_close(engine);
        return 1;
    }

        imu_payload_t batch_payloads[3] = {
        { .ax = 0.3f, .ay = 0.4f, .az = 9.7f, .gx = 4.0f, .gy = 5.0f, .gz = 6.0f },
        { .ax = 0.5f, .ay = 0.6f, .az = 9.6f, .gx = 7.0f, .gy = 8.0f, .gz = 9.0f },
        { .ax = 0.7f, .ay = 0.8f, .az = 9.5f, .gx = 10.0f, .gy = 11.0f, .gz = 12.0f }
    };

    es_record_t batch_records[3] = {
        {
            .timestamp_ns = 2000000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(imu_payload_t),
            .payload = &batch_payloads[0]
        },
        {
            .timestamp_ns = 3000000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(imu_payload_t),
            .payload = &batch_payloads[1]
        },
        {
            .timestamp_ns = 4000000,
            .record_type_id = 1,
            .flags = 0,
            .payload_size = sizeof(imu_payload_t),
            .payload = &batch_payloads[2]
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, batch_records, 3), ES_OK, "write IMU batch") != 0) {
        es_close(engine);
        return 1;
    }

    long size_after_batch_segment_1 = path_file_size(segment_path);
    long expected_size_after_batch_segment_1 = 3 * expected_size;

    if(size_after_batch_segment_1 != expected_size_after_batch_segment_1) {
        printf(
            "FAILED: batch segment 1 size mismatch expected=%ld actual=%ld\n",
            expected_size_after_batch_segment_1,
            size_after_batch_segment_1
        );
        es_close(engine);
        return 1;
    }

    const char* segment_2_path = "./storage_flow_testdata/stream_1/segment_000002.seg";

    if(!path_exists(segment_2_path)) {
        printf("FAILED: segment_000002.seg does not exist after rollover\n");
        es_close(engine);
        return 1;
    }

    long size_after_batch_segment_2 = path_file_size(segment_2_path);

    if(size_after_batch_segment_2 != expected_size) {
        printf(
            "FAILED: batch segment 2 size mismatch expected=%ld actual=%ld\n",
            expected_size,
            size_after_batch_segment_2
        );
        es_close(engine);
        return 1;
    }

    long expected_size_after_batch =
        size_after_batch_segment_1 + size_after_batch_segment_2;

        gps_payload_t gps = {
        .lat = 41.0082,
        .lon = 28.9784,
        .alt = 120.5f,
        .speed = 15.2f
    };

    battery_payload_t battery = {
        .voltage = 11.1f,
        .current = 2.4f,
        .percentage = 87.0f
    };

    temperature_payload_t temperature = {
        .board_temp = 42.5f,
        .motor_temp = 55.0f
    };

    es_record_t mixed_records[3] = {
        {
            .timestamp_ns = 5000000,
            .record_type_id = 2,
            .flags = 0,
            .payload_size = sizeof(gps_payload_t),
            .payload = &gps
        },
        {
            .timestamp_ns = 6000000,
            .record_type_id = 3,
            .flags = 0,
            .payload_size = sizeof(battery_payload_t),
            .payload = &battery
        },
        {
            .timestamp_ns = 7000000,
            .record_type_id = 4,
            .flags = 0,
            .payload_size = sizeof(temperature_payload_t),
            .payload = &temperature
        }
    };

    if(expect_status(es_write_batch(engine, stream_id, mixed_records, 3), ES_OK, "write mixed telemetry batch") != 0) {
        es_close(engine);
        return 1;
    }

        const char* segment_3_path = "./storage_flow_testdata/stream_1/segment_000003.seg";

    if(!path_exists(segment_3_path)) {
        printf("FAILED: segment_000003.seg does not exist after mixed telemetry rollover\n");
        es_close(engine);
        return 1;
    }

    long expected_segment_1_size = 3 * expected_size;
    long expected_segment_2_size =
        expected_size +
        (16 + (long)sizeof(gps_payload_t)) +
        (16 + (long)sizeof(battery_payload_t));
    long expected_segment_3_size =
        16 + (long)sizeof(temperature_payload_t);

    long actual_segment_1_size = path_file_size(segment_path);
    long actual_segment_2_size = path_file_size(segment_2_path);
    long actual_segment_3_size = path_file_size(segment_3_path);

    if(actual_segment_1_size != expected_segment_1_size) {
        printf(
            "FAILED: segment 1 final size mismatch expected=%ld actual=%ld\n",
            expected_segment_1_size,
            actual_segment_1_size
        );
        es_close(engine);
        return 1;
    }

    if(actual_segment_2_size != expected_segment_2_size) {
        printf(
            "FAILED: segment 2 final size mismatch expected=%ld actual=%ld\n",
            expected_segment_2_size,
            actual_segment_2_size
        );
        es_close(engine);
        return 1;
    }

    if(actual_segment_3_size != expected_segment_3_size) {
        printf(
            "FAILED: segment 3 final size mismatch expected=%ld actual=%ld\n",
            expected_segment_3_size,
            actual_segment_3_size
        );
        es_close(engine);
        return 1;
    }

    long total_expected_size =
        expected_segment_1_size +
        expected_segment_2_size +
        expected_segment_3_size;

    long total_actual_size =
        actual_segment_1_size +
        actual_segment_2_size +
        actual_segment_3_size;

    if(total_actual_size != total_expected_size) {
        printf(
            "FAILED: total telemetry size mismatch expected=%ld actual=%ld\n",
            total_expected_size,
            total_actual_size
        );
        es_close(engine);
        return 1;
    }  

    es_close(engine);

    printf("storage_flow_test passed\n");
    return 0;
}