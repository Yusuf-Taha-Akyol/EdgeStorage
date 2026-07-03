#include "edgestorage/edgestorage.h"
#include "segment_format.h"

#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>

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
        .segment_size_bytes = 160,
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
    long segment_header_size = 32;
    long imu_record_size = 16 + (long)sizeof(imu_payload_t);
    long expected_size = segment_header_size + imu_record_size;


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
    long expected_size_after_batch_segment_1 =
        segment_header_size + (3 * imu_record_size);

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

    long gps_record_size = 16 + (long)sizeof(gps_payload_t);
    long battery_record_size = 16 + (long)sizeof(battery_payload_t);
    long temperature_record_size = 16 + (long)sizeof(temperature_payload_t);

    long expected_segment_1_size =
        segment_header_size + (3 * imu_record_size);

    long expected_segment_2_size =
        segment_header_size +
        imu_record_size +
        gps_record_size +
        battery_record_size;

    long expected_segment_3_size =
        segment_header_size + temperature_record_size;

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

    // --- Camera stream test section ---
    printf("Starting camera stream integration test...\n");
    system("rm -rf ./storage_cam_testdata");
    es_config_t config_cam = {
        .storage_path = "./storage_cam_testdata",
        .segment_size_bytes = 1024 * 1024,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 0
    };
    es_engine_t* engine_cam = es_open(&config_cam);
    if (!engine_cam) {
        printf("FAILED: es_open for camera engine returned NULL\n");
        return 1;
    }
    uint32_t cam_stream_id = 0;
    if (expect_status(es_register_stream_schema(engine_cam, &schema, &cam_stream_id), ES_OK, "register stream for camera") != 0) {
        es_close(engine_cam);
        return 1;
    }

    if (expect_status(es_write_record(engine_cam, cam_stream_id, &record), ES_OK, "write sensor record") != 0) {
        es_close(engine_cam);
        return 1;
    }

    const char* dummy_jpeg = "JPEG_DATA_DUMMY_123456789";
    uint32_t dummy_jpeg_size = (uint32_t)strlen(dummy_jpeg);
    if (expect_status(es_write_camera_frame(engine_cam, cam_stream_id, 2000000, 1, dummy_jpeg_size, dummy_jpeg), ES_OK, "write camera frame") != 0) {
        es_close(engine_cam);
        return 1;
    }

    es_close(engine_cam);

    char cam_seg_path[512];
    snprintf(cam_seg_path, sizeof(cam_seg_path), "./storage_cam_testdata/stream_%u/segment_000001.seg", cam_stream_id);
    FILE* seg_file = fopen(cam_seg_path, "rb");
    if (!seg_file) {
        printf("FAILED: Segment file not created: %s\n", cam_seg_path);
        return 1;
    }
    es_segment_header_t seg_header;
    if (fread(&seg_header, sizeof(seg_header), 1, seg_file) != 1) {
        printf("FAILED: Cannot read segment header\n");
        fclose(seg_file);
        return 1;
    }
    
    long expected_offset = (long)sizeof(es_segment_header_t) + 8 + 2 + 2 + 4 + (long)sizeof(imu_payload_t);
    if ((long)seg_header.camera_stream_offset != expected_offset) {
        printf("FAILED: Camera stream offset mismatch expected=%ld actual=%u\n", expected_offset, seg_header.camera_stream_offset);
        fclose(seg_file);
        return 1;
    }

    if (fseek(seg_file, seg_header.camera_stream_offset, SEEK_SET) != 0) {
        printf("FAILED: Cannot seek to camera stream offset\n");
        fclose(seg_file);
        return 1;
    }
    uint64_t rd_timestamp = 0;
    uint32_t rd_index = 0;
    uint32_t rd_jpeg_size = 0;
    char rd_jpeg[64];
    if (fread(&rd_timestamp, sizeof(rd_timestamp), 1, seg_file) != 1 ||
        fread(&rd_index, sizeof(rd_index), 1, seg_file) != 1 ||
        fread(&rd_jpeg_size, sizeof(rd_jpeg_size), 1, seg_file) != 1) {
        printf("FAILED: Cannot read camera frame header from segment file\n");
        fclose(seg_file);
        return 1;
    }
    if (rd_timestamp != 2000000 || rd_index != 1 || rd_jpeg_size != dummy_jpeg_size) {
        printf("FAILED: Camera frame header values mismatch ts=%llu idx=%u size=%u\n", rd_timestamp, rd_index, rd_jpeg_size);
        fclose(seg_file);
        return 1;
    }
    if (fread(rd_jpeg, 1, rd_jpeg_size, seg_file) != rd_jpeg_size) {
        printf("FAILED: Cannot read camera frame JPEG bytes\n");
        fclose(seg_file);
        return 1;
    }
    rd_jpeg[rd_jpeg_size] = '\0';
    if (strcmp(rd_jpeg, dummy_jpeg) != 0) {
        printf("FAILED: Camera frame JPEG content mismatch expected=%s actual=%s\n", dummy_jpeg, rd_jpeg);
        fclose(seg_file);
        return 1;
    }
    fclose(seg_file);

    engine_cam = es_open(&config_cam);
    uint32_t temp_id;
    es_register_stream_schema(engine_cam, &schema, &temp_id);

    es_query_t query = {
        .stream_id = cam_stream_id,
        .start_ts_ns = 0,
        .end_ts_ns = 3000000,
        .record_type_id = 0,
        .limit = 10
    };
    es_result_t query_res = {0};
    if (expect_status(es_query_range(engine_cam, &query, &query_res), ES_OK, "query range with camera offset") != 0) {
        es_close(engine_cam);
        return 1;
    }
    if (query_res.count != 1) {
        printf("FAILED: Query returned wrong record count expected=1 actual=%zu\n", query_res.count);
        es_result_free(&query_res);
        es_close(engine_cam);
        return 1;
    }
    es_result_free(&query_res);
    es_close(engine_cam);
    printf("Camera stream integration test passed!\n");
 
    printf("storage_flow_test passed\n");
    return 0;
}