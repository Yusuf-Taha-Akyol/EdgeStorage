#include "edgestorage/edgestorage.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>

typedef struct {
    uint32_t value;
} benchmark_payload_t;

static double elapsed_seconds(struct timespec start, struct timespec end) {
    double seconds = (double)(end.tv_sec - start.tv_sec);
    double nanoseconds = (double)(end.tv_nsec - start.tv_nsec) / 1000000000.0;
    return seconds + nanoseconds;
}

static long file_size_or_zero(const char* path) {
    struct stat st;
    if(stat(path, &st) != 0) {
        return 0;
    }

    return (long)st.st_size;
}

static long segment_directory_size(const char* directory_path) {
    DIR* dir = opendir(directory_path);
    if(!dir) {
        return 0;
    }

    long total_size = 0;
    struct dirent* entry = NULL;

    while((entry = readdir(dir)) != NULL) {
        if(strstr(entry->d_name, ".seg") == NULL) {
            continue;
        }

        char file_path[1024];
        snprintf(
            file_path,
            sizeof(file_path),
            "%s/%s",
            directory_path,
            entry->d_name
        );

        total_size += file_size_or_zero(file_path);
    }

    closedir(dir);
    return total_size;
}

static void fill_payload(unsigned char* payload, size_t payload_size, size_t record_index) {
    for(size_t i = 0; i < payload_size; i++) {
        payload[i] = (unsigned char)((record_index + i) & 0xFF);
    }
}

int main(int argc, char** argv) {
    size_t record_count = 10000;
    size_t batch_size = 100;
    size_t payload_size_bytes = sizeof(benchmark_payload_t);

    if(argc >= 2) {
        record_count = (size_t)strtoull(argv[1], NULL, 10);
    }

    if(argc >= 3) {
        batch_size = (size_t)strtoull(argv[2], NULL, 10);
    }

    if(argc >= 4) {
        payload_size_bytes = (size_t)strtoull(argv[3], NULL, 10);
    }

    if(record_count == 0) {
        printf("FAILED: record_count must be greater than 0\n");
        return 1;
    }

    if(batch_size == 0) {
        printf("FAILED: batch_size must be greater than 0\n");
        return 1;
    }

    if(payload_size_bytes == 0) {
        printf("FAILED: payload_size_bytes must be greater than 0\n");
        return 1;
    }

    system("rm -rf ./benchmark_testdata");

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
            .payload_size = (uint32_t)payload_size_bytes
        }
    };

    es_stream_schema_t schema = {
        .stream_name = "benchmark_stream",
        .schema_version = 1,
        .record_type_count = 1,
        .record_types = record_types
    };

    es_config_t config = {
        .storage_path = "./benchmark_testdata",
        .segment_size_bytes = 1024 * 1024,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 0
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        printf("FAILED: es_open returned NULL\n");
        return 1;
    }

    uint32_t stream_id = 0;
    es_status_t status = es_register_stream_schema(engine, &schema, &stream_id);
    if(status != ES_OK) {
        printf("FAILED: es_register_stream_schema status=%d\n", status);
        es_close(engine);
        return 1;
    }


    unsigned char* single_payload = (unsigned char*)malloc(payload_size_bytes);
    if(!single_payload) {
        printf("FAILED: single payload allocation failed\n");
        es_close(engine);
        return 1;
    }

    struct timespec single_start;
    struct timespec single_end;

    clock_gettime(CLOCK_MONOTONIC, &single_start);

    for(size_t i = 0; i < record_count; i++) {
        fill_payload(single_payload, payload_size_bytes, i);

        es_record_t record = {
            .timestamp_ns = 1000000000ULL + ((uint64_t)i * 1000000ULL),
            .record_type_id = 1,
            .flags = 0,
            .payload_size = (uint32_t)payload_size_bytes,
            .payload = single_payload
        };

        status = es_write_record(engine, stream_id, &record);
        if(status != ES_OK) {
            printf("FAILED: es_write_record index=%zu status=%d\n", i, status);
            free(single_payload);
            es_close(engine);
            return 1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &single_end);

    free(single_payload);

    clock_gettime(CLOCK_MONOTONIC, &single_end);
    double single_write_seconds = elapsed_seconds(single_start, single_end);
    double single_write_records_per_sec = (double)record_count / single_write_seconds;

    es_close(engine);

    system("rm -rf ./benchmark_batch_testdata");

    es_config_t batch_config = {
        .storage_path = "./benchmark_batch_testdata",
        .segment_size_bytes = 1024 * 1024,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 0
    };

    es_engine_t* batch_engine = es_open(&batch_config);
    if(!batch_engine) {
        printf("FAILED: batch es_open returned NULL\n");
        return 1;
    }

    uint32_t batch_stream_id = 0;
    status = es_register_stream_schema(batch_engine, &schema, &batch_stream_id);
    if(status != ES_OK) {
        printf("FAILED: batch es_register_stream_schema status=%d\n", status);
        es_close(batch_engine);
        return 1;
    }

    unsigned char* batch_payloads =
        (unsigned char*)malloc(payload_size_bytes * batch_size);
    es_record_t* batch_records = 
        (es_record_t*)malloc(sizeof(es_record_t) * batch_size);

    if(!batch_payloads || !batch_records) {
        printf("FAILED: batch allocation failed\n");
        free(batch_payloads);
        free(batch_records);
        es_close(batch_engine);
        return 1;
    }

    struct timespec batch_start;
    struct timespec batch_end;

    clock_gettime(CLOCK_MONOTONIC, &batch_start);

    size_t written = 0;
    while(written < record_count) {
        size_t current_batch_size = batch_size;
        if(record_count - written < current_batch_size) {
            current_batch_size = record_count - written;
        }

        for(size_t i = 0; i < current_batch_size; ++i) {
            size_t record_index = written + i;

            unsigned char* payload = batch_payloads + (i * payload_size_bytes);
            fill_payload(payload, payload_size_bytes, record_index);

            batch_records[i].timestamp_ns = 1000000000ULL + ((uint64_t)record_index * 1000000ULL);
            batch_records[i].record_type_id = 1;
            batch_records[i].flags = 0;
            batch_records[i].payload_size = (uint32_t)payload_size_bytes;
            batch_records[i].payload = payload;
        }

        status = es_write_batch(
            batch_engine,
            batch_stream_id,
            batch_records,
            current_batch_size
        );

        if(status != ES_OK) {
            printf("FAILED: es_write_batch written=%zu status=%d\n", written, status);
            free(batch_payloads);
            free(batch_records);
            es_close(batch_engine);
            return 1;
        }

        written += current_batch_size;
    }

    clock_gettime(CLOCK_MONOTONIC, &batch_end);

    double batch_write_seconds = elapsed_seconds(batch_start, batch_end);
    double batch_write_records_per_sec = (double)record_count / batch_write_seconds;

    free(batch_payloads);
    free(batch_records);

    es_query_t query = {
        .stream_id = batch_stream_id,
        .start_ts_ns = 1000000000ULL,
        .end_ts_ns = 1000000000ULL + ((uint64_t)(record_count - 1) * 1000000000ULL),
        .record_type_id = 1,
        .limit = 0
    };

    es_result_t query_result = {0};

    struct timespec query_start;
    struct timespec query_end;
    
    clock_gettime(CLOCK_MONOTONIC, &query_start);

    status = es_query_range(batch_engine, &query, &query_result);

    clock_gettime(CLOCK_MONOTONIC, &query_end);

    if(status != ES_OK) {
        printf("FAILED: es_query_range status=%d\n", status);
        es_close(batch_engine);
        return 1;
    }

    double query_latency_seconds = elapsed_seconds(query_start, query_end);
    double query_latency_ms = query_latency_seconds * 1000.0;
    size_t query_result_count = query_result.count;

    if(query_result_count != record_count) {
        printf(
            "FAILED: query_result_count mismatch expected=%zu actual=%zu\n",
            record_count,
            query_result_count
        );
        es_result_free(&query_result);
        es_close(batch_engine);
        return 1;
    }

    es_result_free(&query_result);
    es_close(batch_engine);
    
    long uncompressed_size_bytes = segment_directory_size("./benchmark_batch_testdata/stream_1");

    system("rm -rf ./benchmark_compressed_testdata");

    es_config_t compressed_config = {
        .storage_path = "./benchmark_compressed_testdata",
        .segment_size_bytes = 1024 * 1024,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* compressed_engine = es_open(&compressed_config);
    if(!compressed_engine) {
        printf("FAILED: compressed es_open returned NULL\n");
        return 1;
    }

    uint32_t compressed_stream_id = 0;
    status = es_register_stream_schema(compressed_engine, &schema, &compressed_stream_id);
    if(status != ES_OK) {
        printf("FAILED: compressed es_register_stream_schema status=%d\n", status);
        es_close(compressed_engine);
        return 1;
    }

    unsigned char* compressed_payloads =
        (unsigned char*)malloc(payload_size_bytes * batch_size);
    es_record_t* compressed_records =
        (es_record_t*)malloc(sizeof(es_record_t) * batch_size);

    if(!compressed_payloads || !compressed_records) {
        printf("FAILED: compressed allocation failed\n");
        free(compressed_payloads);
        free(compressed_records);
        es_close(compressed_engine);
        return 1;
    }  

    struct timespec compressed_start;
    struct timespec compressed_end;

    clock_gettime(CLOCK_MONOTONIC, &compressed_start);
    
    written = 0;
    while(written < record_count) {
        size_t current_batch_size = batch_size;
        if(record_count - written < current_batch_size) {
            current_batch_size = record_count - written;
        }

        for(size_t i = 0; i < current_batch_size; i++) {
            size_t record_index = written + i;

            unsigned char* payload = compressed_payloads + (i * payload_size_bytes);
            fill_payload(payload, payload_size_bytes, record_index);

            compressed_records[i].timestamp_ns =
                1000000000ULL + ((uint64_t)record_index * 1000000ULL);
            compressed_records[i].record_type_id = 1;
            compressed_records[i].flags = 0;
            compressed_records[i].payload_size = (uint32_t)payload_size_bytes;
            compressed_records[i].payload = payload;
        }

        status = es_write_batch(
            compressed_engine,
            compressed_stream_id,
            compressed_records,
            current_batch_size
        );

        if(status != ES_OK) {
            printf("FAILED: compressed es_write_batch written=%zu status=%d\n", written, status);
            free(compressed_payloads);
            free(compressed_records);
            es_close(compressed_engine);
            return 1;
        }

        written += current_batch_size;
    }

    clock_gettime(CLOCK_MONOTONIC, &compressed_end);

    double compressed_write_seconds = elapsed_seconds(compressed_start, compressed_end);
    double compressed_write_records_per_sec = (double)record_count / compressed_write_seconds;

    double payload_bytes_per_record = (double)payload_size_bytes;
    double batch_payload_mb_per_sec = ((double)record_count * payload_bytes_per_record) / (1024.0 * 1024.0) / batch_write_seconds;
    double compressed_payload_mb_per_sec = ((double)record_count * payload_bytes_per_record) / (1024.0 * 1024.0) /compressed_write_seconds;

    free(compressed_payloads);
    free(compressed_records);
    es_close(compressed_engine);

    long compressed_size_bytes = segment_directory_size("./benchmark_compressed_testdata/stream_1");
    
    double compression_ratio = 0.0;
    if(compressed_size_bytes > 0) {
        compression_ratio = (double)uncompressed_size_bytes / (double)compressed_size_bytes;
    }

    printf("EdgeStorage benchmark\n");
    printf("records=%zu\n", record_count);
    printf("batch_size=%zu\n", batch_size);
    printf("payload_size_bytes=%zu\n", payload_size_bytes);
    printf("single_write_seconds=%.6f\n", single_write_seconds);
    printf("single_write_records_per_sec=%.2f\n", single_write_records_per_sec);
    printf("batch_write_seconds=%.6f\n", batch_write_seconds);
    printf("batch_write_records_per_sec=%.2f\n", batch_write_records_per_sec);
    printf("compressed_write_seconds=%.6f\n", compressed_write_seconds);
    printf("compressed_write_records_per_sec=%.2f\n", compressed_write_records_per_sec);
    printf("payload_bytes_per_record=%.0f\n", payload_bytes_per_record);
    printf("batch_payload_mb_per_sec=%.2f\n", batch_payload_mb_per_sec);
    printf("compressed_payload_mb_per_sec=%.2f\n", compressed_payload_mb_per_sec);
    printf("uncompressed_size_bytes=%ld\n", uncompressed_size_bytes);
    printf("compressed_size_bytes=%ld\n", compressed_size_bytes);
    printf("compression_ratio=%.3f\n", compression_ratio);
    printf("query_latency_ms=%.3f\n", query_latency_ms);
    printf("query_result_count=%zu\n", query_result_count);
    
    return 0;
}