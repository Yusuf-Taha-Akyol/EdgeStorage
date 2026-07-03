#include "edgestorage/edgestorage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    // Disable stdout and stdin buffering to ensure immediate communication over pipes
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stdin, NULL, _IOLBF, 0);

    es_engine_t* engine = NULL;
    char cmd[64];

    while (scanf("%63s", cmd) != EOF) {
        if (strcmp(cmd, "OPEN") == 0) {
            char path[512];
            size_t seg_size;
            size_t buf_size;
            int comp;
            if (scanf("%511s %zu %zu %d", path, &seg_size, &buf_size, &comp) != 4) {
                printf("STATUS ERROR Invalid OPEN arguments\n");
                continue;
            }
            if (engine) {
                es_close(engine);
                engine = NULL;
            }
            es_config_t config = {
                .storage_path = path,
                .segment_size_bytes = seg_size,
                .write_buffer_size_bytes = buf_size,
                .compression_enabled = comp
            };
            engine = es_open(&config);
            if (engine) {
                printf("STATUS OK\n");
            } else {
                printf("STATUS ERROR Failed to open engine\n");
            }
        } else if (strcmp(cmd, "CLOSE") == 0) {
            if (engine) {
                es_close(engine);
                engine = NULL;
            }
            printf("STATUS OK\n");
        } else if (strcmp(cmd, "REGISTER_STREAM") == 0) {
            char stream_name[256];
            uint32_t schema_version;
            uint16_t record_type_count;
            if (scanf("%255s %u %hu", stream_name, &schema_version, &record_type_count) != 3) {
                printf("STATUS ERROR Invalid REGISTER_STREAM arguments\n");
                continue;
            }

            if (!engine) {
                printf("STATUS ERROR Engine not open\n");
                continue;
            }

            es_record_type_def_t* record_types = malloc(record_type_count * sizeof(es_record_type_def_t));
            if (!record_types) {
                printf("STATUS ERROR OOM\n");
                continue;
            }
            memset(record_types, 0, record_type_count * sizeof(es_record_type_def_t));
            int success = 1;

            for (uint16_t i = 0; i < record_type_count; ++i) {
                char rec_cmd[64];
                uint16_t record_type_id;
                char record_name[256];
                uint16_t field_count;
                uint32_t payload_size;
                if (scanf("%63s %hu %255s %hu %u", rec_cmd, &record_type_id, record_name, &field_count, &payload_size) != 5 || strcmp(rec_cmd, "RECORD_TYPE") != 0) {
                    success = 0;
                    break;
                }

                es_field_def_t* fields = malloc(field_count * sizeof(es_field_def_t));
                if (!fields) {
                    success = 0;
                    break;
                }
                memset(fields, 0, field_count * sizeof(es_field_def_t));

                for (uint16_t j = 0; j < field_count; ++j) {
                    char fld_cmd[64];
                    uint16_t field_id;
                    char field_name[256];
                    int field_type;
                    int comp_hint;
                    if (scanf("%63s %hu %255s %d %d", fld_cmd, &field_id, field_name, &field_type, &comp_hint) != 5 || strcmp(fld_cmd, "FIELD") != 0) {
                        success = 0;
                        break;
                    }
                    fields[j].field_id = field_id;
                    fields[j].name = strdup(field_name);
                    fields[j].type = (es_value_type_t)field_type;
                    fields[j].comp_hint = (es_comp_hint_t)comp_hint;
                }

                if (!success) {
                    for (uint16_t j = 0; j < field_count; ++j) {
                        if (fields[j].name) free((void*)fields[j].name);
                    }
                    free(fields);
                    break;
                }

                record_types[i].record_type_id = record_type_id;
                record_types[i].record_name = strdup(record_name);
                record_types[i].field_count = field_count;
                record_types[i].fields = fields;
                record_types[i].payload_size = payload_size;
            }

            if (!success) {
                printf("STATUS ERROR Failed to parse schema types\n");
                for (uint16_t i = 0; i < record_type_count; ++i) {
                    if (record_types[i].fields) {
                        for (uint16_t j = 0; j < record_types[i].field_count; ++j) {
                            if (record_types[i].fields[j].name) free((void*)record_types[i].fields[j].name);
                        }
                        free((void*)record_types[i].fields);
                    }
                    if (record_types[i].record_name) free((void*)record_types[i].record_name);
                }
                free(record_types);
                continue;
            }

            es_stream_schema_t schema = {
                .stream_name = stream_name,
                .schema_version = schema_version,
                .record_type_count = record_type_count,
                .record_types = record_types
            };

            uint32_t stream_id = 0;
            es_status_t status = es_register_stream_schema(engine, &schema, &stream_id);
            if (status == ES_OK) {
                printf("STATUS OK %u\n", stream_id);
            } else {
                printf("STATUS ERROR %d\n", status);
            }

            // Cleanup schema memory
            for (uint16_t i = 0; i < record_type_count; ++i) {
                for (uint16_t j = 0; j < record_types[i].field_count; ++j) {
                    free((void*)record_types[i].fields[j].name);
                }
                free((void*)record_types[i].fields);
                free((void*)record_types[i].record_name);
            }
            free(record_types);

        } else if (strcmp(cmd, "WRITE_RECORD") == 0) {
            uint32_t stream_id;
            uint64_t timestamp_ns;
            uint16_t record_type_id;
            uint16_t flags;
            uint32_t payload_size;
            if (scanf("%u %llu %hu %hu %u", &stream_id, &timestamp_ns, &record_type_id, &flags, &payload_size) != 5) {
                printf("STATUS ERROR Invalid WRITE_RECORD arguments\n");
                continue;
            }

            if (!engine) {
                printf("STATUS ERROR Engine not open\n");
                continue;
            }

            uint8_t* payload = NULL;
            char* payload_hex = NULL;
            int write_success = 1;

            if (payload_size > 0) {
                payload_hex = malloc(payload_size * 2 + 1);
                if (!payload_hex) {
                    printf("STATUS ERROR OOM\n");
                    continue;
                }
                if (scanf("%s", payload_hex) != 1) {
                    printf("STATUS ERROR Failed to read payload hex\n");
                    free(payload_hex);
                    continue;
                }

                payload = malloc(payload_size);
                if (!payload) {
                    printf("STATUS ERROR OOM\n");
                    free(payload_hex);
                    continue;
                }

                for (uint32_t i = 0; i < payload_size; ++i) {
                    unsigned int byte;
                    if (sscanf(&payload_hex[i * 2], "%2x", &byte) != 1) {
                        write_success = 0;
                        break;
                    }
                    payload[i] = (uint8_t)byte;
                }
            }

            if (!write_success) {
                printf("STATUS ERROR Failed to parse payload hex\n");
                if (payload_hex) free(payload_hex);
                if (payload) free(payload);
                continue;
            }

            es_record_t record = {
                .timestamp_ns = timestamp_ns,
                .record_type_id = record_type_id,
                .flags = flags,
                .payload_size = payload_size,
                .payload = payload
            };

            es_status_t status = es_write_record(engine, stream_id, &record);
            if (status == ES_OK) {
                printf("STATUS OK\n");
            } else {
                printf("STATUS ERROR %d\n", status);
            }

            if (payload_hex) free(payload_hex);
            if (payload) free(payload);

        } else if (strcmp(cmd, "QUERY_RANGE") == 0) {
            uint32_t stream_id;
            uint64_t start_ts_ns;
            uint64_t end_ts_ns;
            uint16_t record_type_id;
            size_t limit;
            if (scanf("%u %llu %llu %hu %zu", &stream_id, &start_ts_ns, &end_ts_ns, &record_type_id, &limit) != 5) {
                printf("STATUS ERROR Invalid QUERY_RANGE arguments\n");
                continue;
            }

            if (!engine) {
                printf("STATUS ERROR Engine not open\n");
                continue;
            }

            es_query_t query = {
                .stream_id = stream_id,
                .start_ts_ns = start_ts_ns,
                .end_ts_ns = end_ts_ns,
                .record_type_id = record_type_id,
                .limit = limit
            };

            es_result_t result = {0};
            es_status_t status = es_query_range(engine, &query, &result);
            if (status == ES_OK) {
                printf("STATUS OK %zu\n", result.count);
                for (size_t i = 0; i < result.count; ++i) {
                    es_record_t* rec = &result.records[i];
                    printf("RECORD %llu %hu %hu %u ", rec->timestamp_ns, rec->record_type_id, rec->flags, rec->payload_size);
                    if (rec->payload_size > 0 && rec->payload) {
                        for (uint32_t j = 0; j < rec->payload_size; ++j) {
                            printf("%02x", ((uint8_t*)rec->payload)[j]);
                        }
                    } else {
                        printf("00"); // dummy single byte or empty
                    }
                    printf("\n");
                }
                es_result_free(&result);
            } else {
                printf("STATUS ERROR %d\n", status);
            }
        } else {
            printf("STATUS ERROR Unknown command\n");
        }
    }

    if (engine) {
        es_close(engine);
    }
    return 0;
}
