#include "storage_writer.h"
#include "runtime.h"
#include "segment_format.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

static es_status_t es_storage_writer_ensure_capacity(es_engine_t* engine) {
    if(!engine) {
        return ES_ERR_INVALID_ARG;
    }

    if(engine->stream_storage_count < engine->stream_storage_capacity) {
        return ES_OK;
    }

    size_t new_capacity = engine->stream_storage_capacity == 0
        ? 4
        : engine->stream_storage_capacity * 2;

    es_stream_storage_state_t* new_states = (es_stream_storage_state_t*)realloc(
        engine->stream_storage_states,
        new_capacity * sizeof(es_stream_storage_state_t)
    );

    if(!new_states) {
        return ES_ERR_OOM;
    }

    engine->stream_storage_states = new_states;
    engine->stream_storage_capacity = new_capacity;

    return ES_OK;
}

static es_status_t es_storage_writer_build_stream_dir_path(
    const es_engine_t* engine,
    uint32_t stream_id,
    char* out_path,
    size_t out_path_size
) {
    if(!engine || !out_path || out_path_size == 0 || stream_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    int written = snprintf(
        out_path,
        out_path_size,
        "%s/stream_%u",
        engine->config.storage_path,
        stream_id
    );

    if(written < 0 || (size_t)written >= out_path_size) {
        return ES_ERR_INTERNAL;
    }

    return ES_OK;
}

static es_status_t es_storage_writer_build_segment_path(
    const char* stream_dir_path,
    uint32_t segment_index,
    char* out_path,
    size_t out_path_size
) {
    if(!stream_dir_path || !out_path || out_path_size == 0 || segment_index == 0) {
        return ES_ERR_INVALID_ARG;
    }

    int written = snprintf(
        out_path,
        out_path_size,
        "%s/segment_%06u.seg",
        stream_dir_path,
        segment_index
    );

    if(written < 0 || (size_t)written >= out_path_size) {
        return ES_ERR_INTERNAL;
    }

    return ES_OK;
}

static es_status_t es_storage_writer_ensure_directory(const char* path) {
    if(!path) {
        return ES_ERR_INVALID_ARG;
    }

    if(mkdir(path, 0755) == 0) {
        return ES_OK;
    }

    if(errno == EEXIST) {
        return ES_OK;
    }

    return ES_ERR_IO;
}

static es_status_t es_storage_writer_open_active_segment(
    const es_engine_t* engine,
    es_stream_storage_state_t* state
) {
    if(!engine || !state) {
        return ES_ERR_INVALID_ARG;
    }

    if(state->active_segment_file) {
        return ES_OK;
    }

    state->active_segment_file = fopen(state->active_segment_path, "ab");
    if(!state->active_segment_file) {
        return ES_ERR_IO;
    }

    // Set up active camera path
    snprintf(state->active_camera_path, sizeof(state->active_camera_path), "%s.cam", state->active_segment_path);
    state->camera_stream_size_bytes = 0;
    state->active_camera_file = fopen(state->active_camera_path, state->active_segment_size_bytes == 0 ? "wb" : "ab");
    if(!state->active_camera_file) {
        fclose(state->active_segment_file);
        state->active_segment_file = NULL;
        return ES_ERR_IO;
    }

    if(state->active_segment_size_bytes == 0) {
        es_segment_header_t header = {
            .magic = ES_SEGMENT_MAGIC,
            .version = ES_SEGMENT_FORMAT_VERSION,
            .header_size = sizeof(es_segment_header_t),
            .stream_id = state->stream_id,
            .segment_index = state->active_segment_index,
            .compression_mode = es_segment_format_compression_from_config(
                engine->config.compression_enabled
            ),
            .record_format_version = ES_RECORD_FORMAT_VERSION,
            .flags = 0,
            .camera_stream_offset = 0
        };

        es_status_t status = es_segment_format_write_header(
            state->active_segment_file,
            &header
        );

        if(status != ES_OK) {
            fclose(state->active_camera_file);
            state->active_camera_file = NULL;
            remove(state->active_camera_path);
            return status;
        }

        state->active_segment_size_bytes = sizeof(es_segment_header_t);
    }

    return ES_OK;
}

static void es_storage_writer_close_active_segment(
    es_stream_storage_state_t* state
) {
    if(!state) {
        return;
    }

    if(state->active_segment_file) {
        if(state->active_camera_file) {
            fclose(state->active_camera_file);
            state->active_camera_file = NULL;
        }

        // Close the segment file first (was open in "ab" mode)
        fclose(state->active_segment_file);
        state->active_segment_file = NULL;

        if(state->camera_stream_size_bytes > 0) {
            // 1. Open in "r+b" to overwrite the header offset
            FILE* update_file = fopen(state->active_segment_path, "r+b");
            if(update_file) {
                uint32_t offset = (uint32_t)state->active_segment_size_bytes;
                fseek(update_file, offsetof(es_segment_header_t, camera_stream_offset), SEEK_SET);
                fwrite(&offset, sizeof(offset), 1, update_file);
                fclose(update_file);
            }

            // 2. Open in "ab" to append the camera stream bytes
            FILE* append_file = fopen(state->active_segment_path, "ab");
            FILE* cam_file = fopen(state->active_camera_path, "rb");
            if(append_file && cam_file) {
                char copy_buf[4096];
                size_t read_bytes;
                while((read_bytes = fread(copy_buf, 1, sizeof(copy_buf), cam_file)) > 0) {
                    fwrite(copy_buf, 1, read_bytes, append_file);
                }
            }
            if(append_file) fclose(append_file);
            if(cam_file) fclose(cam_file);
            
            remove(state->active_camera_path);
        } else {
            remove(state->active_camera_path);
        }
    }
}

static es_status_t es_storage_writer_rollover_segment(
    es_stream_storage_state_t* state
) {
    if(!state) {
        return ES_ERR_INVALID_ARG;
    }

    es_storage_writer_close_active_segment(state);

    state->active_segment_index++;
    state->active_segment_size_bytes = 0;
    es_compression_context_reset(&state->compression_ctx);

    es_status_t status = es_storage_writer_build_segment_path(
        state->stream_dir_path,
        state->active_segment_index,
        state->active_segment_path,
        sizeof(state->active_segment_path)
    );

    if(status != ES_OK) {
        return status;
    }

    return ES_OK;
}

static size_t es_storage_writer_record_encoded_size(const es_record_t* record) {
    if(!record) {
        return 0;
    }

    return sizeof(record->timestamp_ns)
        + sizeof(record->record_type_id)
        + sizeof(record->flags)
        + sizeof(record->payload_size)
        + record->payload_size;
}

static es_status_t es_storage_writer_validate_record(
    const es_record_t* record
) {
    if(!record) {
        return ES_ERR_INVALID_ARG;
    }

    if(record->record_type_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(record->payload_size > 0 && !record->payload) {
        return ES_ERR_INVALID_ARG;
    }

    return ES_OK;
}

static es_status_t es_storage_writer_prepare_for_append(
    const es_engine_t* engine,
    es_stream_storage_state_t* state,
    const es_record_t* record
) {
    if(!engine || !state || !record) {
        return ES_ERR_INVALID_ARG;
    }

    size_t record_size = engine->config.compression_enabled
    ? es_compression_encoded_record_size(
        &state->compression_ctx,
        ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA,
        record
    )
    : es_storage_writer_record_encoded_size(record);

    size_t current_segment_size = state->active_segment_size_bytes == 0
        ? sizeof(es_segment_header_t)
        : state->active_segment_size_bytes + state->camera_stream_size_bytes;

    if(current_segment_size + record_size > engine->config.segment_size_bytes) {
        return es_storage_writer_rollover_segment(state);
    }

    return ES_OK;
}

static es_status_t es_storage_writer_write_record_bytes(
    FILE* file,
    const es_record_t* record
) {
    if(!file || !record) {
        return ES_ERR_INVALID_ARG;
    }

    if(fwrite(&record->timestamp_ns, sizeof(record->timestamp_ns), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(fwrite(&record->record_type_id, sizeof(record->record_type_id), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(fwrite(&record->flags, sizeof(record->flags), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(fwrite(&record->payload_size, sizeof(record->payload_size), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(record->payload_size > 0) {
        if(!record->payload) {
            return ES_ERR_INVALID_ARG;
        }

        if(fwrite(record->payload, 1, record->payload_size, file) != record->payload_size) {
            return ES_ERR_IO;
        }
    }

    return ES_OK;
}

es_status_t es_storage_writer_init(es_engine_t* engine) {
    if(!engine) {
        return ES_ERR_INVALID_ARG;
    }

    engine->stream_storage_states = NULL;
    engine->stream_storage_count = 0;
    engine->stream_storage_capacity = 0;

    es_status_t status = es_storage_writer_ensure_directory(engine->config.storage_path);
    if(status != ES_OK) {
        return status;
    }

    return ES_OK;
}

void es_storage_writer_shutdown(es_engine_t* engine) {
    if(!engine) {
        return;
    }

    for(size_t i = 0; i < engine->stream_storage_count; ++i) {
        es_storage_writer_close_active_segment(&engine->stream_storage_states[i]);
        es_compression_context_destroy(&engine->stream_storage_states[i].compression_ctx);
    }

    free(engine->stream_storage_states);
    engine->stream_storage_states = NULL;
    engine->stream_storage_count = 0;
    engine->stream_storage_capacity = 0;
}

es_stream_storage_state_t* es_storage_writer_find_stream_state(
    es_engine_t* engine,
    uint32_t stream_id
) {
    if(!engine || stream_id == 0) {
        return NULL;
    }

    for(size_t i = 0; i < engine->stream_storage_count; ++i) {
        if(engine->stream_storage_states[i].stream_id == stream_id) {
            return &engine->stream_storage_states[i];
        }
    }

    return NULL;
}

es_status_t es_storage_writer_register_stream(
    es_engine_t* engine,
    uint32_t stream_id
) {
    if(!engine || stream_id == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(es_storage_writer_find_stream_state(engine, stream_id)) {
        return ES_OK;
    }

    es_status_t status = es_storage_writer_ensure_capacity(engine);
    if(status != ES_OK) {
        return status;
    }

    es_stream_storage_state_t* state = 
        &engine->stream_storage_states[engine->stream_storage_count];

    state->stream_id = stream_id;
    state->active_segment_index = 1;
    state->active_segment_size_bytes = 0;
    es_compression_context_init(&state->compression_ctx);
    state->active_segment_file = NULL;
    state->stream_dir_path[0] = '\0';
    state->active_segment_path[0] = '\0';
    state->active_camera_file = NULL;
    state->active_camera_path[0] = '\0';
    state->camera_stream_size_bytes = 0;

    es_status_t path_status = es_storage_writer_build_stream_dir_path(
        engine,
        stream_id,
        state->stream_dir_path,
        sizeof(state->stream_dir_path)
    );

    if(path_status != ES_OK) {
        return path_status;
    }

    path_status = es_storage_writer_ensure_directory(state->stream_dir_path);
    if(path_status != ES_OK) {
        return path_status;
    }

    path_status = es_storage_writer_build_segment_path(
        state->stream_dir_path,
        state->active_segment_index,
        state->active_segment_path,
        sizeof(state->active_segment_path)
    );

    if(path_status != ES_OK) {
        return path_status;
    }

    engine->stream_storage_count++;

    return ES_OK;
}

es_status_t es_storage_writer_append_record(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* record
) {
    if(!engine || stream_id == 0 || !record) {
        return ES_ERR_INVALID_ARG;
    }

    es_status_t validation_status = es_storage_writer_validate_record(record);
    if(validation_status != ES_OK) {
        return validation_status;
    }

    es_stream_storage_state_t* state = 
        es_storage_writer_find_stream_state(engine, stream_id);

    if(!state) {
        return ES_ERR_NOT_FOUND;
    }

    es_status_t status = es_storage_writer_prepare_for_append(engine, state, record);
    if(status != ES_OK) {
        return status;
    }

    size_t written_size = engine->config.compression_enabled
        ? es_compression_encoded_record_size(&state->compression_ctx, ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA, record)
        : es_storage_writer_record_encoded_size(record);

    status = es_storage_writer_open_active_segment(engine, state);
    if(status != ES_OK) {
        return status;
    }

    if(engine->config.compression_enabled) {
        status = es_compression_write_record(
            state->active_segment_file,
            &state->compression_ctx,
            ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA,
            record
        );
    } else {
        status = es_storage_writer_write_record_bytes(
            state->active_segment_file,
            record
        );
    }

    if(status != ES_OK) {
        return status;
    }

    if(fflush(state->active_segment_file) != 0) {
        return ES_ERR_IO;
    }

    state->active_segment_size_bytes += written_size;
    return ES_OK;
}

es_status_t es_storage_writer_append_batch(
    es_engine_t* engine,
    uint32_t stream_id,
    const es_record_t* records,
    size_t count
) {
    if(!engine || stream_id == 0 || !records || count == 0) {
        return ES_ERR_INVALID_ARG;
    }

    for(size_t i = 0; i < count; ++i) {
        es_status_t validation_status = 
            es_storage_writer_validate_record(&records[i]);

        if(validation_status != ES_OK) {
            return validation_status;
        }
    }

    for(size_t i = 0; i < count; ++i) {
        es_status_t status = es_storage_writer_append_record(
            engine,
            stream_id,
            &records[i]
        );

        if(status != ES_OK) {
            return status;
        }
    }

    return ES_OK;
}

static es_status_t es_storage_writer_prepare_for_camera_append(
    const es_engine_t* engine,
    es_stream_storage_state_t* state,
    size_t frame_size
) {
    if(!engine || !state) {
        return ES_ERR_INVALID_ARG;
    }

    size_t current_total_size = state->active_segment_size_bytes == 0
        ? sizeof(es_segment_header_t)
        : state->active_segment_size_bytes + state->camera_stream_size_bytes;

    if(current_total_size + frame_size > engine->config.segment_size_bytes) {
        return es_storage_writer_rollover_segment(state);
    }

    return ES_OK;
}

es_status_t es_storage_writer_append_camera_frame(
    es_engine_t* engine,
    uint32_t stream_id,
    uint64_t timestamp_ns,
    uint32_t frame_index,
    uint32_t jpeg_size,
    const void* jpeg_bytes
) {
    if(!engine || stream_id == 0 || jpeg_size == 0 || !jpeg_bytes) {
        return ES_ERR_INVALID_ARG;
    }

    es_stream_storage_state_t* state = 
        es_storage_writer_find_stream_state(engine, stream_id);

    if(!state) {
        return ES_ERR_NOT_FOUND;
    }

    size_t frame_size = sizeof(timestamp_ns) + sizeof(frame_index) + sizeof(jpeg_size) + jpeg_size;

    es_status_t status = es_storage_writer_prepare_for_camera_append(engine, state, frame_size);
    if(status != ES_OK) {
        return status;
    }

    status = es_storage_writer_open_active_segment(engine, state);
    if(status != ES_OK) {
        return status;
    }

    if(!state->active_camera_file) {
        return ES_ERR_IO;
    }

    if(fwrite(&timestamp_ns, sizeof(timestamp_ns), 1, state->active_camera_file) != 1) {
        return ES_ERR_IO;
    }

    if(fwrite(&frame_index, sizeof(frame_index), 1, state->active_camera_file) != 1) {
        return ES_ERR_IO;
    }

    if(fwrite(&jpeg_size, sizeof(jpeg_size), 1, state->active_camera_file) != 1) {
        return ES_ERR_IO;
    }

    if(fwrite(jpeg_bytes, 1, jpeg_size, state->active_camera_file) != jpeg_size) {
        return ES_ERR_IO;
    }

    if(fflush(state->active_camera_file) != 0) {
        return ES_ERR_IO;
    }

    state->camera_stream_size_bytes += frame_size;
    return ES_OK;
}