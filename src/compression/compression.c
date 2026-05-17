#include "compression.h"
#include "segment_format.h"

#include <stdlib.h>
#include <string.h>
#include <limits.h>

void es_compression_context_init(es_compression_context_t* ctx) {
    if(!ctx) {
        return;
    }

    ctx->last_timestamp_ns = 0;
    ctx->has_last_timestamp = 0;
    ctx->previous_payload = NULL;
    ctx->previous_payload_size = 0;
    ctx->has_previous_payload = 0;
}

void es_compression_context_reset(es_compression_context_t* ctx) {
    if(!ctx) {
        return;
    }

    free(ctx->previous_payload);
    ctx->previous_payload = NULL;
    ctx->previous_payload_size = 0;
    ctx->has_previous_payload = 0;

    ctx->last_timestamp_ns = 0;
    ctx->has_last_timestamp = 0;
}

void es_compression_context_destroy(es_compression_context_t* ctx) {
    if(!ctx) {
        return;
    }

    free(ctx->previous_payload);
    ctx->previous_payload = NULL;
    ctx->previous_payload_size = 0;
    ctx->has_previous_payload = 0;

    ctx->last_timestamp_ns = 0;
    ctx->has_last_timestamp = 0;
}

static es_payload_encoding_t es_compression_choose_payload_encoding(
    const es_compression_context_t* ctx,
    const es_record_t* record,
    uint32_t* out_encoded_payload_size
) {
    if(out_encoded_payload_size) {
        *out_encoded_payload_size = record ? record->payload_size : 0;
    }

    if(!ctx || !record || !out_encoded_payload_size) {
        return ES_PAYLOAD_ENCODING_RAW;
    }

    if(record->payload_size == 0) {
        *out_encoded_payload_size = 0;
        return ES_PAYLOAD_ENCODING_RAW;
    }

    if(!ctx->has_previous_payload || !ctx->previous_payload || ctx->previous_payload_size != record->payload_size || record->payload_size % sizeof(int32_t) != 0) {
        *out_encoded_payload_size = record->payload_size;
        return ES_PAYLOAD_ENCODING_RAW;
    }

    const int32_t* previous_values = (const int32_t*)ctx->previous_payload;
    const int32_t* current_values = (const int32_t*)record->payload;
    uint32_t value_count = record->payload_size / sizeof(int32_t);

    int fits_i8 = 1;
    int fits_i16 = 1;

    for(uint32_t i = 0; i < value_count; i++) {
        int64_t delta = (int64_t)current_values[i] - (int64_t)previous_values[i];

        if(delta < INT8_MIN || delta > INT8_MAX) {
            fits_i8 = 0;
        }

        if(delta < INT16_MIN || delta > INT16_MAX) {
            fits_i16 = 0;
            break;
        }
    }

    uint32_t i8_size = value_count * (uint32_t)sizeof(int8_t);
    uint32_t i16_size = value_count * (uint32_t)sizeof(int16_t);

    if(fits_i8 && i8_size < record->payload_size) {
        *out_encoded_payload_size = i8_size;
        return ES_PAYLOAD_ENCODING_DELTA_I32_I8;
    }

    if(fits_i16 && i16_size < record->payload_size) {
        *out_encoded_payload_size = i16_size;
        return ES_PAYLOAD_ENCODING_DELTA_I32_I16;
    }

    *out_encoded_payload_size = record->payload_size;
    return ES_PAYLOAD_ENCODING_RAW;
}

static es_status_t es_compression_update_previous_payload(
    es_compression_context_t* ctx,
    const void* payload,
    uint32_t payload_size
) {
    if(!ctx) {
        return ES_ERR_INVALID_ARG;
    }

    if(payload_size == 0) {
        free(ctx->previous_payload);
        ctx->previous_payload = NULL;
        ctx->previous_payload_size = 0;
        ctx->has_previous_payload = 0;
        return ES_OK;
    }

    if(!payload) {
        return ES_ERR_INVALID_ARG;
    }

    unsigned char* copy = (unsigned char*)malloc(payload_size);
    if(!copy) {
        return ES_ERR_OOM;
    }

    memcpy(copy, payload, payload_size);

    free(ctx->previous_payload);
    ctx->previous_payload = copy;
    ctx->previous_payload_size = payload_size;
    ctx->has_previous_payload = 1;

    return ES_OK;
}

static es_status_t es_compression_write_encoded_payload(
    FILE* file,
    const es_compression_context_t* ctx,
    const es_record_t* record,
    es_payload_encoding_t payload_encoding
) {
    if(!file || !ctx || !record) {
        return ES_ERR_INVALID_ARG;
    }

    if(record->payload_size == 0) {
        return ES_OK;
    }

    if(!record->payload) {
        return ES_ERR_INVALID_ARG;
    }

    if(payload_encoding == ES_PAYLOAD_ENCODING_RAW) {
        if(fwrite(record->payload, 1, record->payload_size, file) != record->payload_size) {
            return ES_ERR_IO;
        }

        return ES_OK;
    }

    if(!ctx->has_previous_payload || !ctx->previous_payload || ctx->previous_payload_size != record->payload_size || record->payload_size % sizeof(int32_t) != 0) {
        return ES_ERR_INVALID_ARG;
    }

    const int32_t* previous_values = (const int32_t*)ctx->previous_payload;
    const int32_t* current_values = (const int32_t*)record->payload;
    uint32_t value_count = record->payload_size / (uint32_t)sizeof(int32_t);

    if(payload_encoding == ES_PAYLOAD_ENCODING_DELTA_I32_I8) {
        for(uint32_t i = 0; i < value_count; i++) {
            int64_t delta = (int64_t)current_values[i] - (int64_t)previous_values[i];

            if(delta < INT8_MIN || delta > INT8_MAX) {
                return ES_ERR_INVALID_ARG;
            }

            int8_t encoded_delta = (int8_t)delta;
            if(fwrite(&encoded_delta, sizeof(encoded_delta), 1, file) != 1) {
                return ES_ERR_IO;
            }
        }

        return ES_OK;
    }

    if(payload_encoding == ES_PAYLOAD_ENCODING_DELTA_I32_I16) {
        for(uint32_t i = 0; i < value_count; i++) {
            int64_t delta = (int64_t)current_values[i] - (int64_t)previous_values[i];

            if(delta < INT16_MIN || delta > INT16_MAX) {
                return ES_ERR_INVALID_ARG;
            }

            int16_t encoded_delta = (int16_t)delta;
            if(fwrite(&encoded_delta, sizeof(encoded_delta), 1, file) != 1) {
                return ES_ERR_IO;
            }
        }

        return ES_OK;
    }

    return ES_ERR_INVALID_ARG;
}

static es_status_t es_compression_read_encoded_payload(
    FILE* file,
    es_compression_context_t* ctx,
    const es_encoded_record_header_t* header,
    void** out_payload
) {
    if(!file || !ctx || !header || !out_payload) {
        return ES_ERR_INVALID_ARG;
    }

    *out_payload = NULL;

    if(header->uncompressed_payload_size == 0) {
        if(header->encoded_payload_size != 0) {
            return ES_ERR_INVALID_ARG;
        }

        return ES_OK;
    }

    if(header->payload_encoding == ES_PAYLOAD_ENCODING_RAW) {
        if(header->encoded_payload_size != header->uncompressed_payload_size) {
            return ES_ERR_INVALID_ARG;
        }

        void* payload = malloc(header->uncompressed_payload_size);

        if(!payload) {
            return ES_ERR_OOM;
        }

        if(fread(payload, 1, header->uncompressed_payload_size, file) != header->uncompressed_payload_size) {
            free(payload);
            return ES_ERR_IO;
        }

        *out_payload = payload;
        return ES_OK;
    }

    if(!ctx->has_previous_payload || !ctx->previous_payload || ctx->previous_payload_size != header->uncompressed_payload_size || header->uncompressed_payload_size % sizeof(int32_t) != 0) {
        return ES_ERR_INVALID_ARG;
    }

    uint32_t value_count = header->uncompressed_payload_size / (uint32_t)sizeof(int32_t);

    if(header->payload_encoding == ES_PAYLOAD_ENCODING_DELTA_I32_I8) {
        if(header->encoded_payload_size != value_count * (uint32_t)sizeof(int8_t)) {
            return ES_ERR_INVALID_ARG;
        }
    } else if (header->payload_encoding == ES_PAYLOAD_ENCODING_DELTA_I32_I16) {
        if(header->encoded_payload_size != value_count * (uint32_t)sizeof(int16_t)) {
            return ES_ERR_INVALID_ARG;
        }
    } else {
        return ES_ERR_INVALID_ARG;
    }

    int32_t* payload = (int32_t*)malloc(header->uncompressed_payload_size);
    if(!payload) {
        return ES_ERR_OOM;
    }

    const int32_t* previous_values = (const int32_t*)ctx->previous_payload;

    for(uint32_t i = 0; i < value_count; i++) {
        if(header->payload_encoding == ES_PAYLOAD_ENCODING_DELTA_I32_I8) {
            int8_t delta = 0;
            if(fread(&delta, sizeof(delta), 1, file) != 1) {
                free(payload);
                return ES_ERR_IO;
            }

            payload[i] = previous_values[i] + (int32_t)delta;
        } else {
            int16_t delta = 0;
            if(fread(&delta, sizeof(delta), 1, file) != 1) {
                free(payload);
                return ES_ERR_IO;
            }

            payload[i] = previous_values[i] + (int32_t)delta;
        }
    }

    *out_payload = payload;
    return ES_OK;
}

size_t es_compression_encoded_record_size(
    const es_compression_context_t* ctx,
    uint32_t compression_mode,
    const es_record_t* record
) {
    if(!ctx || !record) {
        return 0;
    }

    if(compression_mode != ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA) {
        return 0;
    }

    size_t timestamp_size = ctx->has_last_timestamp
        ? sizeof(uint32_t)
        : sizeof(record->timestamp_ns);

    uint32_t encoded_payload_size = record->payload_size;
    (void)es_compression_choose_payload_encoding(
        ctx,
        record,
        &encoded_payload_size
    );

    return sizeof(es_encoded_record_header_t)
        + timestamp_size
        + encoded_payload_size;
}

es_status_t es_compression_write_record(
    FILE* file,
    es_compression_context_t* ctx,
    uint32_t compression_mode,
    const es_record_t* record
) {
    if(!file || !ctx || !record) {
        return ES_ERR_INVALID_ARG;
    }

    if(compression_mode != ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA) {
        return ES_ERR_INVALID_ARG;
    }

    if(record->payload_size > 0 && !record->payload) {
        return ES_ERR_INVALID_ARG;
    }

    uint8_t timestamp_encoding = ctx->has_last_timestamp
        ? ES_TIMESTAMP_ENCODING_DELTA_U32
        : ES_TIMESTAMP_ENCODING_FULL_U64;


    uint32_t encoded_payload_size = record->payload_size;
    es_payload_encoding_t payload_encoding = es_compression_choose_payload_encoding(
        ctx,
        record,
        &encoded_payload_size
    );

    es_encoded_record_header_t header = {
        .timestamp_encoding = timestamp_encoding,
        .payload_encoding = payload_encoding,
        .record_type_id = record->record_type_id,
        .flags = record->flags,
        .reserved = 0,
        .uncompressed_payload_size = record->payload_size,
        .encoded_payload_size = encoded_payload_size
    };

    if(fwrite(&header, sizeof(header), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(ctx->has_last_timestamp) {
        if(record->timestamp_ns < ctx->last_timestamp_ns) {
            return ES_ERR_INVALID_ARG;
        }

        uint64_t delta_ns_64 = record->timestamp_ns - ctx->last_timestamp_ns;

        if(delta_ns_64 > UINT32_MAX) {
            return ES_ERR_INVALID_ARG;
        }

        uint32_t delta_ns = (uint32_t)delta_ns_64;

        if(fwrite(&delta_ns, sizeof(delta_ns), 1, file) != 1) {
            return ES_ERR_IO;
        }
    } else {
        if(fwrite(&record->timestamp_ns, sizeof(record->timestamp_ns), 1, file) != 1) {
            return ES_ERR_IO;
        }
    }

    es_status_t write_payload_status = es_compression_write_encoded_payload(
        file,
        ctx,
        record,
        payload_encoding
    );

    if(write_payload_status != ES_OK) {
        return write_payload_status;
    }

    es_status_t payload_status = es_compression_update_previous_payload(
        ctx,
        record->payload,
        record->payload_size
    );

    if(payload_status != ES_OK) {
        return payload_status;
    }

    ctx->last_timestamp_ns = record->timestamp_ns;
    ctx->has_last_timestamp = 1;

    return ES_OK;
}

es_status_t es_compression_read_record(
    FILE* file,
    es_compression_context_t* ctx,
    uint32_t compression_mode,
    es_record_t* out_record
)
{
    if(!file || !ctx || !out_record) {
        return ES_ERR_INVALID_ARG;
    }

    if(compression_mode != ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA) {
        return ES_ERR_INVALID_ARG;
    }

    memset(out_record, 0, sizeof(*out_record));

    es_encoded_record_header_t header;
    if(fread(&header, sizeof(header), 1, file) != 1) {
        if(feof(file)) {
            return ES_ERR_NOT_FOUND;
        }

        return ES_ERR_IO;
    }

    out_record->record_type_id = header.record_type_id;
    out_record->flags = header.flags;
    out_record->payload_size = header.uncompressed_payload_size;
    out_record->payload = NULL;

    if(header.timestamp_encoding == ES_TIMESTAMP_ENCODING_FULL_U64) {
        if(fread(&out_record->timestamp_ns, sizeof(out_record->timestamp_ns), 1, file) != 1) {
            return ES_ERR_IO;
        }
    } else if(header.timestamp_encoding == ES_TIMESTAMP_ENCODING_DELTA_U32) {
        if(!ctx->has_last_timestamp) {
            return ES_ERR_INVALID_ARG;
        }

        uint32_t delta_ns = 0;
        if(fread(&delta_ns, sizeof(delta_ns), 1, file) != 1) {
            return ES_ERR_IO;
        }

        out_record->timestamp_ns = ctx->last_timestamp_ns + delta_ns;
    } else {
        return ES_ERR_INVALID_ARG;
    }

    void* payload = NULL;
    es_status_t payload_status = es_compression_read_encoded_payload(
        file,
        ctx,
        &header,
        &payload
    );

    if(payload_status != ES_OK) {
        return payload_status;
    }

    out_record->payload = payload;

    payload_status = es_compression_update_previous_payload(
        ctx,
        out_record->payload,
        out_record->payload_size
    );

    if(payload_status != ES_OK) {
        free((void*)out_record->payload);
        out_record->payload = NULL;
        return payload_status;
    }

    ctx->last_timestamp_ns = out_record->timestamp_ns;
    ctx->has_last_timestamp = 1;

    return ES_OK;
}