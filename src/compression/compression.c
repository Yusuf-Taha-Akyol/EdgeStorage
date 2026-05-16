#include "compression.h"
#include "segment_format.h"

#include <stdlib.h>
#include <string.h>

void es_compression_context_init(es_compression_context_t* ctx) {
    if(!ctx) {
        return;
    }
    ctx->last_timestamp_ns = 0;
    ctx->has_last_timestamp = 0;
}

void es_compression_context_reset(es_compression_context_t* ctx) {
    es_compression_context_init(ctx);
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

    return sizeof(es_encoded_record_header_t)
        + timestamp_size
        + record->payload_size;
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

    es_encoded_record_header_t header = {
        .timestamp_encoding = timestamp_encoding,
        .payload_encoding = ES_PAYLOAD_ENCODING_RAW,
        .record_type_id = record->record_type_id,
        .flags = record->flags,
        .reserved = 0,
        .uncompressed_payload_size = record->payload_size,
        .encoded_payload_size = record->payload_size
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

    if(record->payload_size > 0) {
        if(fwrite(record->payload, record->payload_size, 1, file) != 1) {
            return ES_ERR_IO;
        }
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

    if(header.payload_encoding != ES_PAYLOAD_ENCODING_RAW) {
        return ES_ERR_INVALID_ARG;
    }

    if(header.encoded_payload_size != header.uncompressed_payload_size) {
        return ES_ERR_INVALID_ARG;
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

    out_record->payload = NULL;

    if(out_record->payload_size > 0) {
        void* payload = malloc(out_record->payload_size);
        if(!payload) {
            return ES_ERR_OOM;
        }

        if(fread(payload, 1, out_record->payload_size, file) != out_record->payload_size) {
            free(payload);
            return ES_ERR_IO;
        }
        out_record->payload = payload;
    }

    ctx->last_timestamp_ns = out_record->timestamp_ns;
    ctx->has_last_timestamp = 1;

    return ES_OK;
}