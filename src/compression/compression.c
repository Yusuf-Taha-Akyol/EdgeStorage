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

    return timestamp_size
        + sizeof(record->record_type_id)
        + sizeof(record->flags)
        + sizeof(record->payload_size)
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

    if(ctx->has_last_timestamp) {
        uint32_t delta_ns = 0;
        if(fread(&delta_ns, sizeof(delta_ns), 1, file) != 1) {
            if(feof(file)) {
                return ES_ERR_NOT_FOUND;
            }
            return ES_ERR_IO;
        }
        out_record->timestamp_ns = ctx->last_timestamp_ns + delta_ns;
    } else {
        if(fread(&out_record->timestamp_ns, sizeof(out_record->timestamp_ns), 1, file) != 1) {
            if(feof(file)) {
                return ES_ERR_NOT_FOUND;
            }
            return ES_ERR_IO;
        }
    }

    if(fread(&out_record->record_type_id, sizeof(out_record->record_type_id), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(fread(&out_record->flags, sizeof(out_record->flags), 1, file) != 1) {
        return ES_ERR_IO;
    }

    if(fread(&out_record->payload_size, sizeof(out_record->payload_size), 1, file) != 1) {
        return ES_ERR_IO;
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