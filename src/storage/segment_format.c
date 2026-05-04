#include "segment_format.h"

#include <string.h>

es_status_t es_segment_format_write_header(
    FILE* file,
    const es_segment_header_t* header
) {
    if(!file || !header) {
        return ES_ERR_INVALID_ARG;
    }

    if(es_segment_format_validate_header(header) != ES_OK) {
        return ES_ERR_INVALID_ARG;
    }

    if(fwrite(header, sizeof(*header), 1, file) != 1) {
        return ES_ERR_IO;
    }

    return ES_OK;
}

es_status_t es_segment_format_read_header(
    FILE* file,
    es_segment_header_t* out_header
) {
    if(!file || !out_header) {
        return ES_ERR_INVALID_ARG;
    }

    memset(out_header, 0, sizeof(*out_header));

    if(fread(out_header, sizeof(*out_header), 1, file) != 1) {
        return ES_ERR_IO;
    }

    return es_segment_format_validate_header(out_header);
}

es_status_t es_segment_format_validate_header(
    const es_segment_header_t* header
) {
    if(!header) {
        return ES_ERR_INVALID_ARG;
    }

    if(memcmp(header->magic, ES_SEGMENT_MAGIC, sizeof(header->magic)) != 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(header->version != ES_SEGMENT_FORMAT_VERSION) {
        return ES_ERR_INVALID_ARG;
    }

    if(header->header_size != sizeof(es_segment_header_t)) {
        return ES_ERR_INVALID_ARG;
    }

    if(header->stream_id == 0 || header->segment_index == 0) {
        return ES_ERR_INVALID_ARG;
    }

    if(header->record_format_version != ES_RECORD_FORMAT_VERSION) {
        return ES_ERR_INVALID_ARG;
    }

    if(header->compression_mode != ES_SEGMENT_COMPRESSION_NONE &&
       header->compression_mode != ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA) {
        return ES_ERR_INVALID_ARG;
    }

    return ES_OK;
}

es_segment_compression_mode_t es_segment_format_compression_from_config(
    int compression_enabled
) {
    return compression_enabled 
        ? ES_SEGMENT_COMPRESSION_TIMESTAMP_DELTA
        : ES_SEGMENT_COMPRESSION_NONE;
}