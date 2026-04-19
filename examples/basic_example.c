#include "edgestorage/edgestorage.h"

#include <stdio.h>

int main(void) {
    es_config_t config = {
        .storage_path = "./data",
        .segment_size_bytes = 10 * 1024 * 1024,
        .write_buffer_size_bytes = 64 * 1024,
        .compression_enabled = 0
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        fprintf(stderr, "Failed to open engine\n");
        return 1;
    }

    printf("EdgeStorage example started successfully\n");

    es_close(engine);

    return 0;
}