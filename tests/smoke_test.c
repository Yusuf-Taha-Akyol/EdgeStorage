#include "/Users/yusuf_taha/projects/EdgeStorage/include/edgestorage/edgestorage.h"

int main(void) {
    es_config_t config = {
        .storage_path = "./testdata",
        .segment_size_bytes = 1024 * 1024,
        .write_buffer_size_bytes = 4096,
        .compression_enabled = 1
    };

    es_engine_t* engine = es_open(&config);
    if(!engine) {
        return 1;
    }

    es_close(engine);
    return 0;
}