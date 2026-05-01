# Benchmark Results

Current benchmark status: working prototype baseline

Example result snapshot:
- 1M records
- 64B payload
- batch size: 100
- batch write: ~847K rec/s
- compressed write: ~840K rec/s
- query latency: ~131.6 ms
- compression ratio: ~1.053x

Key takeaway:
- write path is promising
- current compression is still limited
- query path works but is linear-scan based