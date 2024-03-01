sui indexer for scallop

install mongodb,
turn on checkpoint executor by adding to sui full node config:
```
checkpoint-executor-config:
    checkpoint-execution-max-concurrency: 200
    local-execution-timeout-sec: 30
    data-ingestion-dir: /mnt/sui/ingestion```