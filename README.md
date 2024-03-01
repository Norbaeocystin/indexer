sui indexer for scallop

[sui custom indexer docs](https://docs.sui.io/guides/developer/advanced/custom-indexer)

install mongodb,redis,
turn on checkpoint executor by adding to sui full node config:
```
checkpoint-executor-config:
    checkpoint-execution-max-concurrency: 200
    local-execution-timeout-sec: 30
    data-ingestion-dir: /mnt/sui/ingestion```


loading of 10k files took 14 s
processing iteration: took 0 ms
removing 10k took: 5 s