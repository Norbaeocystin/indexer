sui indexer for scallop

[sui custom indexer docs](https://docs.sui.io/guides/developer/advanced/custom-indexer)

install mongodb,redis,
turn on checkpoint executor by adding to sui full node config:
```
checkpoint-executor-config:
    checkpoint-execution-max-concurrency: 200
    local-execution-timeout-sec: 30
    data-ingestion-dir: /mnt/sui/ingestion
    
```  
    
 also possible to use option to create service ( exit when there are no files ...)

```
[Unit]
Description= Triggers the service

[Path]
DirectoryNotEmpty=/path/to/monitor

[Install]
WantedBy=multi-user.target
```