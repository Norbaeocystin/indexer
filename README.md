sui indexer for scallop

[sui custom indexer docs](https://docs.sui.io/guides/developer/advanced/custom-indexer)

scallop data are starting from 7 976 007 - checkpoint ... +- epoch 80 ...

install mongodb,redis,
turn on checkpoint executor by adding to sui full node config:
```
checkpoint-executor-config:
    checkpoint-execution-max-concurrency: 200
    local-execution-timeout-sec: 30
    data-ingestion-dir: /mnt/sui/ingestion
    
```  
    
 also possible to use option to create service ( exit when there are no files ...) but there can be problem with sinchronization ...
exit if there is indexer process? or system service will file?

```
[Unit]
Description= Triggers the service

[Path]
DirectoryNotEmpty=/path/to/monitor

[Install]
WantedBy=multi-user.target
```

6769021 23:31
6800301 23:41

10:46 7744606
11:39 7809741
curl --location --request POST 'https://fullnode.mainnet.sui.io:443/' --header 'Content-Type: application/json' --data-raw
'{
"jsonrpc": "2.0",
"id": 1,
"method": "suix_getLatestSuiSystemState",
"params": []
}'

- "/dns/sui-main.astrostakers.com/udp/8084", -Netherlands