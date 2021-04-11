# Mongodb resource requirements

Tool for getting disk and ram requirements for mongodb collection. It supports reading official document schema [format](https://docs.mongodb.com/realm/mongodb/document-schemas/) from file and getting real scaled statistics of required resources by populating running instance of mongodb with random data based on the read schema.

# Example
Getting stat for `schema/example.json` by execution of command:
```shell
go run cmd/main.go -schema schema/example.json -mongo_addr 127.0.0.1:27017
```

Output:
```shell
Collection events stat:
Storage size: 1983.643 gb
Index _id_ size: 198.364 gb
Index user_id size: 358.582 gb
Index user_id_tag size: 740.051 gb

Total stat:
Storage size: 1983.643 gb
Index size: 1296.997 gb
```


