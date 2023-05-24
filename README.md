# Distributed Computation Engine

A DFS that supports MapReduce jobs. Key features include:
- Datatype-aware chunk partitioning
- Job submission and monitoring, including pushing computations to nodes for data locality
- Load balancing across computation nodes
- The Map, Shuffle, and Reduce phases of computation

## Build
Proto File: inside dfs directory<br>
```
proto/build.sh
```

Go Module: inside dfs directory<br>
```
go mod init dfs
go mod tidy
```

Create so files: inside respective job directory<br>
```
go build -buildmode=plugin word_count.go
go build -buildmode=plugin log_analyzer.go
go build -buildmode=plugin sort.go
```

## Run
Start Controller and Storage Node(s): inside go directory
```
./controller listen-port
./storage_node host:port listen-port {dest}
```

Start and Stop Cluster: inside root directory
```
./start_cluster.sh
./stop_cluster.sh
```

## Client Command Line Interface
```
# Split file into 8 (MB) chunks and store
./client host:port put cats.txt 8

# Retrieve a file
./client host:port get cats.txt {dest}

# Delete a file
./client host:port delete cats.txt

# List of all files in cluster
./client host:port ls

# List of total available space, and all active nodes + number of requests handled
./client host:port nodes
```

## Resource Manager Command Line Interface
```
# Perform my_job on cats.txt file (example: word count job)
./yarm host:port my_job.so cats.txt {num_reducers}
```

## Network Topology
Follows a hub-and-spoke model where the controller listens for pings from active storage nodes and records their latest heartbeats.<br>
See [design details](https://github.com/ashleyradford/distributed-file-system/blob/main/docs/design.md).
<img src="https://github.com/ashleyradford/distributed-file-system/blob/main/DFS-design.png" width=90% height=90%>

## MapReduce Computation
The resource manager yarm acts as a secondary client and submits jobs to ber performed on already stored files.

## To Do
- [ ] MessageHandler already closes out conn
- [ ] Store reduced files through storage node communication with controller instead of calling on client
- [ ] Update resource manager to act more like a manager and not a secondary client
- [ ] Key, value pairs are strings, maybe better to have them as bytes
- [ ] Should use serialization for the temp files

## Limitations
- Not verifying a checksum of the sent so file
- No error handling when requesting job for not fully stored file
- Can only run one job at a time
- In sort job, filter out the low number lines bc otherwise: `error in reduce phase: bufio.Scanner: token too long`
