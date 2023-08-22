# Distributed Computation Engine

A DFS that supports multiple storage nodes and MapReduce jobs. Key features include:

- Parallel storage/retrieval: large files can be split into multiple chunks to spread load across the cluster and provide parallelism during retrievals.
- Interoperability: uses Google Protocol Buffers to serialize messages.
- Fault tolerance: detects and withstands concurrent storage node failures all the way down to the minimum replication level (3) provided that there is enough time in between failures to re-replicate data.
- Data Integrity: able to recover corrupted files.
- Datatype-aware chunk partitioning: if the file is text-based then it splits on the line boundaries rather than the byte size.
- Job submission and monitoring, including pushing computations to nodes for data locality.
- Load balancing across computation nodes.
- The Map, Shuffle, and Reduce phases of computation.

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
See [design details](https://github.com/ashleyradford/distributed-file-system/blob/main/docs/dfs_design.md).
<img src="https://github.com/ashleyradford/distributed-file-system/blob/main/docs/dfs_design.png" width=90% height=90%>

## MapReduce Computation
The resource manager yarm acts as a secondary client and submits jobs to ber performed on already stored files.

## To Do
- [ ] Clean up error handling (logging) and make sure that everything is being closed even in case of error.
- [ ] All io.copy calls need error checking as well.
- [ ] Clean up msg names in message_handler, and check that all response names are correctly named.
- [ ] Reorganize protobuf messages, some messages can be grouped and we can split into 3 different receive groups.
- [ ] Consider the case when a node goes down for just a few seconds, but then gets back up and running (didn't miss the hb checks), should the program always join at the beg or just send a HB initially?
- [ ] MessageHandler already closes out conn.
- [ ] Store reduced files through storage node communication with controller instead of calling on client.
- [ ] Update resource manager to act more like a manager and not a secondary client.
- [ ] Key, value pairs are strings, maybe better to have them as bytes.
- [ ] Should use serialization for the temp files.

## Limitations
- When the controller receives a storage request, it only checks if the file is able to fit into the cluster as a whole, it doesn't check that there is a node big enough for each chunk. For example, if we have a chunk that is size 30MB but we only have 12 3MB nodes available, it could be stuck in an infinite loop trying to find a chunk that is big enough since we only checked the overall space.
- Each replica is required to be on a different storage node, if the number of nodes drops below replication level this becomes a problem. Currently just logging this issue.
- Currently, when a storage node detects corruption and requests a copy, the controller responds with a replica address (one that has a copy). If the copy on this node is corrupted, the request will be sent out again from this node. But the replica node is picked at random, so there is a chance that there is an excess amount of communication if a corrupted node is picked multiple times. This issue is amplified if all copies are corrupt as it will become an infinite loop that eventually crashes the nodes. Responding with a list of replica nodes to iterate through would require a bit of refactoring as it results in some messy recursion as is.
- Not verifying a checksum of the sent so file.
- No error handling when requesting job for not fully stored file.
- Can only run one job at a time.
- In sort job, filter out the low number lines bc otherwise: `error in reduce phase: bufio.Scanner: token too long`.
