# Saturn L2 Node

The Saturn L2 Node is a CDN node for the Saturn network that fetches and caches/persists IPLD Dags serialised as CAR files. It fetches CAR files from origin servers that can serve IPLD data such as the IPFS Gateway and Filecoin SPs.


The L2 node is meant to run on NATT'd home machines. This means that the L2 implementation needs to account for:

1. Limited disk space available for caching CAR files -> we should never exceed the limit set by the home user.
2. Limited uplink and download bandwidths and the uplink speed will be much lower than the download speeds.
3. L2's will be NATT'd and not reachable from the public internet.
4. L2's will have flaky connectivity.

We've documented the important considerations and design of the L2 node that will allows us to build a reliable, low latency and high bandwidth CDN abstraction on top of these resource contrained homne machines with flaky connectivity at https://pl-strflt.notion.site/Building-the-Saturn-L2-network-and-L1-L2-interplay-6518deda51344a9db04bd3037b270ada.

The document also details the implementation path we will be taking to eventually build a robust and feature complete MVP for the Saturn L2.

## Features

At present, the L2 implementation has the following features:

#### Cache misses to the IPFS Gateway as an origin server

- The L2 node cache misses to the IPFS Gateway as it's origin server. The eventual goal is to cache miss to the Filecoin SP network and will be implemented down the road.

- The L2 node follows the "cache on second miss rule". It only fetches and caches content(read CAR files) if it see's two requests for the same content in a rolling duration of 24 hours. This is well established CDN engineering wisdom and prevents disk churn on the L2s as most content is only ever requested once from a CDN node.

### Dagstore as a cache for CAR files

- The L2 node uses the [dagstore](https://github.com/filecoin-project/dagstore) as a thread-safe, persistent, high-throughput and fixed sized cache for CAR files whose size we might not know upfront before we stream them from the origin server and download them. Filecoin SPs also use the dagstore for the same purpose i.e. a persistent cache for CAR files.

- The dagstore ensures that the space used to persist CAR files never exceeds the space allocated to the L2 node by the user. It does this by using an automated LRU GC algorithm to evict CAR files to make space for new CAR files when the space used by CAR files exceeds a given threshold. The LRU GC algorithm uses a quota allocation mechanism to account for the fact that the L2 node can't know the size of a CAR file upfront before streaming the entire file from the IPFS Gateway. More details can be found [here](https://github.com/filecoin-project/dagstore/pull/125).

- The dagstore is source agnostic and it should be relatively easy to swap out the IPFS Gateway with the Filecoin SP network down the line without changing the L2 implementation significantly.

### HTTP API for fetching L2 node stats

- The L2 node exposes an HTTP API to fetch stats that the operator/user of the L2 node might be interested in. These stats include the total amount of data downloaded by the L2 node, the amount of data served to other CDN peers by the L2 node etc etc.

- Note that the L2 node only binds to the localhost loopback interface for now and so this HTTP API can only be invoked by a caller running on the same machine.

### L1 Discovery and serving CAR files to L1s

- The L2 now supports interop with the Saturn L1 network. The L2 node can now serve retrievals of CAR files over HTTP for a given (root cid, optional skip offset) tuple if it already has the requested DAG.

- On it's first startup, the L2 node generates an Id(`L2Id`) for itself and persists it in the local file system. For all subsequent runs, the L2 node will reuse the persisted L2Id.

- On every startup, the L2 node connects to the configured L1 Discovery API (See the `L1_DISCOVERY_API_URL` environment variable below) to get back a list of L1 nodes to connect to.

- The L2 node then picks a maximum of `MAX_L1s`(configurable) L1s from those recieved from the Discovery API and joins
  the "swarm" for all those L1s.

   - The L2 node joins an L1's Swarm by invoking the GET `https://{L1_IP}/register/{L2Id}` registration API on the L1. The L1 should send back a 200     status code and then keep the connection alive. 
   - The L2 node then starts reading requests for CAR files from the response stream of the registration call made above. The L1 should send a request as      new line delimited json. The request is currently of the form:

     ```
     type CARTransferRequest struct {
	      RequestId  string
	      Root       string
	      SkipOffset uint64
     } 
     ```
   - For each request, the L2 node serves the CAR file by invoking the POST `https://{L1_IP}/data/{Root}/{RequestId}` API on the L1.
     The L2 will serve the CAR file only if it already has it. The L2 makes no guaruantees of sending back a POST response for each request recieved from an L1.
     If an L2 does not have a CAR file or if there's an error while serving the CAR file, the L2 will simply not send a POST or send some
     invalid bytes(incomplete CAR file) in the POST. The L1 should always ensure that a CAR file stream sent over POST ends with an `EOF` to ensure it has      read a complete valid CAR file.     
   - The number of concurrent requests that an L2 will serve for an L1 is configured using the `MAX_CONCURRENT_L1_REQUESTS` environment variable described      below.
   - **Note**: The L2 node also ships with an upper bound on the number of connections it makes to a single L1(5 for now) to prevent abuse. Connections will        be re-used to send responses for subsequent requests after an L1 has finished reading and closed the stream for an existing response over the              connection. If all connections are busy with ongoing responses, subsequent responses will block till a connection is available.
  

- If an L2 does not have the requested DAG, it simply returns a 404 so the client can fetch it directly from the IPFS Gateway. This decision was taken keeping in mind that it will be faster for the client to fetch the content directly from the IPFS Gateway rather than the client downloading it from the L2 which is itself downloading the content from the IPFS Gateway. This is because the L2 clients i.e. L1 Saturn nodes have significantly superior bandwidth compared to L2s. Low L2 uplink speeds without the benefits of geo-location and without the implementation of multi-peer L2 downloads can definitely become a bottleneck for L1s in the L2 cache miss scenario.

- The L2 network does NOT support parallel download of a DAG from multiple L2 nodes for now.

## Setting up the L2 node and invoking the HTTP APIs

1. Build and Install the Saturn L2 binary located at cmd/saturn-l2.
   ```
   cd cmd/saturn-l2
   go build ./...
   ```

2. Run the saturn-l2 binary
   ```
   cd cmd/saturn-l2
   ./saturn-l2
   ```

   Note that before running the binary, you need to configure/think about the following environment variables:
   
   1. `PORT`
       PORT is the environment variable that determines the port the saturn L2 service will bind to.
       If this environment variable is not configured, this service will bind to any available port by default.

   2. `ROOT_DIR`
       ROOT_DIR is the environment variable that determines the root directory of the Saturn L2 Node.
       All persistent state and cached CAR files will be persisted under this directory.
	    
       The following state is currently persisted inside the root directory on the user's machine:

       a) CAR files fetched from the IPFS Gateway. This is the data that the Saturn L2 CDN 
          node is caching. These are stored as flat files on disk.

       b) Internal dagstore bookkeeping state. The indices for the cached CARv2 files are 
          persisted as flat files on disk and the state of each  dag/shard/CAR file is persisted in a 
          leveldb key-value store on disk.

       c) L2 node stats that the L2 user/Station might be interested in. These are persisted in JSON format 
          in a leveldb key-value store on disk.

   3. `MAX_L2_DISK_SPACE`
       MAX_L2_DISK_SPACE is the environment variable that determines the maximum disk space the L2 node
       can use to store cached CAR files. If this env variable is not configured, it defaults to 200GiB.
       Note: The configured value should be greater than or equal to 200Gib.

   4. `FIL_WALLET_ADDRESS`
       FIL_WALLET_ADDRESS is the environment variable that determines the Filecoin wallet address of the L2 user.
       Note: This is a mandatory environment variable -> no default.

   5. `L1_DISCOVERY_API_URL`
       L1_DISCOVERY_API_URL is the environment variable that determines the URL of the L1 Discovery API to invoke to
       get back the L1 nodes this L2 node will connect and serve CAR files to. For the production environment, this is currently 
       https://orchestrator.strn.pl/nodes/nearby. However, please note that no default has currently been configured for this variable.
  
   6. `MAX_L1s`
       MAX_L1s is the environment variable that determines the maximum number of L1s this L2 will connect to and join the swarm for. 
       Defaults to 100. 

   7. `MAX_CONCURRENT_L1_REQUESTS`
       MAX_CONCURRENT_L1_REQUESTS is the environment variable that determines the maximum number of requests that will be 
       processed concurrently for a single L1. Defaults to 3. 


3. One the binary starts, it will print this to the standard output:

```
./saturn-l2
...
WebUI: http://localhost:52860/webui
API: http://localhost:52860/
...
```

When an L2 connects/disconnects with an L1 it prints this to standard output:

```
INFO: Saturn Node is online and connected to 1 peer(s)
INFO: Saturn Node is online and connected to 2 peer(s)
INFO: Saturn Node is online and connected to 1 peer(s)
ERROR: Saturn Node lost connection to the network
```

If you want to connect to `WebUI`, also run `./scripts/download-webui.sh`.

Note that the Saturn L2 node only binds to the **localhost** loopback network interface and so will only be reachable from the same machine.
In the above snippet, `52860` is the port that the Saturn L2 node binds to on the localhost interface. This port can be configured using the `PORT` environment variable as mentioned above.

### HTTP APIs

1. GET **/stats**

```
curl http://localhost:52860/stats

Response:
{"Version":"v0.0.0",
"BytesCurrentlyStored":0, -> Total space currently taken up by the cached CAR files on the L2 user's machine.
"TotalBytesUploaded":0,   -> Total number of bytes uploaded/served by the L2 node to requesting peers in it's entire lifetime.
"TotalBytesDownloaded":0, -> Total number of bytes downloaded by the L2 node from the IPFS Gateway/origin server in it's entire lifetime.
"NContentRequests":0,     -> Total number of requests received by the L2 node for content from clients in it's entire lifetime.
"NContentNotFoundReqs":0, -> Total number of requests for which the L2 did NOT have a cached CAR file to serve.
"NSuccessfulRetrievals":0 -> Total number of successful retrievals served by the L2 node.
"NContentReqErrors":0}    -> Total number of errors encountered by the L2 node while serving content to client in it's entire lifetime.
```

**Sample cids to test with** can be found [here](https://pl-strflt.notion.site/Sample-cids-for-testing-Saturn-IPFS-Gateway-can-serve-all-these-cids-4387a7b734aa4a5fa3166d8eac7cac5e). These are cids the IPFS Gateway can serve.

## Developer's Guide

### Release process

Publishing a new version is easy:

1. Create a tag
2. Push it to GitHub

GitHub Actions and GoReleaser will take care of the rest.

Example script:

```bash
$ git checkout main
$ git pull
$ git tag -a -s vA.B.C
$ git push origin vA.B.C
```

Replace vA.B.C with the actual version number, and please follow
[Semantic Versioning](https://semver.org).

You can omit `-s` if you are not signing your commits.


