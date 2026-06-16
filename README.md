# ClusterPulse

ClusterPulse is a work-in-progress implementation of a SWIM-style membership and failure-detection protocol in Go.

The project simulates a cluster of nodes that periodically probe each other, exchange membership information, and eventually detect unhealthy or unreachable nodes. The goal is to understand and build the core mechanics behind distributed membership systems used in large-scale infrastructure.

> Status: Work in Progress

## Overview

ClusterPulse models a small distributed cluster where every node maintains its own view of the cluster membership table. Nodes communicate through protocol messages such as `PING`, `ACK`, `PING-REQ`, `SUSPECT`, `ALIVE`, and `DEAD`.

The current implementation focuses on the basic heartbeat and membership propagation flow, with failure-detection and suspicion handling being actively developed.

## Why This Exists

Distributed systems need a reliable way to answer a simple but important question:

> Which nodes are currently alive?

In real-world systems, this is difficult because networks are unreliable, messages can be delayed, and nodes can crash or recover. ClusterPulse is an attempt to build and understand this problem from the ground up.

The project is inspired by the SWIM protocol, which combines randomized probing, indirect checks, and gossip-based membership dissemination.

## Features

- Simulated cluster of multiple nodes
- Periodic randomized peer probing
- `PING` / `ACK` based liveness checks
- Membership table maintained per node
- Piggybacked membership updates
- Message routing between simulated nodes
- Initial support for suspicion and alive-confirmation flow
- Work-in-progress support for dead-node detection

## Current Protocol Flow

### Basic Liveness Check

1. A node randomly selects a peer.
2. It sends a `PING` message.
3. The peer replies with an `ACK`.
4. The sender marks the peer as alive in its membership table.

```text
node-1 -- PING --> node-3
node-1 <-- ACK --- node-3
```

### Suspect / Alive Flow

The suspicion flow is currently under development.

The intended flow is:

1. A node suspects that a peer may be unreachable.
2. It asks a few other peers to indirectly check the suspected node.
3. Those peers probe the suspected node.
4. If the suspected node responds, an `ALIVE` message is forwarded back.
5. The original node clears suspicion and marks the target alive.

```text
node-1 suspects node-4

node-1 -- PING-REQ --> node-2
node-2 -- SUSPECT ---> node-4
node-4 -- ALIVE -----> node-2
node-2 -- ALIVE -----> node-1
```

## Project Structure

```text
.
├── main.go
├── membership
│   └── table.go
├── node
│   └── node.go
└── protocol
    └── protocol.go
```

### `node`

Contains the node runtime logic:

- node startup and event loop
- randomized peer probing
- message handling
- ACK responses
- suspect and alive handling

### `membership`

Contains the membership table implementation:

- alive/suspect/failed/left status tracking
- update merging
- incarnation-based conflict resolution

### `protocol`

Contains protocol-level message and status definitions.

## Getting Started

### Prerequisites

You need Go installed.

Check if Go is available:

```bash
go version
```

If Go is not installed on macOS, install it using Homebrew:

```bash
brew install go
```

### Run the Simulation

From the project root:

```bash
go run main.go
```

You should see logs similar to:

```text
[clusterpulse] cluster started with 5 nodes
[clusterpulse] [node-1] started
[clusterpulse] [node-2] started
[clusterpulse] [node-1] probing node-3
[clusterpulse] [node-3] received PING from node-1
[clusterpulse] [node-3] acknowledging node-1
[clusterpulse] [node-1] received ACK from node-3
```

## Example Output

```text
[clusterpulse] [node-1] probing node-3
[clusterpulse] [router] node-1 -> node-3 PING
[clusterpulse] [node-3] received PING from node-1
[clusterpulse] [node-3] acknowledging node-1
[clusterpulse] [router] node-3 -> node-1 ACK
[clusterpulse] [node-1] received ACK from node-3
```

## Membership States

A node can currently have one of the following statuses:

```text
ALIVE
SUSPECT
FAILED
LEFT
```

These states are maintained in each node's local membership table and exchanged through protocol updates.

## Work in Progress

This project is actively being built.

Current areas under development:

- ACK timeout handling
- triggering suspicion when a peer does not respond
- indirect probing through `PING-REQ`
- forwarding `ALIVE` confirmations
- final `DEAD` / `FAILED` marking
- stronger membership merge semantics
- better test coverage
- deterministic simulation scenarios
- failure injection in the router

## Known Limitations

- Failure detection is not fully complete yet.
- The current happy-path simulation mostly validates `PING` / `ACK`.
- Suspicion and recovery flows need more testing.
- Correlation IDs currently encode the initiator ID in a string format.
- If node IDs contain `-`, correlation ID parsing can become ambiguous.
- The simulation currently runs in-memory and does not use real network transport.

## Roadmap

- Add timeout-based suspicion
- Add router-level packet dropping for failure simulation
- Add indirect probe retries
- Add dead-node confirmation flow
- Add unit tests for membership table merging
- Add integration tests for cluster behavior
- Add CLI flags for node count and simulation duration
- Improve logs for easier protocol tracing

## Contributors

- Guhanesh T
- Ayush Kumar Rai 

## License

None

## Note

ClusterPulse is primarily an educational and experimental project. It is not production-ready and should not be used as a real cluster membership service.
