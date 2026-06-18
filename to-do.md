# To Do

## Gossip Flow

- Replace one-shot `Table.Updates()` gossip with a per-node gossip queue.
- Add a `GossipItem` concept containing:
  - `protocol.Update`
  - remaining retransmit count
- Enqueue a gossip item whenever local membership changes:
  - `alive`
  - `suspect`
  - `failed`
  - `left`
- Piggyback a limited number of queued gossip updates on every outgoing message:
  - `PING`
  - `ACK`
  - `PING-REQ`
  - `SUSPECT`
  - `ALIVE`
- Decrement each gossip item's retransmit count when it is piggybacked.
- Remove a gossip item only after its retransmit count reaches zero.
- When a node receives an update and `table.Merge(update)` changes local state, enqueue that update again so it spreads further.
- Set `ObservedBy` on generated updates.
- Choose a retransmit count, initially fixed at `3` or `5`.
- Later, make retransmit count depend on cluster size, for example `gossipMultiplier * log(N)`.

## Membership Semantics

- Prevent lower-severity updates from downgrading higher-severity local state.
- Keep the status ordering explicit:
  - `alive`
  - `suspect`
  - `failed`
  - `left`
- Add proper incarnation handling.
- Increment incarnation when a node refutes suspicion about itself.
- Require a higher incarnation for a previously failed node to rejoin as alive.

## Failed Node Handling

- Stop selecting `failed` nodes in `probeRandomPeer`.
- Keep failed records as tombstones for a cleanup period.
- Optionally remove failed nodes after the cleanup period expires.
- Add a separate crashed-node simulation where the node runtime is not started, instead of only dropping router traffic.

## Verification

- Add a deterministic simulation test for:
  - `PING` timeout marks target `suspect`
  - missing indirect confirmation marks target `failed`
  - failed status spreads to all healthy nodes
  - failed status does not downgrade back to suspect or alive
- Add a final membership summary helper for any target node, not only `node-3`.
