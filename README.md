# Cluster PoC

This repo contains a PoC for creating a cluster of nodes that can discover each
other and elect a leader using Serf and Raft.

## Quick Start

Build the code:

```sh
make build
```

And run multiple instances:

```sh
./node -serf-port=1001
./node -serf-port=2001 -peers=localhost:1001
./node -serf-port=3001 -peers=localhost:2001,localhost:1001
```

Note that peers can contain any number of addresses separated by commas. At least
one has to be provided for the node to join an existing cluster.

Once a cluster is created you can send messages to the leader by typing directly
to the leader's stdin and you should see all nodes echoing the message. Typing
a number will result in the nodes adding the number to their internal value and
keeping track of the sum.