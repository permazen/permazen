How to set up a Raft distributed key/value store
================================================

The Raft consensus algorithm defines a distributed consensus protocol
for maintaining a shared state machine. Each Raft node maintains a
complete copy of the state machine. Cluster nodes elect a leader
who collects and distributes updates and provides for consistent
reads. As long as as a node is part of a majority, the state machine
is fully operational.

Here's how to set up a Raft key/value store with JSimpleDB using
LevelDB as the local persistent store.

Let A, B, C, ... represent the nodes in the cluster. In this example
the identity of A is "nodeA" and the IP address of A is "A.A.A.A".

 1. Choose a directory for the Raft persistent state on each machine.
    In this example we'll use ~/.raftX for machine X.

 2. On each machine X run this command (all on one line), replacing
    "X" with the appropriate letter (A, B, C, ...):

        $ java -jar jsimpledb-cli.jar
            --kv-mode                               # Run CLI in key/value mode
            --raft-dir ~/.raftX                     # Raft persistent state dir
            --raft-identity nodeX                   # Raft node identity
            --raft-address X.X.X.X                  # Raft node IP address
            --leveldb ~/.raftX/leveldb              # Raft's local persistent store

    Running this command on each machine should show an unconfigured node:

        JSimpleDB> raft-status

    If it shows a configured node, rm -rf ~/.raftX and start over.

 3. Pick one machine (A) to be the first node in the new cluster.
    On this machine, create a new cluster and add it as the first node:

        JSimpleDB> raft-add nodeA A.A.A.A

    Now running the status command should show a single node cluster:

        JSimpleDB> raft-status

 4. Add each other node to the cluster one at a time:

        JSimpleDB> raft-add nodeB B.B.B.B
        JSimpleDB> raft-add nodeC C.C.C.C
        ...

    After adding each node, "raft-status" should include it.

    If an error occurs, double check your IP addresses.

  5. Check updated Raft status on any node at any time:

        JSimpleDB> raft-status

  6. Use "help" to learn about other Raft CLI commands, e.g.:

        JSimpleDB> help raft-step-down

