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

If the nodes are running on the same host, you need to specify different
TCP ports, either by appending them to the address like `127.0.0.1:1001'
or by using the `--raft-port' flag.

 1. Choose a directory for the Raft persistent state on each machine.
    In this example we'll use ~/.raftX for machine X.

 2. On each machine X run this command (all on one line), replacing
    "X" with the appropriate letter (A, B, C, ...):

        $ java -jar jsimpledb-cli.jar
            --kv-mode                               # Run CLI in key/value mode
            --raft-dir ~/.raftX                     # Raft persistent state dir
            --raft-identity nodeX                   # Raft node identity
            --raft-address X.X.X.X                  # Raft node IP address
            --leveldb ~/.raftX/kvstore              # Raft's local persistent store

    Enter the "raft-status" command into the CLI running on each machine.
    It should show that the Raft cluster is in the unconfigured state:

        KeyValue> raft-status
        ...
        Cluster configuration   : Unconfigured

    If it shows a configured node, rm -rf ~/.raftX and start over.

 3. Pick one machine (A) to be the first node in the new cluster. On this
    machine, create the new cluster and make it add itself as the first node:

        KeyValue> raft-add nodeA A.A.A.A

    Now running the status command should show a single node cluster:

        KeyValue> raft-status
        ...
        Cluster configuration:

          Identity         Address
          --------         -------
        * "nodeA"          A.A.A.A

 4. Add each other node to the cluster one at a time, by entering
    these commands into nodeA (or any other already-added node):

        KeyValue> raft-add nodeB B.B.B.B
        KeyValue> raft-add nodeC C.C.C.C
        ...

    After adding each node, "raft-status" should show it as a
    member of the cluster.

    If an error occurs, double check your IP addresses and/or TCP ports.

  5. Check updated Raft status on any node at any time:

        KeyValue> raft-status

  6. Use "help" to learn about other Raft CLI commands, e.g.:

        KeyValue> help raft-step-down

  7. Commit a change to the key/value database and see it appear
     on every other node in the cluster:

     On node #1:

        KeyValue> kvput 1234 5678

     On node #2:

        KeyValue> kvget 1234
        5678
        KeyValue> kvremove 1234

     On node #1:

        KeyValue> kvget 1234
        null

  8. Disconnect any minority of nodes and verify that the remaining majority
     is still fully functiona, can commit transactions, etc.

