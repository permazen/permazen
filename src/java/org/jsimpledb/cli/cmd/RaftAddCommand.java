
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.Consistency;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.parse.ParseContext;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class RaftAddCommand extends AbstractTransactionRaftCommand {

    public RaftAddCommand() {
        super("raft-add identity address");
    }

    @Override
    public String getHelpSummary() {
        return "Adds a node to the Raft cluster";
    }

    @Override
    public String getHelpDetail() {
        return "This command adds a new node to the Raft cluster. The new node's identity and address (in the form"
          + " IPAddress[:port]) must be provided. This command can also be used to change the known address of a node"
          + " already in the cluster as recorded in the cluster configuration (the node must also be restarted with the"
          + " new address if its address actually needs to change). This command may be run from any cluster node."
          + "\n\nThis command is also used to initialize an unconfigured node. This creates a new cluster with the"
          + " local node as the first and only member. In this usage, the local node's identity and address must be given;"
          + " for unconfigured nodes, this is the only allowed usage of this command.";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String identity = (String)params.get("identity");
        final String address = (String)params.get("address");
        return new RaftTransactionAction() {
            @Override
            protected void run(CliSession session, RaftKVTransaction tx) throws Exception {
                tx.setConsistency(Consistency.UNCOMMITTED);
                tx.configChange(identity, address);
            }
        };
    }
}

