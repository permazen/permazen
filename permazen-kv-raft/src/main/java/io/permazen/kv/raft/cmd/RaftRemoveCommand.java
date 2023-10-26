
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import io.permazen.cli.Session;
import io.permazen.kv.raft.RaftKVTransaction;

import java.util.Map;

public class RaftRemoveCommand extends AbstractTransactionRaftCommand {

    public RaftRemoveCommand() {
        super("raft-remove identity");
    }

    @Override
    public String getHelpSummary() {
        return "Removes a node from the Raft cluster";
    }

    @Override
    public String getHelpDetail() {
        return "This command removes the specified node from the cluster. This command may be run from any cluster node."
          + " For the removed node, the behavior is as follows: leaders wait until the change has been committed to the"
          + " cluster, and then step down; followers simply disable their election timers (until added back to the cluster).";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        final String identity = (String)params.get("identity");
        return new RaftTransactionAction() {

            @Override
            protected void run(Session session, RaftKVTransaction tx) throws Exception {
                tx.configChange(identity, null);
            }
        };
    }
}
