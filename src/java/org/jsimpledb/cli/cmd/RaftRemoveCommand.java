
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.parse.ParseContext;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class RaftRemoveCommand extends AbstractRaftCommand {

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
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String identity = (String)params.get("identity");
        return new RaftAction() {
            @Override
            protected void run(CliSession session, RaftKVTransaction tx) throws Exception {
                tx.configChange(identity, null);
            }
        };
    }
}

