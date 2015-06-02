
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.parse.ParseContext;

@Command
public class RaftRemoveCommand extends AbstractRaftCommand {

    public RaftRemoveCommand() {
        super("raft-remove node");
    }

    @Override
    public String getHelpSummary() {
        return "removes a node from a Raft clustered database";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String node = (String)params.get("node");
        return new RaftAction() {
            @Override
            protected void run(CliSession session, RaftKVTransaction tx) throws Exception {
                tx.configChange(node, null);
            }
        };
    }
}

