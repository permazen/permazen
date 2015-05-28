
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.parse.ParseContext;

@Command
public class RaftRemoveCommand extends AbstractCommand {

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
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final KVTransaction kvtx = session.getTransaction().getKVTransaction();
                if (!(kvtx instanceof RaftKVTransaction))
                    throw new Exception("key/value store is not Raft");
                final RaftKVTransaction raftx = (RaftKVTransaction)kvtx;
                raftx.configChange(node, null);
            }
        };
    }
}

