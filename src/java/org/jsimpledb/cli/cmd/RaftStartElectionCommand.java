
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.parse.ParseContext;

@Command
public class RaftStartElectionCommand extends AbstractRaftCommand {

    public RaftStartElectionCommand() {
        super("raft-start-election");
    }

    @Override
    public String getHelpSummary() {
        return "force an early Raft election (followers only)";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return new RaftAction() {
            @Override
            protected void run(CliSession session, RaftKVTransaction tx) throws Exception {
                RaftStartElectionCommand.this.startElection(session, tx.getKVDatabase());
            }
        };
    }

    private void startElection(CliSession session, RaftKVDatabase db) throws Exception {
        final RaftKVDatabase.NonLeaderRole role;
        try {
            role = (RaftKVDatabase.NonLeaderRole)db.getCurrentRole();
        } catch (ClassCastException e) {
            throw new Exception("current role is not follower or candidate; try `raft-status' for more info");
        }
        session.getWriter().println("Triggering early Raft election");
        role.startElection();
    }
}

