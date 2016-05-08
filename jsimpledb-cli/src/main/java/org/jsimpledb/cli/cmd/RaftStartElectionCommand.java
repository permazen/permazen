
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.NonLeaderRole;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.util.ParseContext;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class RaftStartElectionCommand extends AbstractRaftCommand {

    public RaftStartElectionCommand() {
        super("raft-start-election");
    }

    @Override
    public String getHelpSummary() {
        return "Forces a immediate Raft election";
    }

    @Override
    public String getHelpDetail() {
        return "This command forces an immediate election timeout on the local node, which must be a follower (or a candidate)."
          + " This node will then (be very likely to) be elected for a new term, deposing the current leader.";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return new RaftAction() {
            @Override
            protected void run(CliSession session, RaftKVDatabase db) throws Exception {

                // Get current role, which must not be leader
                final NonLeaderRole role;
                try {
                    role = (NonLeaderRole)db.getCurrentRole();
                } catch (ClassCastException e) {
                    throw new Exception("current role is not follower or candidate; try `raft-status' for more info");
                }

                // Trigger an election
                session.getWriter().println("Triggering early Raft election");
                role.startElection();
            }
        };
    }
}

