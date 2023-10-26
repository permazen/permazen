
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import io.permazen.cli.Session;
import io.permazen.kv.raft.NonLeaderRole;
import io.permazen.kv.raft.RaftKVDatabase;

import java.util.Map;

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
          + " If follower probing is disabled, this node will then (be very likely to) be elected for a new term,"
          + " deposing the current leader.";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        return new RaftAction() {
            @Override
            protected void run(Session session, RaftKVDatabase db) throws Exception {

                // Get current role, which must not be leader
                final NonLeaderRole role;
                try {
                    role = (NonLeaderRole)db.getCurrentRole();
                } catch (ClassCastException e) {
                    throw new Exception("current role is not follower or candidate; try `raft-status' for more info");
                }

                // Trigger an election
                session.getOutput().println("Triggering early Raft election");
                role.startElection();
            }
        };
    }
}
