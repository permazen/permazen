
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import java.util.Map;

import io.permazen.cli.CliSession;
import io.permazen.kv.raft.LeaderRole;
import io.permazen.kv.raft.RaftKVDatabase;
import io.permazen.util.ParseContext;

public class RaftStepDownCommand extends AbstractRaftCommand {

    public RaftStepDownCommand() {
        super("raft-step-down");
    }

    @Override
    public String getHelpSummary() {
        return "Step down as the Raft cluster leader";
    }

    @Override
    public String getHelpDetail() {
        return "This command forces the local node, which must be the cluster leader, to step down, which (very likely)"
          + " results in another node being elected leader for the new term. If the cluster only contains the local node,"
          + " then the local node will just immediately become leader again.";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return new RaftAction() {
            @Override
            protected void run(CliSession session, RaftKVDatabase db) throws Exception {

                // Get current role, which must be leader
                final LeaderRole leader;
                try {
                    leader = (LeaderRole)db.getCurrentRole();
                } catch (ClassCastException e) {
                    throw new Exception("current role is not leader; try `raft-status' for more info");
                }

                // Step down
                session.getWriter().println("Stepping down as Raft cluster leader");
                leader.stepDown();
            }
        };
    }
}

