
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.parse.ParseContext;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
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
            protected void run(CliSession session, RaftKVTransaction tx) throws Exception {
                session.setRollbackOnly();
                RaftStepDownCommand.this.stepDown(session, tx.getKVDatabase());
            }
        };
    }

    private void stepDown(CliSession session, RaftKVDatabase db) throws Exception {
        final RaftKVDatabase.LeaderRole leader;
        try {
            leader = (RaftKVDatabase.LeaderRole)db.getCurrentRole();
        } catch (ClassCastException e) {
            throw new Exception("current role is not leader; try `raft-status' for more info");
        }
        session.getWriter().println("Stepping down as Raft cluster leader");
        leader.stepDown();
    }
}

