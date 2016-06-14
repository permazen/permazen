
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.cmd;

import java.util.EnumSet;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.cli.cmd.AbstractCommand;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.fallback.FallbackKVDatabase;

public abstract class AbstractRaftCommand extends AbstractCommand {

    protected AbstractRaftCommand(String spec) {
        super(spec);
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    protected abstract class RaftAction implements CliSession.Action {

        @Override
        public final void run(CliSession session) throws Exception {
            final KVDatabase db = session.getKVDatabase();
            final RaftKVDatabase raftKV;
            if (db instanceof RaftKVDatabase)
                raftKV = (RaftKVDatabase)db;
            else if (db instanceof FallbackKVDatabase)
                raftKV = ((FallbackKVDatabase)db).getFallbackTarget().getRaftKVDatabase();
            else
                throw new Exception("key/value store is not Raft or Raft fallback");
            this.run(session, raftKV);
        }

        protected abstract void run(CliSession session, RaftKVDatabase db) throws Exception;
    }
}

