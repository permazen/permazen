
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.cli.cmd.AbstractCommand;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.raft.RaftKVDatabase;
import io.permazen.kv.raft.fallback.FallbackKVDatabase;

import java.util.EnumSet;

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

