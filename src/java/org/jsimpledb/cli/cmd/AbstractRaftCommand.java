
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.raft.RaftKVDatabase;

public abstract class AbstractRaftCommand extends AbstractCommand {

    protected AbstractRaftCommand(String spec) {
        super(spec);
    }

    protected abstract class RaftAction implements CliSession.Action {

        @Override
        public final void run(CliSession session) throws Exception {
            final KVDatabase db = session.getKVDatabase();
            if (!(db instanceof RaftKVDatabase))
                throw new Exception("key/value store is not Raft");
            this.run(session, (RaftKVDatabase)db);
        }

        protected abstract void run(CliSession session, RaftKVDatabase db) throws Exception;
    }
}

