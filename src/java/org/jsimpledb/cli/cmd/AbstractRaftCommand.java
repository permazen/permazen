
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.raft.RaftKVTransaction;

public abstract class AbstractRaftCommand extends AbstractCommand {

    protected AbstractRaftCommand(String spec) {
        super(spec);
    }

    protected abstract class RaftAction implements CliSession.Action {

        @Override
        public final void run(CliSession session) throws Exception {
            final KVTransaction kvt = session.getTransaction().getKVTransaction();
            if (!(kvt instanceof RaftKVTransaction))
                throw new Exception("key/value store is not Raft");
            this.run(session, (RaftKVTransaction)kvt);
        }

        protected abstract void run(CliSession session, RaftKVTransaction tx) throws Exception;
    }
}

