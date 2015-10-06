
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.RaftKVTransaction;

public abstract class AbstractTransactionRaftCommand extends AbstractRaftCommand {

    protected AbstractTransactionRaftCommand(String spec) {
        super(spec);
    }

    protected abstract class RaftTransactionAction extends RaftAction implements CliSession.TransactionalAction {

        @Override
        public final void run(CliSession session, RaftKVDatabase db) throws Exception {
            this.run(session, (RaftKVTransaction)session.getKVTransaction());
        }

        protected abstract void run(CliSession session, RaftKVTransaction tx) throws Exception;
    }
}

