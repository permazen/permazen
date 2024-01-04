
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import io.permazen.cli.Session;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.raft.Consistency;
import io.permazen.kv.raft.RaftKVDatabase;
import io.permazen.kv.raft.RaftKVTransaction;
import io.permazen.kv.raft.fallback.FallbackKVTransaction;
import io.permazen.kv.raft.fallback.FallbackTarget;

import java.util.Collections;
import java.util.Map;

public abstract class AbstractTransactionRaftCommand extends AbstractRaftCommand {

    protected AbstractTransactionRaftCommand(String spec) {
        super(spec);
    }

    protected abstract class RaftTransactionAction extends RaftAction
      implements Session.RetryableTransactionalAction, Session.TransactionalActionWithOptions {

        @Override
        public final void run(Session session, RaftKVDatabase db) throws Exception {

            // Run in the current thread's Raft transaction if we can (if not, we're in standalone mode)
            final KVTransaction kvt = session.getKVTransaction();
            final RaftKVTransaction raftTX;
            if (kvt instanceof RaftKVTransaction) {
                this.run(session, (RaftKVTransaction)kvt);
                return;
            }
            if (!(kvt instanceof FallbackKVTransaction))
                throw new Exception("key/value store is not Raft or Raft fallback");
            final FallbackKVTransaction fbtx = (FallbackKVTransaction)kvt;
            final KVTransaction kvt2 = fbtx.getKVTransaction();
            if (kvt2 instanceof RaftKVTransaction) {
                this.run(session, (RaftKVTransaction)kvt2);
                return;
            }

            // We're in standalone mode; create a new transaction using the preferred Raft target
            final FallbackTarget target = fbtx.getKVDatabase().getFallbackTarget();
            if (target == null)
                throw new Exception("can't find Raft fallback target");
            final RaftKVTransaction raftTx = target.getRaftKVDatabase().createTransaction();
            try {
                this.run(session, raftTx);
                raftTx.commit();
            } finally {
                raftTx.rollback();
            }
        }

        @Override
        public Map<String, ?> getTransactionOptions() {
            return Collections.singletonMap(RaftKVDatabase.OPTION_CONSISTENCY, this.getConsistency());
        }

        protected Consistency getConsistency() {
            return Consistency.LINEARIZABLE;
        }

        protected abstract void run(Session session, RaftKVTransaction tx) throws Exception;
    }
}
