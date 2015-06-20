
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import org.jsimpledb.kv.KVTransactionException;

abstract class AbstractTransactionService extends Service {

    protected final RaftKVTransaction tx;

    /**
     * Constructor.
     */
    public AbstractTransactionService(Role role, RaftKVTransaction tx, String desc) {
        super(role, desc);
        assert tx != null;
        this.tx = tx;
    }

    @Override
    public final void run() {
        try {
            this.doRun();
        } catch (KVTransactionException e) {
            this.role.raft.fail(tx, e);
        } catch (Exception e) {
            this.role.raft.fail(tx, new KVTransactionException(tx, e));
        }
    }

    protected abstract void doRun();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final AbstractTransactionService that = (AbstractTransactionService)obj;
        return this.tx.equals(that.tx);
    }

    @Override
    public int hashCode() {
        return this.tx.hashCode();
    }
}

