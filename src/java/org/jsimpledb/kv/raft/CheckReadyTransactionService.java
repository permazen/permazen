
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

class CheckReadyTransactionService extends AbstractTransactionService {

    /**
     * Constructor.
     */
    public CheckReadyTransactionService(Role role, RaftKVTransaction tx) {
        super(role, tx, "check ready tx#" + tx.getTxId());
    }

    @Override
    protected void doRun() {
        if (this.tx.getState().equals(TxState.COMMIT_READY))
            this.role.checkReadyTransaction(this.tx);
    }
}

