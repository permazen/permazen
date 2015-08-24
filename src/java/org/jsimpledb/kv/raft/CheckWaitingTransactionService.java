
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

class CheckWaitingTransactionService extends AbstractTransactionService {

    /**
     * Constructor.
     */
    public CheckWaitingTransactionService(Role role, RaftKVTransaction tx) {
        super(role, tx, "check waiting tx#" + tx.getTxId());
    }

    @Override
    protected void doRun() {
        if (this.tx.getState().equals(TxState.COMMIT_WAITING))
            this.role.checkWaitingTransaction(this.tx);
    }
}

