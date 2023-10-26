
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

class CheckWaitingTransactionService extends AbstractTransactionService {

    /**
     * Constructor.
     */
    CheckWaitingTransactionService(Role role, RaftKVTransaction tx) {
        super(role, tx, "check waiting tx#" + tx.txId);
    }

    @Override
    protected void doRun() {
        if (this.tx.getState().equals(TxState.COMMIT_WAITING))
            this.role.checkWaitingTransaction(this.tx);
    }
}
