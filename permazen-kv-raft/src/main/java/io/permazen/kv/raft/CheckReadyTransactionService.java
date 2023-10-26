
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

class CheckReadyTransactionService extends AbstractTransactionService {

    /**
     * Constructor.
     */
    CheckReadyTransactionService(Role role, RaftKVTransaction tx) {
        super(role, tx, "check ready tx#" + tx.txId);
    }

    @Override
    protected void doRun() {
        if (this.tx.getState().equals(TxState.COMMIT_READY))
            this.role.checkReadyTransaction(this.tx);
    }
}
