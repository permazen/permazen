
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

class RebaseTransactionService extends AbstractTransactionService {

    /**
     * Constructor.
     */
    RebaseTransactionService(Role role, RaftKVTransaction tx) {
        super(role, tx, "rebase tx#" + tx.getTxId());
    }

    @Override
    protected void doRun() {
        switch (this.tx.getState()) {
        case EXECUTING:
        case COMMIT_READY:
            this.role.rebaseTransaction(this.tx);
            break;
        default:
            break;
        }
    }
}

