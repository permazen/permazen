
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

class SnapshotTransaction extends Transaction {

    SnapshotTransaction(Transaction parent) {
        super(parent.db, new SnapshotKVTransaction(parent), parent.schema, parent.version);
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    @Override
    public void setRollbackOnly() {
        throw new UnsupportedOperationException("snapshot transaction");
    }

    @Override
    public void addCallback(Callback callback) {
        throw new UnsupportedOperationException("snapshot transaction");
    }
}

