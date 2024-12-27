
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * A toy key/value database that only allows one transaction at a time.
 */
public class TestKVDatabase implements KVDatabase {

    private final MemoryKVStore kv = new MemoryKVStore();

    private Transaction tx;

    public KVStore getKVStore() {
        return this.kv;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public KVTransaction createTransaction(Map<String, ?> options) {
        return this.createTransaction();
    }

    @Override
    public synchronized KVTransaction createTransaction() {
        if (this.tx != null)
            throw new IllegalStateException("there is already a transaction outstanding");
        this.tx = new Transaction();
        return this.tx;
    }

    private synchronized void commit(Transaction tx) {
        if (tx != this.tx)
            throw new StaleKVTransactionException(tx);
        synchronized (tx.view) {
            tx.view.setReadOnly();
            tx.view.getWrites().applyTo(this.kv);
        }
        this.tx = null;
    }

    private synchronized void rollback(Transaction tx) {
        if (tx == this.tx)
            this.tx = null;
    }

// Transaction

    public class Transaction extends ForwardingKVStore implements KVTransaction {

        private final MutableView view;

        Transaction() {
            this.view = new MutableView(TestKVDatabase.this.kv, false);
        }

        @Override
        public void commit() {
            TestKVDatabase.this.commit(this);
        }

        @Override
        public KVDatabase getKVDatabase() {
            return TestKVDatabase.this;
        }

        @Override
        public boolean isReadOnly() {
            return this.view.isReadOnly();
        }

        @Override
        public CloseableKVStore readOnlySnapshot() {
            final MemoryKVStore copy = new MemoryKVStore();
            try (CloseableIterator<KVPair> i = this.getRange(KeyRange.FULL)) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();
                    copy.put(pair.getKey(), pair.getValue());
                }
            }
            return new CloseableForwardingKVStore(copy, null);
        }

        @Override
        public void rollback() {
            TestKVDatabase.this.rollback(this);
        }

        @Override
        public void setReadOnly(boolean readOnly) {
            if (readOnly)
                this.view.setReadOnly();
            else if (this.view.isReadOnly())
                throw new IllegalStateException();
        }

        @Override
        public void setTimeout(long timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<Void> watchKey(ByteData key) {
            throw new UnsupportedOperationException();
        }

    // ForwardingKVStore

        @Override
        protected KVStore delegate() {
            return this.view;
        }
    }
}
