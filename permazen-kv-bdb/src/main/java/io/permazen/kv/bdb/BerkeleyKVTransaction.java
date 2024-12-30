
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.bdb;

import com.google.common.base.Preconditions;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.CloseableTracker;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle Berkeley DB Java Edition {@link KVTransaction} implementation.
 */
public class BerkeleyKVTransaction extends AbstractKVStore implements KVTransaction, Closeable {

// Note: locking order: (1) BerkeleyKVTransaction, (2) BerkeleyKVDatabase

    private static final ByteData MIN_KEY = ByteData.empty();               // minimum possible key (inclusive)
    private static final ByteData MAX_KEY = ByteData.of(0xff);              // maximum possible key (exclusive)

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final BerkeleyKVDatabase store;
    private final Transaction tx;
    private final CursorConfig cursorConfig = new CursorConfig().setNonSticky(true);
    private final CloseableTracker cursorTracker = new CloseableTracker();  // unclosed Cursors are tracked here

    private boolean readOnly;
    private boolean closed;

    /**
     * Constructor.
     */
    BerkeleyKVTransaction(BerkeleyKVDatabase store, Transaction tx) {
        assert store != null;
        assert tx != null;
        this.store = store;
        this.tx = tx;
    }

// KVTransaction

    @Override
    public BerkeleyKVDatabase getKVDatabase() {
        return this.store;
    }

    /**
     * Get the underlying {@link Transaction} associated with this instance.
     *
     * @return the associated transaction
     */
    public Transaction getTransaction() {
        return this.tx;
    }

    @Override
    public void setTimeout(long timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.tx.setLockTimeout(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Watch a key to monitor for changes in its value.
     *
     * <p>
     * Oracle Berkeley DB Java Edition does not support key watches;
     * the implementation in {@link BerkeleyKVDatabase} always throws {@link UnsupportedOperationException}.
     *
     * @param key {@inheritDoc}
     * @return {@inheritDoc}
     * @throws UnsupportedOperationException always
     */
    @Override
    public Future<Void> watchKey(ByteData key) {
        throw new UnsupportedOperationException();
    }

// KVStore

    @Override
    public synchronized ByteData get(ByteData key) {
        if (this.closed)
            throw new StaleKVTransactionException(this);
        this.cursorTracker.poll();
        Preconditions.checkArgument(!key.startsWith(ByteData.of(0xff)), "key starts with 0xff");
        final DatabaseEntry value = new DatabaseEntry();
        try {
            final OperationStatus status = this.store.getDatabase().get(this.tx, new DatabaseEntry(key.toByteArray()), value, null);
            switch (status) {
            case SUCCESS:
                return ByteData.of(value.getData());
            case NOTFOUND:
                return null;
            default:
                throw this.weirdStatus(status, "get");
            }
        } catch (DatabaseException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        try (CursorIterator i = this.getRange(minKey, maxKey, false)) {
            return i.hasNext() ? i.next() : null;
        }
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        try (CursorIterator i = this.getRange(minKey, maxKey, true)) {
            return i.hasNext() ? i.next() : null;
        }
    }

    @Override
    public synchronized CursorIterator getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        if (this.closed)
            throw new StaleKVTransactionException(this);
        this.cursorTracker.poll();
        final Cursor cursor;
        try {
            cursor = this.store.getDatabase().openCursor(this.tx, this.cursorConfig);
        } catch (DatabaseException e) {
            throw this.wrapException(e);
        }
        return new CursorIterator(cursor, minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(ByteData key, ByteData value) {
        if (this.closed)
            throw new StaleKVTransactionException(this);
        this.cursorTracker.poll();
        Preconditions.checkArgument(!key.startsWith(ByteData.of(0xff)), "key starts with 0xff");
        try {
            this.store.getDatabase().put(this.tx, new DatabaseEntry(key.toByteArray()), new DatabaseEntry(value.toByteArray()));
        } catch (DatabaseException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public synchronized void remove(ByteData key) {
        if (this.closed)
            throw new StaleKVTransactionException(this);
        this.cursorTracker.poll();
        Preconditions.checkArgument(!key.startsWith(ByteData.of(0xff)), "key starts with 0xff");
        try {
            this.store.getDatabase().delete(this.tx, new DatabaseEntry(key.toByteArray()));
        } catch (DatabaseException e) {
            throw this.wrapException(e);
        }
    }

// More KVTransaction

    @Override
    public synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public synchronized void commit() {
        if (this.closed)
            throw new StaleKVTransactionException(this);
        this.close();
        try {
            if (this.readOnly)
                this.tx.abort();
            else
                this.tx.commit();
        } catch (DatabaseException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public synchronized void rollback() {
        if (this.closed)
            return;
        this.close();
        try {
            this.tx.abort();
        } catch (DatabaseException e) {
            throw this.wrapException(e);
        }
    }

    @Override
    public CloseableKVStore readOnlySnapshot() {
        throw new UnsupportedOperationException();
    }

// Closeable

    @Override
    public synchronized void close() {

        // Already closed?
        if (this.closed)
            return;
        this.closed = true;

        // Close all unclosed cursors
        this.cursorTracker.close();

        // Remove this transaction from database
        this.store.removeTransaction(this);
    }

// Object

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            if (!this.closed)
               this.log.warn(this + " leaked without commit() or rollback()");
            this.close();
        } finally {
            super.finalize();
        }
    }

// Other methods

    /**
     * Wrap the given {@link DatabaseException} in the appropriate {@link KVTransactionException}.
     *
     * @param e Berkeley database exception
     * @return appropriate {@link KVTransactionException} with chained exception {@code e}
     * @throws NullPointerException if {@code e} is null
     */
    public KVTransactionException wrapException(DatabaseException e) {
        if (e instanceof LockConflictException)
            return new RetryKVTransactionException(this, e);
        return new KVTransactionException(this, e);
    }

    private KVTransactionException weirdStatus(OperationStatus status, String methodName) {
        return new KVTransactionException(BerkeleyKVTransaction.this,
          String.format("unexpected status %s from %s()", status, methodName));
    }

// CursorIterator

    /**
     * {@link java.util.Iterator} implementation used by {@link BerkeleyKVTransaction#getRange BerkeleyKVTransaction.getRange()}.
     *
     * <p>
     * Instances implement {@link Closeable}.
     */
    public final class CursorIterator implements CloseableIterator<KVPair> {

        private final Cursor cursor;
        private final ByteData minKey;
        private final ByteData maxKey;
        private final boolean reverse;

        private KVPair nextPair;
        private ByteData removeKey;
        private boolean canRemoveWithCursor;
        private boolean completed;
        private boolean initialized;

        CursorIterator(Cursor cursor, ByteData minKey, ByteData maxKey, boolean reverse) {
            assert Thread.holdsLock(BerkeleyKVTransaction.this);
            assert cursor != null;
            Preconditions.checkArgument(minKey == null || maxKey == null || minKey.compareTo(maxKey) <= 0, "minKey > maxKey");
            this.cursor = cursor;
            this.minKey = minKey != null ? ByteUtil.min(minKey, BerkeleyKVTransaction.MAX_KEY) : BerkeleyKVTransaction.MIN_KEY;
            this.maxKey = maxKey != null ? ByteUtil.min(maxKey, BerkeleyKVTransaction.MAX_KEY) : BerkeleyKVTransaction.MAX_KEY;
            this.reverse = reverse;

            // Make sure we eventually close the BDB cursor
            BerkeleyKVTransaction.this.cursorTracker.add(this, this.cursor);
        }

    // Iterator

        @Override
        public synchronized boolean hasNext() {
            return this.findNext();
        }

        @Override
        public synchronized KVPair next() {
            if (!this.findNext())
                throw new NoSuchElementException();
            assert this.nextPair != null;
            final KVPair result = this.nextPair;
            this.removeKey = result.getKey();
            this.canRemoveWithCursor = true;
            this.nextPair = null;
            return result;
        }

        @Override
        public synchronized void remove() {
            if (BerkeleyKVTransaction.this.closed)
                throw new StaleKVTransactionException(BerkeleyKVTransaction.this);
            if (this.removeKey == null)
                throw new IllegalStateException();
            try {
                final OperationStatus status = this.canRemoveWithCursor ? this.cursor.delete() :
                  this.cursor.getDatabase().delete(BerkeleyKVTransaction.this.tx, new DatabaseEntry(this.removeKey.toByteArray()));
                switch (status) {
                case SUCCESS:
                case KEYEMPTY:
                    break;
                default:
                    throw BerkeleyKVTransaction.this.weirdStatus(status, "delete");
                }
            } catch (DatabaseException e) {
                throw BerkeleyKVTransaction.this.wrapException(e);
            }
            this.removeKey = null;
        }

    // Closeable

        @Override
        public void close() {
            try {
                this.cursor.close();
            } catch (Throwable e) {
                BerkeleyKVTransaction.this.log.debug("caught exception closing iterator cursor (ignoring)", e);
            }
        }

    // Internal methods

        @SuppressWarnings("fallthrough")
        private /*synchronized*/ boolean findNext() {
            assert Thread.holdsLock(this);
            if (BerkeleyKVTransaction.this.closed)
                throw new StaleKVTransactionException(BerkeleyKVTransaction.this);
            if (!this.initialized)
                this.initialize();
            assert this.initialized;
            if (this.nextPair != null)
                return true;
            if (this.completed)
                return false;
            final DatabaseEntry key = new DatabaseEntry();
            final DatabaseEntry value = new DatabaseEntry();
            this.canRemoveWithCursor = false;
            try {
                final OperationStatus status = this.reverse ?
                  this.cursor.getPrev(key, value, null) : this.cursor.getNext(key, value, null);
                switch (status) {
                case SUCCESS:
                    final ByteData keyData = ByteData.of(key.getData());
                    if (this.reverse ? keyData.compareTo(this.minKey) >= 0 : keyData.compareTo(this.maxKey) < 0) {
                        this.nextPair = new KVPair(keyData, ByteData.of(value.getData()));
                        return true;
                    }
                    // FALLTHROUGH
                case NOTFOUND:
                    this.completed = true;
                    return false;
                default:
                    throw BerkeleyKVTransaction.this.weirdStatus(status, this.reverse ? "getPrev" : "getNext");
                }
            } catch (DatabaseException e) {
                throw BerkeleyKVTransaction.this.wrapException(e);
            }
        }

        // We initialize on demand from findNext()
        @SuppressWarnings("fallthrough")
        private /*synchronized*/ void initialize() {
            assert Thread.holdsLock(this);
            assert !this.initialized;
            assert !this.completed;
            assert this.nextPair == null;
            if (reverse) {

                // Search for maxKey; thereafter, a call to getPrev() will return the first element in the reverse iteration.
                // We don't care whether maxKey is found or not, we are just positioning the cursor.
                try {
                    final OperationStatus status = this.cursor.getSearchKey(
                      new DatabaseEntry(this.maxKey.toByteArray()), new DatabaseEntry(), null);
                    switch (status) {
                    case SUCCESS:
                    case NOTFOUND:
                        break;
                    default:
                        throw BerkeleyKVTransaction.this.weirdStatus(status, "getSearchKey");
                    }
                } catch (DatabaseException e) {
                    throw BerkeleyKVTransaction.this.wrapException(e);
                }
            } else if (!this.minKey.isEmpty()) {    // if minKey has zero length, no need to initialize (getNext() will just work)
                final DatabaseEntry key = new DatabaseEntry(this.minKey.toByteArray());
                final DatabaseEntry value = new DatabaseEntry();
                try {
                    final OperationStatus status = this.cursor.getSearchKeyRange(key, value, null);
                    switch (status) {
                    case SUCCESS:
                        final ByteData keyData = ByteData.of(key.getData());
                        if (keyData.compareTo(this.maxKey) < 0) {
                            this.nextPair = new KVPair(keyData, ByteData.of(value.getData()));
                            break;
                        }
                        // FALLTHROUGH
                    case NOTFOUND:
                        this.completed = true;
                        break;
                    default:
                        throw BerkeleyKVTransaction.this.weirdStatus(status, "getSearchKeyRange");
                    }
                } catch (DatabaseException e) {
                    throw BerkeleyKVTransaction.this.wrapException(e);
                }
            }
            this.initialized = true;
        }
    }
}
