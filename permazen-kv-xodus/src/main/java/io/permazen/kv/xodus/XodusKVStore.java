
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Straightforward {@link KVStore} view of a Xodus {@link Store} viewed within an open {@link Transaction}.
 *
 * <p>
 * Instances must be {@link #close}'d when no longer needed to avoid leaking resources.
 */
public class XodusKVStore extends AbstractKVStore implements CloseableKVStore {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final TransactionType txType;
    private final Transaction tx;
    private final Store store;
    private final AtomicBoolean closed = new AtomicBoolean();

// Constructors

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to: {@code SnapshotXodusKVStore(env, storeName, true, txType)}.
     *
     * @param env Xodus environment
     * @param storeName Xodus store name
     * @param txType transaction type
     * @throws IllegalArgumentException if {@code env}, {@code storeName}, or {@code txType} is null
     */
    public XodusKVStore(Environment env, String storeName, TransactionType txType) {
        this(env, storeName, true, txType);
    }

    /**
     * Constructor.
     *
     * <p>
     * The specified {@link Store} will be created if necessary.
     *
     * @param env Xodus environment
     * @param storeName Xodus store name
     * @param keyPrefixing if creating a new store, true to create the store with key prefixing
     *  ({@link StoreConfig#WITHOUT_DUPLICATES_WITH_PREFIXING}), or false to create the store without key prefixing
     *  ({@link StoreConfig#WITHOUT_DUPLICATES})
     * @param txType transaction type
     * @throws IllegalArgumentException if {@code env}, {@code storeName}, or {@code txType} is null
     */
    @SuppressWarnings("this-escape")
    public XodusKVStore(Environment env, String storeName, boolean keyPrefixing, TransactionType txType) {
        Preconditions.checkArgument(env != null, "null env");
        Preconditions.checkArgument(storeName != null, "null storeName");
        Preconditions.checkArgument(txType != null, "null txType");
        this.txType = txType;
        this.tx = this.txType.apply(env);
        boolean success = false;
        try {
            this.store = env.openStore(storeName,
              keyPrefixing ? StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING : StoreConfig.WITHOUT_DUPLICATES, this.tx);
            success = true;
        } finally {
            if (!success)
                this.tx.abort();
        }
        if (this.log.isTraceEnabled())
            this.log.trace("created {}", this);
    }

    // Used by readOnlySnapshot()
    private XodusKVStore(TransactionType txType, Transaction tx, Store store) {
        Preconditions.checkArgument(txType != null, "null txType");
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(store != null, "null store");
        this.txType = txType;
        this.tx = tx;
        this.store = store;
    }

    /**
     * Get the {@link TransactionType} associated with this instance.
     *
     * @return associated transaction type
     */
    public TransactionType getTransactionType() {
        return this.txType;
    }

    /**
     * Get the {@link Transaction} associated with this instance.
     *
     * @return associated transaction
     */
    public Transaction getTransaction() {
        return this.tx;
    }

    /**
     * Get the {@link Store} associated with this instance.
     *
     * @return associated store
     */
    public Store getStore() {
        return this.store;
    }

    /**
     * Determine if this instance is closed.
     *
     * @return true if closed, false if still open
     */
    public boolean isClosed() {
        return this.closed.get();
    }

    /**
     * Return a read-only snapshot containing the same data as this instance.
     *
     * <p>
     * Though based on the same underlying data, the returned instance and this instance retain no references to each other.
     *
     * @return read-only snapshot
     * @throws IllegalStateException if this instance is closed
     */
    public XodusKVStore readOnlySnapshot() {
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        return new XodusKVStore(TransactionType.READ_ONLY, this.tx.getReadonlySnapshot(), this.store);
    }

// KVStore

    @Override
    public ByteData get(ByteData key) {
        key.getClass();
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        final ByteIterable bytes = this.store.get(this.tx, Util.wrap(key));
        return bytes != null ? Util.unwrap(bytes) : null;
    }

    @Override
    public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        try (Cursor cursor = this.store.openCursor(this.tx)) {
            final boolean found = minKey != null && !minKey.isEmpty() ?
              cursor.getSearchKeyRange(Util.wrap(minKey)) != null : cursor.getNext();
            if (!found)
                return null;
            final ByteData key = Util.unwrap(cursor.getKey());
            if (maxKey != null && key.compareTo(maxKey) >= 0)
                return null;
            return new KVPair(key, Util.unwrap(cursor.getValue()));
        }
    }

    @Override
    public KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        try (Cursor cursor = this.store.openCursor(this.tx)) {
            // It's possible somebody could be simultaneously inserting keys just after maxKey, in which case we
            // could be tricked into returning a key > maxKey. This is unlikely, but make sure it can't affect us.
            while (true) {
                if (maxKey != null)
                    cursor.getSearchKeyRange(Util.wrap(maxKey));
                if (!cursor.getPrev())
                    return null;
                final ByteData key = Util.unwrap(cursor.getKey());
                if (maxKey != null && key.compareTo(maxKey) >= 0)
                    continue;
                if (minKey != null && key.compareTo(minKey) < 0)
                    return null;
                return new KVPair(key, Util.unwrap(cursor.getValue()));
            }
        }
    }

    // Note Xodus closes all associated Cursors when a Transaction is closed, so we don't need to track the returned iterators
    @Override
    public CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        return new XodusIter(this.store.openCursor(this.tx), minKey, maxKey, reverse);
    }

    @Override
    public void put(ByteData key, ByteData value) {
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        Preconditions.checkState(!this.txType.isReadOnly(), "read-only transaction");
        this.store.put(this.tx, Util.wrap(key), Util.wrap(value));
    }

    @Override
    public void remove(ByteData key) {
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        Preconditions.checkState(!this.txType.isReadOnly(), "read-only transaction");
        this.store.delete(this.tx, Util.wrap(key));
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        Preconditions.checkState(!this.closed.get(), "transaction closed");
        Preconditions.checkState(!this.txType.isReadOnly(), "read-only transaction");
        try (Cursor cursor = this.store.openCursor(this.tx)) {
            boolean found = minKey != null && !minKey.isEmpty() ?
              cursor.getSearchKeyRange(Util.wrap(minKey)) != null : cursor.getNext();
            while (found) {
                if (maxKey != null && Util.unwrap(cursor.getKey()).compareTo(maxKey) >= 0)
                    break;
                cursor.deleteCurrent();
                found = cursor.getNext();
            }
        }
    }

// Object

    /**
     * Finalize this instance. Invokes {@link #close} to close any unclosed iterators.
     */
    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            if (!this.closed.get()) {
                this.log.warn(this + " leaked without invoking close()");
                this.close();
            }
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[store=\"" + this.store.getName() + "\""
          + ",type=" + this.txType
          + ",tx=" + this.tx
          + "]";
    }

// Closeable

    /**
     * Close this instance, discarding any changes.
     *
     * <p>
     * This closes the underlying {@link Transaction} and any unclosed iterators returned from {@link #getRange getRange()}.
     * This method just invokes {@link #close(boolean) close(false)}.
     */
    @Override
    public void close() {
        this.close(false);
    }

    /**
     * Close this instance, optionally comitting any changes.
     *
     * <p>
     * This closes or commits the underlying {@link Transaction}, and closes any unclosed iterators returned from
     * {@link #getRange getRange()}. This instance will end up being closed even if commit fails.
     *
     * @param commit true to commit changes (if any)
     * @return true if already closed or successfully closed/commited, false if {@code commit} is true but the commit fails
     */
    public boolean close(boolean commit) {
        if (!this.closed.compareAndSet(false, true))
            return true;
        if (this.log.isTraceEnabled())
            this.log.trace("closing {}", this);
        if (commit) {
            if (this.tx.commit())
                return true;
            this.tx.abort();
            return false;
        }
        this.tx.abort();
        return true;
    }

// XodusIter

    final class XodusIter implements CloseableIterator<KVPair> {

        private final Cursor cursor;
        private final ByteData minKey;
        private final ByteData maxKey;
        private final boolean reverse;

        private KVPair next;                // the next key/value pair to return, if known
        private ByteData removeKey;         // the key to delete if remove() is invoked
        private boolean removable;          // if true, we can use cursor.deleteCurrent() to delete it
        private boolean finished;           // iteration has completed
        private boolean closed;

        private XodusIter(Cursor cursor, ByteData minKey, ByteData maxKey, boolean reverse) {

            // Sanity checks
            Preconditions.checkArgument(minKey == null || maxKey == null || minKey.compareTo(maxKey) <= 0,
              "minKey > maxKey");

            // Initialize
            if (minKey != null && minKey.isEmpty())
                minKey = null;
            this.cursor = cursor;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.reverse = reverse;
            if (XodusKVStore.this.log.isTraceEnabled())
                XodusKVStore.this.log.trace("created {}", this);
            if (this.reverse) {
                if (this.maxKey != null) {
                    final boolean found = this.cursor.getSearchKeyRange(Util.wrap(this.maxKey)) != null;
                    assert !found || Util.unwrap(this.cursor.getKey()).compareTo(this.maxKey) >= 0 :
                      "cusor.getSearchKeyRange() returned " + ByteUtil.toString(Util.unwrap(this.cursor.getKey()))
                      + " < " + ByteUtil.toString(this.maxKey);
                    if (XodusKVStore.this.log.isTraceEnabled())
                        XodusKVStore.this.log.trace("initial seek to {} -> {}", ByteUtil.toString(this.maxKey), found);
                }
            } else if (this.minKey != null) {
                final boolean found = this.cursor.getSearchKeyRange(Util.wrap(this.minKey)) != null;
                if (XodusKVStore.this.log.isTraceEnabled())
                    XodusKVStore.this.log.trace("initial seek to {} -> {}", ByteUtil.toString(this.minKey), found);
                if (found) {
                    final ByteData key = Util.unwrap(cursor.getKey());
                    assert key.compareTo(this.minKey) >= 0 : "cusor.getSearchKeyRange() returned "
                      + ByteUtil.toString(key) + " < " + ByteUtil.toString(this.minKey);
                    if (maxKey != null && key.compareTo(maxKey) >= 0)
                        this.finished = true;
                    else
                        this.next = new KVPair(key, Util.unwrap(cursor.getValue()));
                } else {
                    if (XodusKVStore.this.log.isTraceEnabled())
                        XodusKVStore.this.log.trace("initial seek failed -> DONE");
                    this.finished = true;
                }
            }
        }

    // Iterator

        @Override
        public synchronized boolean hasNext() {
            Preconditions.checkState(!this.closed, "iterator closed");
            return this.next != null || this.findNext();
        }

        @Override
        public synchronized KVPair next() {
            Preconditions.checkState(!this.closed, "iterator closed");
            if (this.next == null && !this.findNext())
                throw new NoSuchElementException();
            assert this.next != null;
            final KVPair pair = this.next;
            this.removeKey = pair.getKey();
            this.removable = true;
            this.next = null;
            return pair;
        }

        @Override
        public synchronized void remove() {
            Preconditions.checkState(!this.closed, "iterator closed");
            Preconditions.checkState(this.removeKey != null);
            Preconditions.checkState(!XodusKVStore.this.txType.isReadOnly(), "read-only transaction");
            if (XodusKVStore.this.log.isTraceEnabled())
                XodusKVStore.this.log.trace("remove {}", ByteUtil.toString(this.removeKey));
            if (this.removable)
                this.cursor.deleteCurrent();
            else
                XodusKVStore.this.remove(this.removeKey);
            this.removeKey = null;
            this.removable = false;
        }

        private boolean findNext() {

            // Sanity check
            assert Thread.holdsLock(this);
            assert this.next == null;
            if (this.finished)
                return false;

            // Advance Xodus cursor
            this.removable = false;                             // this.remove() can no longer use cursor.deleteCurrent()
            ByteData key;
            if (this.reverse) {
                while (true) {

                    // Any more keys?
                    if (!this.cursor.getPrev()) {
                        if (XodusKVStore.this.log.isTraceEnabled())
                            XodusKVStore.this.log.trace("seek previous -> DONE");
                        this.finished = true;
                        return false;
                    }
                    key = Util.unwrap(this.cursor.getKey());

                    // It's possible somebody could be simultaneously inserting keys just after maxKey, in which case we
                    // could be tricked into returning a key > maxKey. This is unlikely, but make sure it can't affect us.
                    if (this.maxKey != null && key.compareTo(this.maxKey) >= 0) {
                        if (XodusKVStore.this.log.isTraceEnabled()) {
                            XodusKVStore.this.log.trace("seek previous -> skip over "
                              + ByteUtil.toString(key) + " >= " + ByteUtil.toString(this.maxKey));
                        }
                        this.cursor.getSearchKeyRange(Util.wrap(this.maxKey));          // recalibrate and try again
                        continue;
                    }

                    // Check lower bound
                    if (this.minKey != null && key.compareTo(this.minKey) < 0) {
                        if (XodusKVStore.this.log.isTraceEnabled()) {
                            XodusKVStore.this.log.trace("seek previous -> "
                              + ByteUtil.toString(key) + " < bound " + ByteUtil.toString(this.minKey) + " -> DONE");
                        }
                        this.finished = true;
                        return false;
                    }
                    break;
                }
            } else {

                // Any more keys?
                if (!this.cursor.getNext()) {
                    if (XodusKVStore.this.log.isTraceEnabled())
                        XodusKVStore.this.log.trace("seek next -> DONE");
                    this.finished = true;
                    return false;
                }
                key = Util.unwrap(this.cursor.getKey());

                // Check lower bound
                assert this.minKey == null || key.compareTo(this.minKey) >= 0 :
                  "cusor.getNext() returned " + ByteUtil.toString(key) + " < " + ByteUtil.toString(this.minKey);

                // Check upper bound
                if (this.maxKey != null && key.compareTo(this.maxKey) >= 0) {
                    if (XodusKVStore.this.log.isTraceEnabled()) {
                        XodusKVStore.this.log.trace("seek next -> "
                          + ByteUtil.toString(key) + " >= bound " + ByteUtil.toString(this.minKey) + " -> DONE");
                    }
                    this.finished = true;
                    return false;
                }
            }

            // We found the next pair
            this.next = new KVPair(key, Util.unwrap(cursor.getValue()));
            if (XodusKVStore.this.log.isTraceEnabled())
                XodusKVStore.this.log.trace("seek {} -> {}", this.reverse ? "previous" : "next", this.next);

            // Done
            return true;
        }

    // Closeable

        @Override
        public void close() {
            synchronized (this) {
                if (this.closed)
                    return;
                this.closed = true;
            }
            if (XodusKVStore.this.log.isTraceEnabled())
                XodusKVStore.this.log.trace("closing {}", this);
            try {
                this.cursor.close();
            } catch (Throwable e) {
                XodusKVStore.this.log.debug("caught exception closing Xodus cursor (ignoring)", e);
            }
        }

    // Object

        @Override
        public String toString() {
            return XodusKVStore.class.getSimpleName() + "." + this.getClass().getSimpleName()
              + "[minKey=" + ByteUtil.toString(this.minKey)
              + ",maxKey=" + ByteUtil.toString(this.maxKey)
              + (this.reverse ? ",reverse" : "")
              + "]";
        }
    }
}
