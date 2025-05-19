
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVDatabaseException;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRange;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;
import io.permazen.util.CloseableRefs;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.dellroad.stuff.spring.RetryTransactionProvider;

/**
 * An in-memory {@link KVTransaction} that is based on a {@link KVTransaction#readOnlySnapshot readOnlySnapshot()} of some
 * original {@link KVTransaction} and that can, after an arbitrary amount of time has passed and changes have been made, be
 * commited back to the database within a new {@link KVTransaction}, as long as no conflicting changes have been written
 * to the databse since the original transaction.
 *
 * <p>
 * In effect, this class gives the appearance of a regular {@link KVTransaction} that can stay open for an arbitrarily long time,
 * detached from the database, and still be committed with the same consistency guarantees as a normal transaction. It can be
 * useful in certain scenarios, for example, to support editing an entity in a GUI application where the user could take several
 * minutes to complete a form, while also ensuring that everything the user sees while editing the form is still up to date when
 * the form is actually submitted.
 *
 * <p>
 * Instances themselves support {@link #readOnlySnapshot} and {@link #withWeakConsistency withWeakConsistency()}.
 *
 * <p>
 * Instances do not support {@link #setTimeout setTimeout()} or {@link #watchKey watchKey()}.
 *
 * <p><b>Transaction Management</b></p>
 *
 * <p>
 * New instances must be explicitly opened via {@link #open}. At that time a regular database transaction is opened,
 * a {@linkplain KVTransaction#readOnlySnapshot snapshot} of the database taken, and then that transaction is immediately closed.
 * Reads and writes in this transaction are then tracked and kept entirely in memory; no regular transaction remains open.
 * Later, when this transaction is {@link #commit}'ed, a new regular database transaction is opened, a conflict check is
 * performed (to determine whether any of the keys read by this transaction have since changed), and if there are no conflicts,
 * all of this transaction's accumulated writes are applied; otherwise, {@link TransactionConflictException} is thrown.
 *
 * <p>
 * The conflict check can also be performed on demand at any time (based on all keys read so far) while leaving this transaction
 * open via {@link #checkForConflicts()}.
 *
 * <p>
 * Because open instances retain an underlying snaphot which consumes resources, callers should ensure that instances are always
 * eventually committed or rolled back as soon as they are no longer needed. Note that {@link #close} can be used as a synonym
 * for {@link #rollback}.
 *
 * <p><b>Underlying Transaction Configuration</b></p>
 *
 * <p>
 * The real transactions that are used at open and commit time are configurable via the usual transaction options.
 * In addition, this class adds to those options {@link RETRY_PROVIDER_OPTION} which allows the configuration of
 * automatic retries for these transactions; for some key/value technologies, retry logic is important for robustness.
 * The {@link RETRY_PROVIDER_OPTION} option's value is a {@link RetryTransactionProvider}.
 *
 * <p><b>Caveats</b></p>
 *
 * <p>
 * This class only works with {@link KVDatabase}'s that support {@link KVTransaction#readOnlySnapshot readOnlySnapshot()}.
 *
 * The probability for a conflict increases in proportion to the number of distinct keys (or key ranges) read in this transaction
 * and/or the amount of writes committed to the underlying database while this transaction remains open. Of course, this also
 * depends greatly on the problem domain, i.e., whether this and other transactions are doing work that overlaps.
 *
 * <p>
 * Reads and writes are tracked via {@link MutableView}; the amount of memory required is as described in that class.
 *
 * <p>
 * The work required for the conflict check scales in proportion to the number of distinct keys (or key ranges)
 * read in the transaction.
 *
 * @see io.permazen.Permazen#createBranchedTransaction
 */
public class BranchedKVTransaction implements KVTransaction, CloseableKVStore {

    /**
     * Transaction option for a {@link RetryTransactionProvider} to use with the underlying open and/or commit transactions.
     */
    public static final String RETRY_PROVIDER_OPTION = "org.dellroad.stuff.spring.RetryTransactionProvider";

    private final KVDatabase kvdb;
    private final Retryer openRetryer;
    private final Retryer syncRetryer;
    private final Map<String, ?> openOptions;
    private final Map<String, ?> syncOptions;

    private State state = State.INITIAL;

    // The store for this transaction. Null except in state OPEN. Locking order: (a) this (b) this.view.
    private MutableView view;

    // Tracks when to close() the underlying transaction snapshot, i.e., view.getBaseKVStore().
    private CloseableRefs<CloseableKVStore> snapshotRefs;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * This instance must be {@link #open}'ed before use.
     *
     * @param kvdb database
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public BranchedKVTransaction(KVDatabase kvdb) {
        this(kvdb, null, null);
    }

    /**
     * Constructor.
     *
     * <p>
     * This instance must be {@link #open}'ed before use.
     *
     * <p>
     * Any {@link #RETRY_PROVIDER_OPTION} option in {@code openOptions} and/or {@code syncOptions}
     * is removed (i.e., not passed down {@code kvdb}) and used to automatically retry those transactions.
     *
     * @param kvdb database
     * @param openOptions transaction options for opening transaction; may be null
     * @param syncOptions transaction options for sync/commit transaction(s); may be null
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public BranchedKVTransaction(KVDatabase kvdb, Map<String, ?> openOptions, Map<String, ?> syncOptions) {
        Preconditions.checkArgument(kvdb != null, "null kvdb");
        this.kvdb = kvdb;
        if (openOptions != null && openOptions.containsKey(RETRY_PROVIDER_OPTION)) {
            this.openOptions = new HashMap<>(openOptions);
            this.openRetryer = new Retryer(this.openOptions.remove(RETRY_PROVIDER_OPTION));
        } else {
            this.openOptions = openOptions;
            this.openRetryer = null;
        }
        if (syncOptions != null && syncOptions.containsKey(RETRY_PROVIDER_OPTION)) {
            this.syncOptions = new HashMap<>(syncOptions);
            this.syncRetryer = new Retryer(this.syncOptions.remove(RETRY_PROVIDER_OPTION));
        } else {
            this.syncOptions = syncOptions;
            this.syncRetryer = null;
        }
    }

// Lifecycle

    /**
     * Open this transaction.
     *
     * <p>
     * This results in a snapshot being taken of the database.
     *
     * @throws UnsupportedOperationException if the database doesn't support {@link KVTransaction#readOnlySnapshot}
     * @throws IllegalStateException if this method has already been invoked
     * @throws KVDatabaseException if the underlying transaction fails
     */
    public synchronized void open() {
        this.checkState(State.INITIAL);
        final AtomicReference<CloseableKVStore> snapshotRef = new AtomicReference<>();
        this.tx(this.openOptions, this.openRetryer,
          this.getClass().getSimpleName() + ".open()",
          tx -> snapshotRef.set(tx.readOnlySnapshot()),
          () -> {
            synchronized (this) {       // this is redundant here but it silences a spotbugs warning
                this.snapshotRefs = new CloseableRefs<>(snapshotRef.get());
                this.view = new MutableView(snapshotRef.get());
                this.state = State.OPEN;
            }
          },
          () -> Optional.ofNullable(snapshotRef.get()).ifPresent(CloseableKVStore::close));
    }

    @Override
    public synchronized void commit() {
        this.checkState(State.OPEN);

        // Disallow Iterator.remove() from getRange() after commit() invoked
        synchronized (this.view) {
            this.view.setReadOnly();
        }

        // Check for conflicts and apply our changes
        this.tx(this.syncOptions, this.syncRetryer,
          this.getClass().getSimpleName() + ".commit()",
          tx -> {
            synchronized (this) {       // this is redundant here but it silences a spotbugs warning
                synchronized (this.view) {
                    this.checkForConflicts(tx);
                    this.view.getWrites().applyTo(tx);
                }
            }
          },
          this::close,
          this::close);
    }

    @Override
    public void rollback() {
        this.close();
    }

    /**
     * Close this transaction.
     *
     * <p>
     * Equivalent to {@link #rollback}.
     */
    @Override
    public synchronized void close() {
        if (this.state != State.CLOSED) {
            if (this.snapshotRefs != null) {
                this.snapshotRefs.unref();
                this.snapshotRefs = null;
            }
            this.view = null;
            this.state = State.CLOSED;
        }
    }

// KVStore

    @Override
    public synchronized ByteData get(ByteData key) {
        this.checkState(State.OPEN);
        return this.view.get(key);
    }

    @Override
    public synchronized KVPair getAtLeast(ByteData minKey, ByteData maxKey) {
        this.checkState(State.OPEN);
        return this.view.getAtLeast(minKey, maxKey);
    }

    @Override
    public synchronized KVPair getAtMost(ByteData maxKey, ByteData minKey) {
        this.checkState(State.OPEN);
        return this.view.getAtMost(minKey, maxKey);
    }

    @Override
    public synchronized CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse) {
        this.checkState(State.OPEN);
        return this.view.getRange(minKey, maxKey, reverse);
    }

    @Override
    public synchronized void put(ByteData key, ByteData value) {
        this.checkState(State.OPEN);
        this.view.put(key, value);
    }

    @Override
    public synchronized void remove(ByteData key) {
        this.checkState(State.OPEN);
        this.view.remove(key);
    }

    @Override
    public synchronized void removeRange(ByteData minKey, ByteData maxKey) {
        this.checkState(State.OPEN);
        this.view.removeRange(minKey, maxKey);
    }

    @Override
    public synchronized ByteData encodeCounter(long value) {
        this.checkState(State.OPEN);
        return this.view.encodeCounter(value);
    }

    @Override
    public synchronized long decodeCounter(ByteData value) {
        this.checkState(State.OPEN);
        return this.view.decodeCounter(value);
    }

    @Override
    public synchronized void adjustCounter(ByteData key, long amount) {
        this.checkState(State.OPEN);
        this.view.adjustCounter(key, amount);
    }

// KVTransaction

    @Override
    public KVDatabase getKVDatabase() {
        return this.kvdb;
    }

    @Override
    public void setTimeout(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized boolean isReadOnly() {
        this.checkState(State.OPEN);
        return this.view.isReadOnly();
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        this.checkState(State.OPEN);
        this.view.setReadOnly();
    }

    @Override
    public Future<Void> watchKey(ByteData key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void withWeakConsistency(Runnable action) {
        MutableView theView;
        synchronized (this) {
            this.checkState(State.OPEN);
            theView = this.view;
        }
        theView.withoutReadTracking(true, action);
    }

    @Override
    public synchronized CloseableKVStore readOnlySnapshot() {
        this.checkState(State.OPEN);

        // Clone the current view
        final MutableView snapshot = this.view.clone();
        snapshot.disableReadTracking();
        snapshot.setReadOnly();

        // Count a new reference to the original snapshot
        this.snapshotRefs.ref();

        // Wrap in a CloseableKVStore
        return new CloseableForwardingKVStore(snapshot, this.snapshotRefs::unref);
    }

// Conflict Checking

    /**
     * Check for conflicts between this transaction and the current database contents.
     *
     * <p>
     * This method performs the conflict check that normally occurs during {@link #commit}, but without
     * actually flushing any writes back to the underlying database. This transaction instance remains open.
     *
     * <p>
     * This checks whether any of the keys read by this transaction have changed since the original snapshot
     * was taken. If so, then the writes from this transaction cannot be consistently merged back into the
     * database and so an exception is immediately thrown.
     *
     * @throws TransactionConflictException if any conflicts are found
     */
    public void checkForConflicts() {
        this.tx(this.syncOptions, this.syncRetryer,
          this.getClass().getSimpleName() + ".checkForConflicts()",
          this::checkForConflicts,
          null,
          null);
    }

    private void tx(Map<String, ?> options, Retryer retryer, String description,
      Consumer<KVTransaction> operation, Runnable onSuccess, Runnable onFailure) {

        // Sanity check
        Preconditions.checkArgument(operation != null, "null operation");

        // Wrap the operation in a transaction
        final Runnable txOperation = () -> {
            final KVTransaction tx = this.kvdb.createTransaction(options);
            try {
                operation.accept(tx);
                tx.commit();
            } finally {
                tx.rollback();
            }
        };

        // If we are retrying, wrap with that as well
        final Runnable retriedOperation = retryer != null ? retryer.wrap(txOperation, description) : txOperation;

        // Now do the operation and invoke callback
        boolean success = false;
        try {
            retriedOperation.run();
            success = true;
        } finally {
            (success ? onSuccess : onFailure).run();
        }
    }

    // Verify that every key (range) that has been read so far in this transaction has not changed in "newKV"
    private synchronized void checkForConflicts(KVTransaction newKV) {
        this.checkState(State.OPEN);
        final KVStore oldKV = this.view.getBaseKVStore();
        synchronized (this.view) {
            for (KeyRange range : this.view.getReads())
                this.checkForConflicts(range, oldKV, newKV);
        }
    }

    // Check whether any key/value pair in "range" differs between "oldKV" and "newKV"
    private void checkForConflicts(KeyRange range, KVStore oldKV, KVStore newKV) {
        if (range.isSingleKey()) {
            final ByteData key = range.getMin();
            final Optional<ByteData> keyOpt = Optional.of(key);
            final KVPair oldPair = keyOpt.map(oldKV::get).map(value -> new KVPair(key, value)).orElse(null);
            final KVPair newPair = keyOpt.map(newKV::get).map(value -> new KVPair(key, value)).orElse(null);
            this.checkForConflicts(oldPair, newPair);
        } else {

            // Compare the values from each store within the range
            try (
              CloseableIterator<KVPair> oldIter = oldKV.getRange(range);
              CloseableIterator<KVPair> newIter = newKV.getRange(range)) {
                while (true) {
                    final KVPair oldPair = oldIter.hasNext() ? oldIter.next() : null;
                    final KVPair newPair = newIter.hasNext() ? newIter.next() : null;
                    if (this.checkForConflicts(oldPair, newPair))
                        break;
                }
            }
        }
    }

    // Check whether the two key/value pairs differ. Each pair is the next key in sequence, if any, else null.
    // Returns true if both are null (i.e., iteration is complete).
    private boolean checkForConflicts(KVPair oldPair, KVPair newPair) {

        // If both iterations are exhausted, no conflict
        if (oldPair == null && newPair == null)
            return true;

        // If either iteration is exhausted, the other is not, so there is a conflict
        if (oldPair == null)
            throw new TransactionConflictException(this, new ReadWriteConflict(newPair.getKey()));
        final ByteData oldKey = oldPair.getKey();
        if (newPair == null)
            throw new TransactionConflictException(this, new ReadRemoveConflict(oldKey));

        // See if one key is "earlier" than then other
        final ByteData newKey = newPair.getKey();
        int diff = oldKey.compareTo(newKey);
        if (diff > 0)
            throw new TransactionConflictException(this, new ReadWriteConflict(newKey));
        if (diff < 0)
            throw new TransactionConflictException(this, new ReadRemoveConflict(oldKey));

        // The keys are the same, so the compare values
        final ByteData oldValue = oldPair.getValue();
        final ByteData newValue = newPair.getValue();
        if (!oldValue.equals(newValue))
            throw new TransactionConflictException(this, new ReadWriteConflict(oldKey));

        // Done
        return false;
    }

// Internal Methods

    private void checkState(State expectedState) {
        assert Thread.holdsLock(this);
        if (!this.state.equals(expectedState))
            throw this.state.newStateMismatchException(this);
    }

// State

    private enum State {
        INITIAL("transaction is not open yet", (tx, msg) -> new IllegalStateException(msg)),
        OPEN("transaction is already open", (tx, msg) -> new IllegalStateException(msg)),
        CLOSED("transaction is no longer open", StaleKVTransactionException::new);

        private final String stateMismatchMessage;
        private final BiFunction<KVTransaction, String, RuntimeException> exceptionCreator;

        State(String stateMismatchMessage, BiFunction<KVTransaction, String, RuntimeException> exceptionCreator) {
            this.stateMismatchMessage = stateMismatchMessage;
            this.exceptionCreator = exceptionCreator;
        }

        public RuntimeException newStateMismatchException(BranchedKVTransaction tx) {
            return this.exceptionCreator.apply(tx, this.stateMismatchMessage);
        }
    }

// Retryer

    // Separate class due to linkage to optional dependency dellroad-stuff-spring
    private static class Retryer {

        private final RetryTransactionProvider provider;

        Retryer(Object obj) {
            Preconditions.checkArgument(obj != null, String.format("transaction option \"%s\" is null", RETRY_PROVIDER_OPTION));
            try {
                this.provider = (RetryTransactionProvider)obj;
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(String.format(
                  "transaction option \"%s\" is not a %s", RETRY_PROVIDER_OPTION, RetryTransactionProvider.class.getName()), e);
            }
        }

        public Runnable wrap(Runnable operation, String description) {
            Preconditions.checkArgument(operation != null, "null operation");
            final RetryTransactionProvider.RetrySetup<Void> setup = new RetryTransactionProvider.RetrySetup<>(
              BranchedKVTransaction.class.getName(), description, () -> {
                operation.run();
                return null;
              });
            return () -> this.provider.retry(setup);
        }
    }
}
