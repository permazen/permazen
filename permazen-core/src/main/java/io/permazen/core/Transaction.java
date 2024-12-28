
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.core.util.ObjIdMap;
import io.permazen.core.util.ObjIdSet;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.util.CloseableForwardingKVStore;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ImmutableNavigableMap;
import io.permazen.util.NavigableSets;
import io.permazen.util.UnsignedIntEncoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.dellroad.stuff.util.LongMap;
import org.dellroad.stuff.util.LongSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Permazen {@link Database} transaction.
 *
 * <p>
 * Note: this is the lower level, core API for {@link io.permazen.Permazen}. In most cases this API
 * will only be used indirectly through the higher level {@link io.permazen.Permazen}, {@link io.permazen.PermazenTransaction},
 * and {@link io.permazen.PermazenObject} APIs.
 *
 * <p>
 * Methods in this class can be divided into the following categories:
 *
 * <p>
 * <b>Transaction Meta-Data</b>
 * <ul>
 *  <li>{@link #getDatabase getDatabase()} - Get the associated {@link Database}</li>
 *  <li>{@link #getKVTransaction getKVTransaction()} -  Get the underlying key/value store transaction.</li>
 *  <li>{@link #getSchema getSchema()} - Get the {@link Schema} that will be used by this transaction</li>
 *  <li>{@link #getUserObject} - Get user object associated with this instance</li>
 *  <li>{@link #setUserObject setUserObject()} - Set user object associated with this instance</li>
 * </ul>
 *
 * <p>
 * <b>Transaction Lifecycle</b>
 * <ul>
 *  <li>{@link #commit commit()} - Commit transaction</li>
 *  <li>{@link #rollback rollback()} - Roll back transaction</li>
 *  <li>{@link #isOpen isOpen()} - Test whether transaction is still open</li>
 *  <li>{@link #setTimeout setTimeout()} - Set transaction timeout</li>
 *  <li>{@link #setReadOnly setReadOnly()} - Set transaction to read-only</li>
 *  <li>{@link #setRollbackOnly setRollbackOnly()} - Set transaction for rollack only</li>
 *  <li>{@link #addCallback addCallback()} - Register a {@link Callback} on transaction completion</li>
 *  <li>{@link #createDetachedTransaction createDetachedTransaction()} - Create a empty, in-memory transaction</li>
 *  <li>{@link #createSnapshotTransaction createSnapshotTransaction()} - Create an in-memory transaction
 *      pre-populated with a snapshot of this transaction</li>
 *  <li>{@link #isDetached} - Determine whether this transaction is a detached transaction</li>
 * </ul>
 *
 * <p>
 * <b>Schema Management</b>
 * <ul>
 *  <li>{@link #getObjType(ObjId) getObjType()} - Get an object's database object type</li>
 *  <li>{@link #migrateSchema migrateSchema()} - Migrate an object's schema to match this transaction's schema</li>
 *  <li>{@link #addSchemaChangeListener addSchemaChangeListener()} - Receive notifications about object schema migrations</li>
 *  <li>{@link #removeSchemaChangeListener removeSchemaChangeListener()} - Unregister a schema migration listener</li>
 *  <li>{@link #getSchemaBundle getSchemaBundle()} - Get all {@link Schema}s registered in the database</li>
 *  <li>{@link #addSchema addSchema()} - Register a new {@link Schema} in the database</li>
 *  <li>{@link #removeSchema removeSchema()} - Remove a registered {@link Schema} from the database</li>
 * </ul>
 *
 * <p>
 * <b>Object Lifecycle</b>
 * <ul>
 *  <li>{@link #create(String) create()} - Create a database object</li>
 *  <li>{@link #delete delete()} - Delete a database object</li>
 *  <li>{@link #copy copy()} - Copy an object into a (possibly different) transaction</li>
 *  <li>{@link #addCreateListener addCreateListener()} - Register a {@link CreateListener} for notifications about new objects</li>
 *  <li>{@link #removeCreateListener removeCreateListener()} - Unregister a {@link CreateListener}</li>
 *  <li>{@link #addDeleteListener addDeleteListener()} - Register a {@link DeleteListener} for notifications
 *      about deleted objects</li>
 *  <li>{@link #removeDeleteListener removeDeleteListener()} - Unregister a {@link DeleteListener}</li>
 * </ul>
 *
 * <p>
 * <b>Object Queries</b>
 * <ul>
 *  <li>{@link #getAll getAll()} - Get all objects</li>
 *  <li>{@link #getAll getAll(String)} - Get all objects of a specific object type</li>
 *  <li>{@link #exists exists()} - Test whether a database object exists</li>
 * </ul>
 *
 * <p>
 * <b>Index Queries</b>
 * <ul>
 *  <li>{@link #querySimpleIndex querySimpleIndex()} - Query an index on a {@link SimpleField}
 *      or a {@link ComplexField} sub-field</li>
 *  <li>{@link #queryListElementIndex queryListElementIndex()}
 *      - Query an index on a {@link ListField}'s elements, also returning their corresponding list indexes</li>
 *  <li>{@link #queryMapValueIndex queryMapValueIndex()}
 *      - Query an index on a {@link MapField}'s values, also returning their corresponding keys</li>
 *  <li>{@link #queryCompositeIndex2 queryCompositeIndex2()} - Query a composite index on two fields</li>
 *  <li>{@link #queryCompositeIndex3 queryCompositeIndex3()} - Query a composite index on three fields</li>
 *  <li>{@link #queryCompositeIndex3 queryCompositeIndex4()} - Query a composite index on four fields</li>
 *  <!-- COMPOSITE-INDEX -->
 *  <li>{@link #querySchemaIndex querySchemaIndex()} - Query the index that groups objects by their schema</li>
 * </ul>
 *
 * <p>
 * <b>Field Access</b>
 * <ul>
 *  <li>{@link #readSimpleField readSimpleField()} - Read the value of a {@link SimpleField} in an object</li>
 *  <li>{@link #writeSimpleField writeSimpleField()} - Write the value of a {@link SimpleField} in an object</li>
 *  <li>{@link #readCounterField readCounterField()} - Read the value of a {@link CounterField} in an object</li>
 *  <li>{@link #writeCounterField writeCounterField()} - Write the value of a {@link CounterField} in an object</li>
 *  <li>{@link #adjustCounterField adjustCounterField()} - Adjust the value of a {@link CounterField} in an object</li>
 *  <li>{@link #readSetField readSetField()} - Access a {@link SetField} in an object as a {@link NavigableSet}</li>
 *  <li>{@link #readListField readListField()} - Access a {@link ListField} in an object as a {@link List}</li>
 *  <li>{@link #readMapField readMapField()} - Access a {@link MapField} in an object as a {@link NavigableMap}</li>
 *  <li>{@link #getKey getKey(ObjId)} - Get the {@link KVDatabase} key prefix corresponding to an object</li>
 * </ul>
 *
 * <p>
 * <b>Field Change Notifications</b>
 * <ul>
 *  <li>{@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} - Register a {@link SimpleFieldChangeListener} for
 *      notifications of changes in a {@link SimpleField}, as seen through a path of object references</li>
 *  <li>{@link #addSetFieldChangeListener addSetFieldChangeListener()} - Register a {@link SetFieldChangeListener} for
 *      notifications of changes in a {@link SetField}, as seen through a path of object references</li>
 *  <li>{@link #addListFieldChangeListener addListFieldChangeListener()} - Register a {@link ListFieldChangeListener} for
 *      notifications of changes in a {@link ListField}, as seen through a path of object references</li>
 *  <li>{@link #addMapFieldChangeListener addMapFieldChangeListener()} - Register a {@link MapFieldChangeListener} for
 *      notifications of changes in a {@link MapField}, as seen through a path of object references</li>
 *  <li>{@link #removeSimpleFieldChangeListener removeSimpleFieldChangeListener()} - Unregister a previously registered
 *      {@link SimpleFieldChangeListener}</li>
 *  <li>{@link #removeSetFieldChangeListener removeSetFieldChangeListener()} - Unregister a previously registered
 *      {@link SetFieldChangeListener}</li>
 *  <li>{@link #removeListFieldChangeListener removeListFieldChangeListener()} - Unregister a previously registered
 *      {@link ListFieldChangeListener}</li>
 *  <li>{@link #removeMapFieldChangeListener removeMapFieldChangeListener()} - Unregister a previously registered
 *      {@link MapFieldChangeListener}</li>
 * </ul>
 *
 * <p>
 * <b>Reference Paths</b>
 * <ul>
 *  <li>{@link #followReferencePath followReferencePath()} - Find all objects referred to by any element in a given set
 *      of starting objects through a specified reference path</li>
 *  <li>{@link #invertReferencePath invertReferencePath()} - Find all objects that refer to any element in a given set
 *      of target objects through a specified reference path</li>
 * </ul>
 *
 * <p>
 * <b>Listener Sets</b>
 * <ul>
 *  <li>{@link #snapshotListeners} - Create an immutable snapshot of all registered listeners</li>
 *  <li>{@link #setListeners setListeners()} - Bulk registration of listeners from a previously created snapshot</li>
 * </ul>
 *
 * <p>
 * All methods returning a set of values return a {@link NavigableSet}.
 * The {@link NavigableSets} utility class provides methods for the efficient {@link NavigableSets#intersection intersection},
 * {@link NavigableSets#union union}, {@link NavigableSets#difference difference}, and
 * {@link NavigableSets#symmetricDifference symmetric difference} of {@link NavigableSet}s containing the same elements and
 * ordering, thereby providing the equivalent of traditional database joins.
 *
 * <p>
 * Instances of this class are thread safe.
 */
@ThreadSafe
public class Transaction {

    private static final int MAX_GENERATED_KEY_ATTEMPTS
      = Integer.parseInt(System.getProperty(Transaction.class.getName() + ".MAX_GENERATED_KEY_ATTEMPTS", "64"));
    private static final int MAX_OBJ_INFO_CACHE_ENTRIES
      = Integer.parseInt(System.getProperty(Transaction.class.getName() + ".MAX_OBJ_INFO_CACHE_ENTRIES", "1000"));

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Database
    final Database db;

    // Underlying transaction
    final KVTransaction kvt;

    // Schema state
    @GuardedBy("this")
    Schema schema;
    @GuardedBy("this")
    SchemaBundle schemaBundle;

    // TX state
    @GuardedBy("this")
    boolean stale;
    @GuardedBy("this")
    boolean ending;
    @GuardedBy("this")
    boolean rollbackOnly;
    @GuardedBy("this")
    boolean disableListenerNotifications;

    // Listeners
    @GuardedBy("this")
    private LongMap<Set<SchemaChangeListener>> schemaChangeListeners;   // grouped by object type storage ID
    @GuardedBy("this")
    private LongMap<Set<CreateListener>> createListeners;               // grouped by object type storage ID
    @GuardedBy("this")
    private LongMap<Set<DeleteMonitor>> deleteMonitors;                 // grouped by object type storage ID
    @GuardedBy("this")
    private NavigableMap<Integer, Set<FieldMonitor<?>>> fieldMonitors;  // grouped by field storage ID
    @GuardedBy("this")
    private MonitorCache monitorCache;                                  // quick check for monitors; only if ListenerSet installed

    // Callbacks
    @GuardedBy("this")
    private LinkedHashSet<Callback> callbacks;

    // Deletion
    @GuardedBy("this")
    private ObjIdSet deleteNotified;

    // Misc
    @GuardedBy("this")
    private final ThreadLocal<TreeMap<Integer, ArrayList<FieldChangeNotifier<?, ?>>>> pendingFieldChangeNotifications
      = new ThreadLocal<>();
    @GuardedBy("this")
    private final ObjIdMap<ObjInfo> objInfoCache = new ObjIdMap<>();
    @GuardedBy("this")
    private Object userObject;

    // Recording of deleted assignments used during a copy() operation (otherwise should be null)
    private ObjIdMap<ReferenceField> deletedAssignments;

// Constructors

    Transaction(Database db, KVTransaction kvt, Schema schema) {
        assert db != null;
        assert kvt != null;
        assert schema != null;
        this.db = db;
        this.kvt = kvt;
        this.schema = schema;
        this.schemaBundle = schema.getSchemaBundle();
        assert this.schema.isEmpty() || this.schema == this.schemaBundle.getSchema(this.schema.getSchemaIndex());
        assert this.schema.isEmpty() || this.schema == this.schemaBundle.getSchema(this.schema.getSchemaId());
    }

// Transaction Meta-Data

    /**
     * Get the database with which this transaction is associated.
     *
     * @return associated database
     */
    public synchronized Database getDatabase() {
        return this.db;
    }

    /**
     * Get the database's schema bundle
     *
     * <p>
     * This returns all of the schemas currently recorded in the database as seen by this transaction.
     *
     * @return database schema bundle
     */
    public synchronized SchemaBundle getSchemaBundle() {
        return this.schemaBundle;
    }

    /**
     * Get the database schema associated with this transaction.
     *
     * <p>
     * This is the target schema for newly created and {@linkplain #migrateSchema migrated} objects.
     *
     * @return this transaction's schema
     */
    public synchronized Schema getSchema() {
        return this.schema;
    }

    /**
     * Get the underlying key/value store transaction.
     *
     * <p>
     * <b>Warning:</b> making changes directly to the key/value store directly is not supported.
     * If changes are made, future behavior is undefined.
     *
     * @return the associated key/value transaction
     */
    public synchronized KVTransaction getKVTransaction() {
        return this.kvt;
    }

    /**
     * Manually record the given non-empty schema in the database.
     *
     * <p>
     * If successful, {@linkplain #getSchemaBundle this transaction's schema bundle} will be updated.
     *
     * <p>
     * This transaction's current schema does not change.
     *
     * <p>
     * This method is dangerous and should normally not be used except by low-level tools.
     *
     * @return true if the schema was added, false if it was already in the schema bundle
     * @throws InvalidSchemaException if {@code schemaModel} is invalid (i.e., does not pass validation checks)
     * @throws SchemaMismatchException if {@code schemaModel} has explicit storage ID assignments
     *  that conflict with storage ID assignments already recorded in the database
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code schemaModel} is empty
     * @throws IllegalArgumentException if {@code schemaModel} is null
     */
    public boolean addSchema(SchemaModel schemaModel) {

        // Sanity check
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");
        if (!schemaModel.isLockedDown(true)) {
            schemaModel = schemaModel.clone();
            schemaModel.lockDown(true);
        }
        schemaModel.validate();
        Preconditions.checkArgument(!schemaModel.isEmpty(), "empty schema");

        // Add new schema
        synchronized (this) {

            // Encode new schema bundle with the schema added
            final SchemaBundle.Encoded newEncoded = this.schemaBundle.withSchemaAdded(0, schemaModel);
            if (newEncoded == null)
                return false;

            // Update our current bundle
            this.updateSchemaBundle(true, newEncoded);
        }

        // Done
        return true;
    }

    /**
     * Manually remove the given schema from the database.
     *
     * <p>
     * If successful, {@linkplain #getSchemaBundle this transaction's schema bundle} will be updated.
     *
     * <p>
     * If the removed schema is also this transaction's current schema, then this transaction's schema reverts to the
     * empty schema. The empty schema itself is never recorded in a database, so it will never be found by this method.
     *
     * <p>
     * This method is dangerous and should normally not be used except by low-level tools.
     *
     * @return true if the schema was removed, false if it was not found
     * @throws IllegalArgumentException if any objects in the schema still exist
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code schemaId} is null
     */
    public boolean removeSchema(SchemaId schemaId) {

        // Sanity check
        Preconditions.checkArgument(schemaId != null, "null schemaId");

        // Remove schema
        synchronized (this) {

            // Find the old schema
            final Schema oldSchema = this.schemaBundle.getSchemasBySchemaId().get(schemaId);
            if (oldSchema == null)
                return false;

            // Verify no objects in this schema still exist
            final int schemaIndex = oldSchema.getSchemaIndex();
            if (Layout.getSchemaIndex(this.kvt).asMap().containsKey(schemaIndex))
                throw new IllegalArgumentException(String.format("one or more objects in schema \"%s\" still exist", schemaId));

            // Encode new schema bundle with the schema removed
            final SchemaBundle.Encoded newEncoded = this.schemaBundle.withSchemaRemoved(schemaId);

            // Update our current bundle
            this.updateSchemaBundle(false, newEncoded);
        }

        // Done
        return true;
    }

    private synchronized void updateSchemaBundle(boolean added, SchemaBundle.Encoded encoded) {
        Preconditions.checkArgument(encoded != null, "null encoded");

        // Update the schemas recorded in the database
        encoded.writeTo(this.kvt);

        // Update this transaction's schema bundle
        this.schemaBundle = new SchemaBundle(encoded, this.db.getEncodingRegistry());

        // Update this transaction's schema
        final SchemaModel currentSchemaModel = this.schema.getSchemaModel();
        final SchemaId currentSchemaId = currentSchemaModel.getSchemaId();
        final Schema newSchema = this.schemaBundle.getSchemasBySchemaId().get(currentSchemaId);
        if (newSchema != null)
            this.schema = newSchema;
        else if (currentSchemaModel.isEmpty() || !added)
            this.schema = new Schema(this.schemaBundle);
        else
            throw new IllegalArgumentException(String.format("internal error: current schema \"%s\" not found", currentSchemaId));
    }

// Transaction Lifecycle

    /**
     * Commit this transaction.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws io.permazen.kv.RetryKVTransactionException from {@link KVTransaction#commit KVTransaction.commit()}
     * @throws RollbackOnlyTransactionException if this instance has been {@linkplain #setRollbackOnly marked} rollback only;
     *  this instance will be automatically rolled back
     */
    public synchronized void commit() {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.ending)
            throw new StaleTransactionException(this, "commit() invoked re-entrantly from commit callback");

        // Rollback only?
        if (this.rollbackOnly) {
            this.log.debug("commit() invoked on transaction {} marked rollback-only, rolling back", this);
            this.rollback();
            throw new RollbackOnlyTransactionException(this);
        }
        this.ending = true;

        // Do beforeCommit() and beforeCompletion() callbacks
        if (this.log.isTraceEnabled())
            this.log.trace("commit() invoked on{} transaction {}", this.isReadOnly() ? " read-only" : "", this);
        Callback failedCallback = null;
        try {
            if (this.callbacks != null) {
                for (Callback callback : this.callbacks) {
                    failedCallback = callback;
                    if (this.log.isTraceEnabled())
                        this.log.trace("commit() invoking beforeCommit() on transaction {} callback {}", this, callback);
                    callback.beforeCommit(this.isReadOnly());
                }
                failedCallback = null;
            }
        } finally {

            // TX operations no longer permitted
            this.stale = true;

            // Log the offending callback, if any
            if (failedCallback != null) {
                this.log.warn("error invoking beforeCommit() method on transaction "
                  + this + " callback " + failedCallback + ", rolling back");
            }

            // Do before completion callback
            this.triggerBeforeCompletion();
            if (failedCallback != null) {
                try {
                    try {
                        this.kvt.rollback();
                    } catch (KVTransactionException e) {
                        // ignore
                    }
                } finally {
                    this.triggerAfterCompletion(false);
                }
            }
        }

        // Commit KVTransaction and trigger after completion callbacks
        try {
            this.kvt.commit();
            if (this.callbacks != null) {
                for (Callback callback : this.callbacks) {
                    failedCallback = callback;
                    if (this.log.isTraceEnabled())
                        this.log.trace("commit() invoking afterCommit() on transaction {} callback {}", this, callback);
                    callback.afterCommit();
                }
                failedCallback = null;
            }
        } finally {

            // Log the offending callback, if any
            if (failedCallback != null)
                this.log.warn("error invoking afterCommit() method on transaction {} callback {}", this, failedCallback);

            // Do after completion callback
            this.triggerAfterCompletion(true);
        }
    }

    /**
     * Roll back this transaction.
     *
     * <p>
     * This method may be invoked at any time, even after a previous invocation of
     * {@link #commit} or {@link #rollback}, in which case the invocation will be ignored.
     */
    public synchronized void rollback() {

        // Sanity check
        if (this.stale)
            return;
        if (this.ending) {
            this.log.warn("rollback() invoked re-entrantly from commit callback (ignoring)");
            return;
        }
        this.ending = true;
        if (this.log.isTraceEnabled())
            this.log.trace("rollback() invoked on{} transaction {}", this.isReadOnly() ? " read-only" : "", this);

        // Do before completion callbacks
        try {
            this.triggerBeforeCompletion();
        } finally {
            this.stale = true;
        }

        // Roll back KVTransaction and trigger after completion callbacks
        try {
            this.kvt.rollback();
        } finally {
            this.triggerAfterCompletion(false);
        }
    }

    private /*synchronized*/ void triggerBeforeCompletion() {
        assert Thread.holdsLock(this);
        if (this.callbacks == null)
            return;
        for (Callback callback : this.callbacks) {
            if (this.log.isTraceEnabled())
                this.log.trace("invoking beforeCompletion() on transaction {} callback {}", this, callback);
            try {
                callback.beforeCompletion();
            } catch (Throwable t) {
                this.log.error("error from beforeCompletion() method of transaction "
                  + this + " callback " + callback + " (ignoring)", t);
            }
        }
    }

    private /*synchronized*/ void triggerAfterCompletion(boolean committed) {
        assert Thread.holdsLock(this);
        if (this.callbacks == null)
            return;
        for (Callback callback : this.callbacks) {
            if (this.log.isTraceEnabled())
                this.log.trace("invoking afterCompletion() on transaction {} callback {}", this, callback);
            try {
                callback.afterCompletion(committed);
            } catch (Throwable t) {
                this.log.error("error from afterCompletion() method of transaction "
                  + this + " callback " + callback + " (ignoring)", t);
            }
        }
    }

    /**
     * Determine whether this transaction is still open.
     *
     * <p>
     * In other words, other methods in this class won't throw {@link StaleTransactionException}.
     *
     * @return true if this instance is still usable
     */
    public synchronized boolean isOpen() {
        return !this.stale;
    }

    /**
     * Determine whether this transaction is read-only.
     *
     * <p>
     * This method just invokes {@link KVTransaction#isReadOnly} on the underlying key/value transaction.
     *
     * @return true if this instance is read-only
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean isReadOnly() {
        if (this.stale)
            throw new StaleTransactionException(this);
        return this.kvt.isReadOnly();
    }

    /**
     * Enable or disable read-only mode.
     *
     * <p>
     * Read-only transactions allow mutations, but all changes are discarded on {@link #commit}.
     * Registered {@link Callback}s are still processed normally.
     *
     * <p>
     * This method just invokes {@link KVTransaction#setReadOnly} on the underlying key/value transaction.
     *
     * @param readOnly read-only setting
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void setReadOnly(boolean readOnly) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.kvt.setReadOnly(readOnly);
    }

    /**
     * Determine whether this transaction is marked rollback only.
     *
     * @return true if this instance is marked for rollback only
     */
    public synchronized boolean isRollbackOnly() {
        return this.rollbackOnly;
    }

    /**
     * Mark this transaction for rollback only.
     *
     * <p>
     * Once a transaction is marked rollback only, any subsequent {@link #commit} attempt will throw an exception.
     */
    public synchronized void setRollbackOnly() {
        this.rollbackOnly = true;
    }

    /**
     * Change the timeout for this transaction from its default value (optional operation).
     *
     * <p>
     * This method just invokes {@link KVTransaction#setTimeout} on the underlying key/value transaction.
     *
     * @param timeout transaction timeout in milliseconds, or zero for unlimited
     * @throws UnsupportedOperationException if this transaction does not support timeouts
     * @throws IllegalArgumentException if {@code timeout} is negative
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void setTimeout(long timeout) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.kvt.setTimeout(timeout);
    }

    /**
     * Register a transaction {@link Callback} to be invoked when this transaction completes.
     *
     * <p>
     * Callbacks will be invoked in the order they are registered, but <i>duplicate registrations are ignored</i>
     * (based on comparison via {@link Object#equals}).
     *
     * <p>
     * Note: if you are using Spring for transaction demarcation (via {@link io.permazen.spring.PermazenTransactionManager}),
     * then you may also use Spring's
     * {@link org.springframework.transaction.support.TransactionSynchronizationManager#registerSynchronization
     * TransactionSynchronizationManager.registerSynchronization()} instead of this method.
     *
     * @param callback callback to invoke
     * @throws IllegalArgumentException if {@code callback} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void addCallback(Callback callback) {
        Preconditions.checkArgument(callback != null, "null callback");
        if (this.stale || this.ending)
            throw new StaleTransactionException(this);
        if (this.callbacks == null)
            this.callbacks = new LinkedHashSet<>();
        this.callbacks.add(callback);
    }

    /**
     * Create an in-memory detached transaction.
     *
     * <p>
     * The detached transaction will be initialized with the same schema meta-data as this instance but will be otherwise empty
     * (i.e., contain no objects). It can be used as a destination for in-memory copies of objects made via {@link #copy copy()}.
     *
     * <p>
     * The returned {@link DetachedTransaction} does not support {@link #commit} or {@link #rollback}.
     * It can be used indefinitely after this transaction closes, but it must be {@link DetachedTransaction#close close()}'d
     * when no longer needed to release any associated resources.
     *
     * @return empty in-memory detached transaction with compatible schema information
     * @see Database#createDetachedTransaction Database.createDetachedTransaction()
     */
    public synchronized DetachedTransaction createDetachedTransaction() {
        final MemoryKVStore kvstore = new MemoryKVStore();
        Layout.copyMetaData(this.kvt, kvstore);
        return new DetachedTransaction(this.db, kvstore, this.schema);
    }

    /**
     * Create a detached transaction pre-populated with a snapshot of this transaction.
     *
     * <p>
     * The returned transaction will have the same schema meta-data and object content as this instance.
     * It will be a mutable transaction, but being detached, changes can't be committed.
     *
     * <p>
     * This method requires the underlying key/value transaction to support {@link KVTransaction#readOnlySnapshot}.
     * As with any other information extracted from this transaction, the returned content is not guaranteed to be
     * valid until this transaction has been successfully committed.
     *
     * <p>
     * The returned {@link DetachedTransaction} does not support {@link #commit} or {@link #rollback}.
     * It can be used indefinitely after this transaction closes, but it must be {@link DetachedTransaction#close close()}'d
     * when no longer needed to release any associated resources.
     *
     * @return in-memory copy of this transaction
     * @throws UnsupportedOperationException if they underlying key/value transaction doesn't support
     *  {@link KVTransaction#readOnlySnapshot}
     */
    public synchronized DetachedTransaction createSnapshotTransaction() {

        // Create a snapshot
        final CloseableKVStore snapshot = this.kvt.readOnlySnapshot();

        // Make it mutable
        final MutableView mutableView = new MutableView(snapshot, false);

        // Ensure the snapshot is closed when the detached transaction is closed
        final CloseableForwardingKVStore kvstore = new CloseableForwardingKVStore(mutableView, snapshot::close);

        // Create new transaction
        return new DetachedTransaction(this.db, kvstore, this.schema);
    }

    /**
     * Determine whether this instance is a {@link DetachedTransaction}.
     *
     * @return true if this instance is a {@link DetachedTransaction}, otherwise false
     */
    public boolean isDetached() {
        return false;
    }

    /**
     * Apply weaker transaction consistency while performing the given action, if supported.
     *
     * <p>
     * Some key/value implementations support reads with weaker consistency guarantees, where reads generate fewer
     * transaction conflicts in exchange for returning possibly out-of-date information.
     *
     * <p>
     * Depending on the key/value implementation, in this mode writes may not be supported; instead, they would
     * generate a {@link IllegalStateException} or just be ignored.
     *
     * <p>
     * The weaker consistency is only applied for the current thread, and it ends when this method returns.
     *
     * <p>
     * <b>This method is for experts only</b>; inappropriate use can result in a corrupted database.
     * In general, after this method returns you should not make any changes to the database that are
     * based on any information read by the {@code action}.
     *
     * @param action the action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void withWeakConsistency(Runnable action) {
        this.kvt.withWeakConsistency(action);
    }

// Object Lifecycle

    /**
     * Create a new object with the given object ID, if it doesn't already exist.
     *
     * <p>
     * If the object already exists, nothing happens.
     *
     * <p>
     * If the object doesn't already exist, all fields are set to their default values and the object's
     * schema is set to the {@linkplain #getSchema() schema associated with this transaction}.
     *
     * @param id object ID
     * @return true if the object did not exist and was created, false if the object already existed
     * @throws UnknownTypeException if {@code id} does not correspond to an object type in this transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public boolean create(ObjId id) {
        return this.create(id, this.getSchema().getSchemaId());
    }

    /**
     * Create a new object with the given object ID, if it doesn't already exist. If it does exist, nothing happens.
     *
     * <p>
     * If the object doesn't already exist, the object's schema is set to the specified schema and all fields are set
     * to their default values.
     *
     * @param id object ID
     * @param schemaId the schema to use for the newly created object
     * @return true if the object did not exist and was created, false if the object already existed
     * @throws UnknownTypeException if {@code id} does not correspond to a known object type in the specified schema
     * @throws InvalidSchemaException if {@code schemaId} is invalid
     * @throws IllegalArgumentException if {@code id} or {@code schemaId} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean create(ObjId id, SchemaId schemaId) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(schemaId != null, "null schemaId");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Does object already exist?
        if (this.exists(id))
            return false;

        // Find object type
        final Schema objSchema = schemaId.equals(this.schema.getSchemaId()) ? this.schema : this.schemaBundle.getSchema(schemaId);
        assert objSchema != null;
        final ObjType objType = objSchema.getObjType(id.getStorageId());

        // Initialize object
        this.createObjectData(id, objSchema, objType);

        // Done
        return true;
    }

    /**
     * Create a new object with a randomly assigned object ID and having the given type.
     *
     * <p>
     * All fields will be set to their default values.
     * The object's schema will be set to {@linkplain #getSchema() the associated with this transaction}.
     *
     * @param typeName object type name
     * @return object id of newly created object
     * @throws UnknownTypeException if {@code typeName} does not correspond to a known object type in this transaction's schema
     * @throws IllegalArgumentException if {@code typeName} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public ObjId create(String typeName) {
        return this.create(typeName, this.getSchema().getSchemaId());
    }

    /**
     * Create a new object with a randomly assigned object ID and having the given type and schema.
     *
     * <p>
     * All fields will be set to their default values.
     * The object's schema will be set to the specified schema.
     *
     * @param typeName object type name
     * @param schemaId ID of the schema to use for the newly created object
     * @return object id of newly created object
     * @throws UnknownTypeException if {@code typeName} does not correspond to a known object type in the specified schema
     * @throws InvalidSchemaException if {@code schemaId} is invalid
     * @throws IllegalArgumentException if {@code id} or {@code schemaId} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized ObjId create(String typeName, SchemaId schemaId) {

        // Sanity check
        Preconditions.checkArgument(typeName != null, "null typeName");
        Preconditions.checkArgument(schemaId != null, "null schemaId");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Find object type
        final Schema objSchema = schemaId.equals(this.schema.getSchemaId()) ? this.schema : this.schemaBundle.getSchema(schemaId);
        assert objSchema != null;
        final ObjType objType = objSchema.getObjType(typeName);

        // Generate object ID
        final ObjId id = this.generateIdValidated(objType.getStorageId());

        // Initialize object
        this.createObjectData(id, objSchema, objType);

        // Done
        return id;
    }

    /**
     * Generate a random, unused {@link ObjId} for the given object type.
     *
     * @param typeName object type name
     * @return random unassigned object id
     * @throws UnknownTypeException if {@code typeName} does not correspond to any known object type
     * @throws IllegalArgumentException if {@code typeName} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized ObjId generateId(String typeName) {

        // Sanity check
        Preconditions.checkArgument(typeName != null, "null typeName");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Get storage ID
        final Integer storageId = this.schemaBundle.getStorageIdsByTypeName().get(typeName);
        if (storageId == null)
            throw new UnknownTypeException(typeName, null);

        // Generate ID
        return this.generateIdValidated(storageId);
    }

    private /*synchronized*/ ObjId generateIdValidated(int storageId) {
        assert Thread.holdsLock(this);

        // Create a new, unique key
        final ByteData.Writer keyWriter = ByteData.newWriter();
        for (int attempts = 0; attempts < MAX_GENERATED_KEY_ATTEMPTS; attempts++) {
            final ObjId id = new ObjId(storageId);
            id.writeTo(keyWriter);
            if (this.kvt.get(keyWriter.toByteData()) == null)
                return id;
            keyWriter.reset();
        }

        // Give up
        throw new DatabaseException(String.format(
          "could not find a new, unused object ID after %d attempts; is our source of randomness truly random?",
          MAX_GENERATED_KEY_ATTEMPTS));
    }

    /**
     * Initialize key/value pairs for a new object. The object must not already exist.
     */
    private synchronized void createObjectData(ObjId id, Schema schema, ObjType objType) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        assert this.kvt.get(id.getBytes()) == null;
        assert this.objInfoCache.get(id) == null;

        // Write object meta-data and update object info cache
        this.updateObjInfo(id, schema.getSchemaIndex(), schema, objType);

        // Write object schema index entry
        this.kvt.put(Layout.buildSchemaIndexKey(id, schema.getSchemaIndex()), ByteData.empty());

        // Initialize counters to zero
        if (!objType.counterFields.isEmpty()) {
            for (CounterField field : objType.counterFields.values())
                this.kvt.put(field.buildKey(id), this.kvt.encodeCounter(0));
        }

        // Write simple field index entries
        objType.indexedSimpleFields
          .forEach(field -> this.kvt.put(Transaction.buildSimpleIndexEntry(field, id, null), ByteData.empty()));

        // Write composite index entries
        for (CompositeIndex index : objType.compositeIndexes.values())
            this.kvt.put(Transaction.buildDefaultCompositeIndexEntry(id, index), ByteData.empty());

        // Notify listeners
        if (!this.disableListenerNotifications && this.createListeners != null) {
            final Set<CreateListener> objTypeCreateListeners = this.createListeners.get(objType.storageId);
            if (objTypeCreateListeners != null)
                new ArrayList<>(objTypeCreateListeners).forEach(listener -> listener.onCreate(this, id));
        }
    }

    /**
     * Delete an object. Does nothing if object does not exist (e.g., has already been deleted).
     *
     * <p>
     * This method does <i>not</i> change the object's schema if it is different from this transaction's schema.
     *
     * <p><b>Notifications</b></p>
     *
     * <p>
     * If the object exists, {@link DeleteListener}'s will be notified synchronously by this method before the object
     * is actually deleted. Therefore it's possible for a {@link DeleteListener} to (perhaps indirectly) re-entrantly
     * invoke this method with the same {@code id}. In such cases, false is returned matching {@link DeleteListener}s
     * are not (redundantly) notified.
     *
     * <p><b>Secondary Deletions</b></p>
     *
     * <p>
     * Deleting an object can trigger additional automatic secondary deletions. Specifically,
     * (a) if the object contains reference fields with {@linkplain ReferenceField#forwardDelete forward delete cascade} enabled,
     * any objects referred to through those fields will also be deleted, and (b) if the object is referred to by any other objects
     * through fields configured for {@link DeleteAction#DELETE}, those referring objects will be deleted.
     *
     * <p>
     * In any case, deletions occur one at a time, and only after an object is actually deleted do any associated secondary
     * deletions take place. However, the order in which secondary deletions occur is unspecified.
     * For an example of where this ordering matters, consider an object {@code A} referring to objects
     * {@code B} and {@code C} with delete cascading references, where B also refers to C with a {@link DeleteAction#EXCEPTION}
     * reference. Then if {@code A} is deleted, it's indeterminate whether a {@link ReferencedObjectException} will be thrown,
     * as that depends on whether {@code B} or {@code C} is deleted first (with the answer being, respectively, no and yes).
     *
     * @param id object ID of the object to delete
     * @return true if object was found and deleted, false if object does not exist or this method is
     *  being invoked re-entrantly with the same {@code id}
     * @throws ReferencedObjectException if the object is referenced by some other object
     *  through a reference field configured for {@link DeleteAction#EXCEPTION}
     * @throws IllegalArgumentException if {@code id} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean delete(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Track in-progess notifications to handle re-entrancy
        final boolean topLevel = this.deleteNotified == null;
        if (topLevel)
            this.deleteNotified = new ObjIdSet();
        else if (this.deleteNotified.contains(id))               // we are being invoked re-entrantly for the same ID
            return false;

        // Find and delete the object and notify listeners
        boolean found = false;
        try {
            final ObjIdSet deletables = new ObjIdSet();
            deletables.add(id);
            do
                found |= this.delete(deletables);
            while (!deletables.isEmpty());
        } finally {
            if (topLevel)
                this.deleteNotified = null;
        }

        // Done
        return found;
    }

    private synchronized boolean delete(ObjIdSet deletables) {

        // Get the next deletable object ID
        final ObjId id = deletables.removeOne();

        // Loop here to handle any mutations within delete notification listener callbacks
        ObjInfo info;
        while (true) {

            // See if object (still) exists
            if ((info = this.getObjInfoIfExists(id, false)) == null)
                return false;

            // Determine if any EXCEPTION reference fields refer to the object (from some other object); if so, throw exception
            for (Map.Entry<Integer, NavigableSet<ObjId>> entry : this.findReferrers(id, DeleteAction.EXCEPTION).entrySet()) {
                final int fieldStorageId = entry.getKey();
                final NavigableSet<ObjId> referrers = entry.getValue();
                for (ObjId referrer : referrers) {
                    if (!referrer.equals(id)) {
                        final ReferenceField field = this.schemaBundle.getSchemaItem(fieldStorageId, ReferenceField.class);
                        throw new ReferencedObjectException(this, id, referrer, field.getName());
                    }
                }
            }

            // Do we need to issue delete notifications for the object type being deleted?
            if (!this.deleteNotified.add(id) || this.disableListenerNotifications)
                break;
            final int objTypeStorageId = info.getObjType().storageId;
            if (this.monitorCache != null && !this.monitorCache.hasDeleteMonitor(objTypeStorageId))
                break;
            final Set<DeleteMonitor> objTypeDeleteMonitors = Optional.ofNullable(this.deleteMonitors)
              .map(map -> map.get(objTypeStorageId))
              .filter(map -> !map.isEmpty())
              .orElse(null);
            if (objTypeDeleteMonitors == null)
                break;

            // Notify delete monitors and retry
            this.monitorNotify(new DeleteNotifier(id), NavigableSets.singleton(id), new ArrayList<>(objTypeDeleteMonitors));
        }

        // Find all objects referred to by a reference field with forwardDelete = true and add them to deletables
        for (ReferenceField field : info.getObjType().referenceFieldsAndSubFields.values()) {
            if (!field.forwardDelete)
                continue;
            final Iterable<ObjId> refs = field.parent != null ?
              field.parent.iterateSubField(this, id, field) : Collections.singleton(field.getValue(this, id));
            for (ObjId ref : refs) {
                if (ref != null)
                    deletables.add(ref);
            }
        }

        // Actually delete the object
        this.deleteObjectData(info);
        this.deleteNotified.remove(id);

        // Find all NULLIFY references and nullify them, and then find all REMOVE references and remove them
        for (boolean remove : new boolean[] { false, true }) {
            final DeleteAction deleteAction = remove ? DeleteAction.REMOVE : DeleteAction.NULLIFY;
            for (Map.Entry<Integer, NavigableSet<ObjId>> entry : this.findReferrers(id, deleteAction).entrySet()) {
                final int fieldStorageId = entry.getKey();
                final NavigableSet<ObjId> referrers = entry.getValue();
                final ReferenceField field = this.schemaBundle.getSchemaItem(fieldStorageId, ReferenceField.class);
                field.getIndex().unreferenceAll(this, remove, id, referrers);
            }
        }

        // Find all DELETE references and mark the containing object for deletion (caller will call us back to actually delete)
        this.findReferrers(id, DeleteAction.DELETE).values()
          .forEach(deletables::addAll);

        // Done
        return true;
    }

    /**
     * Delete all of an object's data. The object must exist.
     *
     * <p>
     * This is the opposite of {@link #createObjectData}.
     */
    private void deleteObjectData(ObjInfo info) {

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.kvt.get(info.getId().getBytes()) != null;

        // Delete object's simple field index entries
        final ObjId id = info.getId();
        final ObjType type = info.getObjType();
        type.indexedSimpleFields
          .forEach(field -> this.kvt.remove(Transaction.buildSimpleIndexEntry(field, id, this.kvt.get(field.buildKey(id)))));

        // Delete object's composite index entries
        for (CompositeIndex index : type.compositeIndexes.values())
            this.kvt.remove(this.buildCompositeIndexEntry(id, index));

        // Delete object's complex field index entries
        for (ComplexField<?> field : type.complexFields.values())
            field.removeIndexEntries(this, id);

        // Delete object meta-data and all field content
        final ByteData minKey = info.getId().getBytes();
        final ByteData maxKey = ByteUtil.getKeyAfterPrefix(minKey);
        this.kvt.removeRange(minKey, maxKey);

        // Delete object's schema index entry
        this.kvt.remove(Layout.buildSchemaIndexKey(id, info.getSchemaIndex()));

        // Update ObjInfo cache
        this.objInfoCache.remove(id);
    }

    /**
     * Determine if an object exists.
     *
     * <p>
     * This method does <i>not</i> change the object's schema if it exists and is different from this transaction's schema.
     *
     * @param id object ID of the object to find
     * @return true if object was found, false if object was not found, or if {@code id} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized boolean exists(ObjId id) {
        return this.getObjInfoIfExists(id, false) != null;
    }

    /**
     * Copy an object into a (possibly different) transaction.
     *
     * <p>
     * This copies the object, including all of its field data, to {@code dest}. If the object already exists in {@code dest},
     * the existing copy is completely replaced, otherwise it will be created automatically.
     *
     * <p><b>Object Schemas</b></p>
     *
     * <p>
     * In order to perform the copy, {@code source}'s schema must also already exist in {@code dest}. If it does not,
     * a {@code SchemaMismatchException} is thrown.
     *
     * <p>
     * But first, if {@link migrateSchema} is true, {@code source}'s schema is first migrated to match this transaction,
     * if needed.
     *
     * <p><b>Notifications</b></p>
     *
     * <p>
     * {@link CreateListener}s in the destination transaction will be notified if the target object must be created, and
     * field change listeners in the destination transaction will be notified for non-trivial changes to the target object's
     * fields as each field is copied. These notifications may be disabled by setting {@code notifyListeners} to false.
     *
     * <p>
     * Matching {@link SchemaChangeListener}s in this transaction are notified if/when {@code source} is migrated due
     * to {@link migrateSchema} being true.
     *
     * <p><b>Deleted Assignments</b></p>
     *
     * <p>
     * If a reference field configured to {@linkplain ReferenceField#isAllowDeleted disallow deleted assignments} is copied,
     * but the referenced object does not exist in {@code dest}, then a {@link DeletedObjectException} is thrown and no copy
     * is performed. However, this presents an impossible chicken-and-egg situation when multiple objects need to be copied
     * and there are cycles in the graph of references between objects.
     *
     * <p>
     * To handle that situation, if {@code deletedAssignments} is non-null, then instead of triggering an exception,
     * illegal references to deleted objects are collected in {@code deletedAssignments}; each entry maps a deleted object
     * to (some) referring field in the copied object. This lets the caller to decide what to do about them.
     *
     * <p><b>Object ID Remapping</b></p>
     *
     * <p>
     * By default, the {@link ObjId} of {@code source} is also the {@link ObjId} of the target object in {@code dest},
     * and all reference fields are copied as-is. The optional {@code objectIdMap} allows the caller to remap these
     * {@link ObjId}s arbitrarily, as long as the {@linkplain ObjId#getStorageId implied object types} are the same.
     * If {@code objectIdMap} maps an {@link ObjId} to null, then a new, unused {@link ObjId} in {@code dest} will be
     * chosen and updated in {@code objectIdMap}.
     *
     * <p><b>Return Value</b></p>
     *
     * <p>
     * If {@code dest} is this instance, and the {@code source} is not remapped, no fields are changed and false is
     * returned, otherwise true is returned. Even if false is returned, a schema migration can still occur
     * (if {@code migrateSchema} is true), and deleted assignment checks are still applied.
     *
     * <p><b>Deadlock Avoidance</b></p>
     *
     * <p>
     * If two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock can result.
     *
     * @param source object ID of the source object in this transaction
     * @param dest destination for the copy of {@code source} (possibly this transaction)
     * @param migrateSchema whether to migrate {@code source}'s schema (if necessary) to match this transaction prior to the copy
     * @param notifyListeners whether to notify {@link CreateListener}s and field change listeners in {@code dest}
     * @param deletedAssignments if not null, where to collect assignments to deleted objects instead of throwing
     *  {@link DeletedObjectException}s; the map key is the deleted object and the map value is some referring field
     * @param objectIdMap if not null, a remapping of object ID's in this transaction to object ID's in {@code dest}
     * @return false if the target object already existed in {@code dest}, true if it was newly created
     * @throws DeletedObjectException if no object with ID equal to {@code source} exists in this transaction
     * @throws DeletedObjectException if {@code deletedAssignments} is null, and a non-null reference field in {@code source}
     *  that disallows deleted assignments contains a reference to an object that does not exist in {@code dest}
     * @throws UnknownTypeException if {@code source} or an {@link ObjId} in {@code objectIdMap} specifies an unknown object type
     * @throws IllegalArgumentException if {@code objectIdMap} maps {@code source} to a different object type
     * @throws IllegalArgumentException if {@code objectIdMap} maps the value of a reference field to an incompatible object type
     * @throws IllegalArgumentException if any parameter is null
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws SchemaMismatchException if {@code source}'s schema does not exist in {@code dest}
     * @throws SchemaMismatchException if the object's ID in {@code dest} does not match the assigned storage ID for its type
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     */
    public synchronized boolean copy(ObjId source, final Transaction dest, final boolean migrateSchema,
      final boolean notifyListeners, final ObjIdMap<ReferenceField> deletedAssignments, final ObjIdMap<ObjId> objectIdMap) {

        // Sanity check
        Preconditions.checkArgument(source != null, "null source");
        Preconditions.checkArgument(dest != null, "null dest");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Get source object info, and update schema if requested
        final ObjInfo srcInfo = this.getObjInfo(source, migrateSchema);

        // Do the copy while both transactions are locked
        synchronized (dest) {

            // Sanity check
            if (dest.stale)
                throw new StaleTransactionException(dest);

            // Copy fields
            return dest.mutateAndNotify(() -> {
                final ObjIdMap<ReferenceField> previousCopyDeletedAssignments = dest.deletedAssignments;
                dest.deletedAssignments = deletedAssignments;
                final boolean previousDisableListenerNotifications = dest.disableListenerNotifications;
                dest.disableListenerNotifications = !notifyListeners;
                try {
                    return Transaction.doCopyFields(srcInfo, Transaction.this, dest, objectIdMap);
                } finally {
                    dest.deletedAssignments = previousCopyDeletedAssignments;
                    dest.disableListenerNotifications = previousDisableListenerNotifications;
                }
            });
        }
    }

    // This method assumes both transactions are locked
    private static boolean doCopyFields(ObjInfo srcInfo, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {

        // Sanity check
        assert Thread.holdsLock(srcTx);
        assert Thread.holdsLock(dstTx);

        // Get info
        final ObjId srcId = srcInfo.getId();
        final Schema srcSchema = srcInfo.getSchema();
        final SchemaId schemaId = srcSchema.getSchemaId();
        final ObjType srcType = srcInfo.getObjType();
        final String typeName = srcType.getName();
        final SchemaBundle srcSchemaBundle = srcTx.getSchemaBundle();
        final SchemaBundle dstSchemaBundle = dstTx.getSchemaBundle();

        // Find the same schema in the destination transaction
        final Schema dstSchema;
        try {
            dstSchema = dstSchemaBundle.getSchema(schemaId);
        } catch (IllegalArgumentException e) {
            throw new SchemaMismatchException(schemaId, String.format("destination transaction has no schema \"%s\"", schemaId));
        }
        final ObjType dstType = dstSchema.getObjType(typeName);

        // Get pre-determined destination object ID, if any
        ObjId dstId = objectIdMap != null && objectIdMap.containsKey(srcId) ? objectIdMap.get(srcId) : srcId;

        // Verify the destination object ID has the right storage ID
        if (dstId != null && dstId.getStorageId() != dstType.getStorageId()) {
            throw new SchemaMismatchException(schemaId, String.format(
              "can't copy %s to %s because %s has storage ID %d but the storage ID for type \"%s\" in the"
              + " destination transaction is %d", srcId, dstId, dstId, dstId.getStorageId(), typeName, dstType.getStorageId()));
        }

        // Create destination object ID on-demand if needed, otherwise see if it already exists
        ObjInfo dstInfo;
        final boolean existed;
        if (dstId == null) {
            dstId = dstTx.generateIdValidated(dstType.getStorageId());
            objectIdMap.put(srcId, dstId);
            dstInfo = null;
            existed = false;
        } else {
            dstInfo = dstTx.getObjInfoIfExists(dstId, false);
            existed = dstInfo != null;
        }

        // If destination object already exists and needs schema migration, go through the normal migration process first
        if (existed && !dstInfo.getSchemaId().equals(schemaId)) {
            dstTx.migrateSchema(dstInfo, dstSchema);
            dstInfo = dstTx.loadIntoCache(dstId);
        }

        // Do field-by-field copy if we have to for various reasons, otherwise do fast direct copy of key/value pairs
        if (objectIdMap != null
          || srcSchema.getSchemaIndex() != dstSchema.getSchemaIndex()
          || !dstSchemaBundle.matches(srcSchemaBundle)
          || (!dstTx.disableListenerNotifications && dstTx.hasFieldMonitor(dstType))) {

            // Create destination object if it does not exist yet
            if (!existed)
                dstTx.createObjectData(dstId, dstSchema, dstType);

            // Copy fields
            for (Field<?> field : srcType.fields.values())
                field.copy(srcId, dstId, srcTx, dstTx, objectIdMap);
        } else {

            // Check for any deleted reference assignments
            for (ReferenceField field : dstType.referenceFieldsAndSubFields.values())
                field.findAnyDeletedAssignments(srcTx, dstTx, dstId);

            // We can short circuit here if source and target are the same object in the same transaction
            if (srcId.equals(dstId) && srcTx == dstTx)
                return !existed;

            // Nuke previous destination object, if any
            if (dstInfo != null)
                dstTx.deleteObjectData(dstInfo);

            // Copy object meta-data and all field content in one key range sweep
            final KeyRange srcKeyRange = KeyRange.forPrefix(srcId.getBytes());
            final ByteData.Writer dstWriter = ByteData.newWriter();
            dstWriter.write(dstId.getBytes());
            final int dstMark = dstWriter.size();
            try (CloseableIterator<KVPair> i = srcTx.kvt.getRange(srcKeyRange)) {
                while (i.hasNext()) {
                    final KVPair kv = i.next();
                    assert srcKeyRange.contains(kv.getKey());
                    dstWriter.truncate(dstMark);
                    dstWriter.write(kv.getKey().substring(ObjId.NUM_BYTES));
                    dstTx.kvt.put(dstWriter.toByteData(), kv.getValue());
                }
            }

            // Add schema index entry
            dstTx.kvt.put(Layout.buildSchemaIndexKey(dstId, dstSchema.getSchemaIndex()), ByteData.empty());

            // Create object's simple (non-subfield) field index entries
            for (SimpleField<?> field : dstType.indexedSimpleFields) {
                final ByteData fieldValue = dstTx.kvt.get(field.buildKey(dstId));     // can be null (if field has default value)
                final ByteData indexKey = Transaction.buildSimpleIndexEntry(field, dstId, fieldValue);
                dstTx.kvt.put(indexKey, ByteData.empty());
            }

            // Create object's composite index entries
            for (CompositeIndex index : dstType.compositeIndexes.values())
                dstTx.kvt.put(Transaction.buildCompositeIndexEntry(dstTx, dstId, index), ByteData.empty());

            // Create object's complex field index entries
            for (ComplexField<?> field : dstType.complexFields.values()) {
                for (SimpleField<?> subField : field.getSubFields()) {
                    if (subField.indexed)
                        field.addIndexEntries(dstTx, dstId, subField);
                }
            }
        }

        // Done
        return !existed;
    }

// CreateListener's

    /**
     * Add a {@link CreateListener} to this transaction.
     *
     * @param storageId storage ID of the object type to listen for creation
     * @param listener the listener to add
     * @throws UnknownTypeException if {@code storageId} specifies an unknown object type
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addCreateListener(int storageId, CreateListener listener) {
        this.validateListenerChange(listener);
        this.schemaBundle.getSchemaItem(storageId, ObjType.class);
        if (this.createListeners == null)
            this.createListeners = new LongMap<>();
        this.createListeners.computeIfAbsent((long)storageId, i -> new HashSet<>(1)).add(listener);
    }

    /**
     * Remove an {@link CreateListener} from this transaction.
     *
     * @param storageId storage ID of the object type to listen for creation
     * @throws UnknownTypeException if {@code storageId} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeCreateListener(int storageId, CreateListener listener) {
        this.validateListenerChange(listener);
        this.schemaBundle.getSchemaItem(storageId, ObjType.class);
        this.removeFromMappedSet(this.createListeners, storageId, listener);
    }

// DeleteListener's

    /**
     * Add a {@link DeleteListener} to this transaction.
     *
     * @param path path of reference fields (represented by storage IDs) through which to monitor for deletion;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on object deletion
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addDeleteListener(int[] path, KeyRanges[] filters, DeleteListener listener) {
        this.validateListenerChange(listener, path);
        final DeleteMonitor monitor = new DeleteMonitor(path, filters, listener);
        if (this.deleteMonitors == null)
            this.deleteMonitors = new LongMap<>();
        for (int objTypeStorageId : this.schemaBundle.getObjTypeStorageIds()) {
            final ByteData objTypeBytes = ObjId.getMin(objTypeStorageId).getBytes();
            if (new DeleteMonitorPredicate(objTypeBytes).test(monitor))
                this.deleteMonitors.computeIfAbsent((long)objTypeStorageId, i -> new HashSet<>(3)).add(monitor);
        }
    }

    /**
     * Remove a {@link DeleteListener} from this transaction.
     *
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on object deletion
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeDeleteListener(int[] path, KeyRanges[] filters, DeleteListener listener) {
        this.validateListenerChange(listener, path);
        if (this.deleteMonitors != null) {
            final DeleteMonitor monitor = new DeleteMonitor(path, filters, listener);
            for (int objTypeStorageId : this.schemaBundle.getObjTypeStorageIds()) {
                final ByteData objTypeBytes = ObjId.getMin(objTypeStorageId).getBytes();
                if (new DeleteMonitorPredicate(objTypeBytes).test(monitor))
                    this.removeFromMappedSet(this.deleteMonitors, objTypeStorageId, monitor);
            }
        }
    }

    private <M> void removeFromMappedSet(LongMap<Set<M>> map, int storageId, M monitor) {
        if (map == null)
            return;
        final Set<M> set = map.get((long)storageId);
        if (set != null && set.remove(monitor) && set.isEmpty())
            map.remove((long)storageId);
        return;
    }

// Object Schemas

    /**
     * Get the given object's {@link ObjType}.
     *
     * @param id object id
     * @return object's object type
     * @throws DeletedObjectException if no such object exists
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized ObjType getObjType(ObjId id) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(id != null, "null id");

        // Get object schema
        return this.getObjInfo(id, false).getObjType();
    }

    /**
     * Get the object type assigned to the given storage ID.
     *
     * @param storageId object type storage ID
     * @return the corresponding object type name
     * @throws UnknownTypeException if {@code storageId} specifies an unknown object type
     */
    public synchronized String getTypeName(int storageId) {
        final String typeName = this.schemaBundle.getTypeNamesByStorageId().get(storageId);
        if (typeName == null)
            throw new UnknownTypeException(String.format("storage ID %d", storageId), null);
        return typeName;
    }

    /**
     * Migrate the specified object, if necessary, to {@linkplain #getSchema() the schema associated with this transaction}.
     *
     * <p>
     * If a schema change occurs, any matching {@link SchemaChangeListener}s will be notified prior
     * to this method returning.
     *
     * @param id object ID of the object to migrate
     * @return true if the object's schema was migrated, false if it's schema already matched
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws IllegalArgumentException if {@code id} is null
     * @throws TypeNotInSchemaException if the object's type is not defined in this transaction's schema
     */
    public synchronized boolean migrateSchema(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Get object info
        final ObjInfo info = this.getObjInfo(id, false);
        if (info.getSchemaIndex() == this.schema.getSchemaIndex())
            return false;

        // Migrate schema
        this.mutateAndNotify(() -> this.migrateSchema(info, this.getSchema()));

        // Done
        return true;
    }

    /**
     * Migrate object's schema to the specified schema and notify listeners. This assumes we are locked.
     *
     * @param info original object info
     * @param newSchema schema to change to
     * @throws TypeNotInSchemaException if {@code newSchema} does not define the object's type
     */
    private synchronized void migrateSchema(ObjInfo info, final Schema newSchema) {

        // Sanity check
        assert Thread.holdsLock(this);
        final ObjId id = info.getId();
        final Schema oldSchema = info.getSchema();
        Preconditions.checkArgument(newSchema != oldSchema, "object already migrated");

        // Get old and new types
        final ObjType oldType = info.getObjType();
        final String typeName = oldType.getName();
        final ObjType newType;
        try {
            newType = newSchema.getObjType(typeName);
        } catch (UnknownTypeException e) {
            throw (TypeNotInSchemaException)new TypeNotInSchemaException(id, typeName, newSchema).initCause(e);
        }

        // Are there any matching SchemaChangeListeners registered?
        final int objTypeStorageId = oldType.storageId;
        assert oldType.storageId == newType.storageId;
        final Set<SchemaChangeListener> listeners = Optional.ofNullable(this.schemaChangeListeners)
          .map(map -> map.get((long)objTypeStorageId))
          .filter(set -> !set.isEmpty())
          .orElse(null);

        // If so, we need to remember the removed fields' values so we can provide them to listerners
        final NavigableMap<String, Object> oldValueMap = listeners != null ? new TreeMap<>() : null;

    //////// Remove the index entries corresponding to removed composite indexes

        // Remove index entries for composite indexes that are going away
        oldType.compositeIndexes.forEach((name, oldIndex) -> {
            final Index newIndex = newType.compositeIndexes.get(name);
            if (newIndex == null || !newIndex.getSchemaId().equals(oldIndex.getSchemaId()))
                this.kvt.remove(this.buildCompositeIndexEntry(id, oldIndex));
        });

    //////// Determine Field Compatibility

        // Build a mapping from old field -> compatible new field or null if none
        final HashMap<Field<?>, Field<?>> compatibleFieldMap = new HashMap<>(oldType.fields.size());
        oldType.fields.forEach((name, oldField) -> {
            final Field<?> newField = newType.fields.get(name);
            final boolean compatible = newField != null && newField.getSchemaId().equals(oldField.getSchemaId());
            compatibleFieldMap.put(oldField, compatible ? newField : null);
        });

        // Build a list of the new fields that are not compatible with some old field
        final ArrayList<Field<?>> newFieldsToReset = new ArrayList<>(newType.fields.size());
        newType.fields.forEach((name, newField) -> {
            final Field<?> oldField = oldType.fields.get(name);
            if (oldField == null || compatibleFieldMap.get(oldField) == null)
                newFieldsToReset.add(newField);
        });

    //////// Process old fields

        // Iterate over all the fields that existed in the old schema
        for (Map.Entry<Field<?>, Field<?>> entry : compatibleFieldMap.entrySet()) {
            final Field<?> oldField = entry.getKey();

            // Grab the old field's original value for the schema change notification, if any
            if (oldValueMap != null) {
                oldField.visit(new FieldSwitch<Void>() {

                    @Override
                    @SuppressWarnings("shadow")
                    public <T> Void caseSimpleField(SimpleField<T> oldField) {
                        final ByteData key = Field.buildKey(id, oldField.storageId);
                        final ByteData oldValue = Transaction.this.kvt.get(key);
                        oldValueMap.put(oldField.name, oldValue != null ?
                          oldField.encoding.read(oldValue.newReader()) : oldField.encoding.getDefaultValue());
                        return null;
                    }

                    @Override
                    @SuppressWarnings("shadow")
                    public <T> Void caseComplexField(ComplexField<T> oldField) {
                        oldValueMap.put(oldField.name, oldField.getValueReadOnlyCopy(Transaction.this, id));
                        return null;
                    }

                    @Override
                    @SuppressWarnings("shadow")
                    public Void caseCounterField(CounterField oldField) {
                        final ByteData key = Field.buildKey(id, oldField.storageId);
                        final ByteData oldValue = Transaction.this.kvt.get(key);
                        oldValueMap.put(oldField.name, oldValue != null ? Transaction.this.kvt.decodeCounter(oldValue) : 0L);
                        return null;
                    }
                });
            }

            // Reset the field's value and add/remove index entries as needed
            oldField.visit(new FieldSwitch<Void>() {

                @Override
                @SuppressWarnings("shadow")
                public Void caseReferenceField(ReferenceField oldField) {

                    // We must reset a reference to an object type that is no longer allowed by the new reference field
                    final ReferenceField newField = (ReferenceField)entry.getValue();
                    if (newField != null) {
                        final Set<Integer> xtypes = Transaction.this.findRemovedTypes(oldField, newField);
                        if (!xtypes.isEmpty()) {
                            final ObjId ref = oldField.getValue(Transaction.this, id);
                            if (ref != null && xtypes.contains(ref.getStorageId())) {

                                // Change new field to be incompatible, so it will get reset
                                entry.setValue(null);
                                newFieldsToReset.add(newField);
                            }
                        }
                    }

                    // Proceed
                    return this.caseSimpleField(oldField);
                }

                @Override
                @SuppressWarnings("shadow")
                public <T> Void caseSimpleField(SimpleField<T> oldField) {

                    // Reset field?
                    final SimpleField<?> newField = (SimpleField<?>)entry.getValue();
                    final boolean reset = newField == null;

                    // Add/remove indexes as needed
                    final ByteData key = Field.buildKey(id, oldField.storageId);
                    if (oldField.indexed && (reset || !newField.indexed)) {
                        final ByteData value = Transaction.this.kvt.get(key);
                        Transaction.this.kvt.remove(Transaction.buildSimpleIndexEntry(oldField, id, value));
                    }
                    if (newField != null && newField.indexed && (reset || !oldField.indexed)) {
                        final ByteData value = !reset ? Transaction.this.kvt.get(key) : null;
                        Transaction.this.kvt.put(Transaction.buildSimpleIndexEntry(newField, id, value), ByteData.empty());
                    }

                    // Reset field value if needed
                    if (reset)
                        Transaction.this.kvt.remove(key);
                    return null;
                }

                @Override
                @SuppressWarnings("shadow")
                public <E> Void caseComplexField(ComplexField<E> oldField) {

                    // Reset field?
                    final ComplexField<?> newField = (ComplexField<?>)entry.getValue();
                    final boolean reset = newField == null;

                    // Add/remove index entries as needed
                    final List<? extends SimpleField<?>> oldSubFields = oldField.getSubFields();
                    final List<? extends SimpleField<?>> newSubFields = !reset ? newField.getSubFields() : null;
                    for (int i = 0; i < oldSubFields.size(); i++) {
                        final SimpleField<?> oldSubField = oldSubFields.get(i);
                        final SimpleField<?> newSubField = !reset ? newSubFields.get(i) : null;

                        // We must also reset references to object types that are no longer allowed by the new reference field
                        if (!reset && oldSubField instanceof ReferenceField) {
                            final ReferenceField oldRefField = (ReferenceField)oldSubField;
                            final ReferenceField newRefField = (ReferenceField)newSubField;
                            final Set<Integer> xtypes = Transaction.this.findRemovedTypes(oldRefField, newRefField);
                            if (!xtypes.isEmpty())
                                oldField.unreferenceRemovedTypes(Transaction.this, id, oldRefField, xtypes);
                        }

                        // Add/remove sub-field indexes
                        if (oldSubField.indexed && (reset || !newSubField.indexed))
                            oldField.removeIndexEntries(Transaction.this, id, oldSubField);
                        if (!oldSubField.indexed && !reset && newSubField.indexed)
                            newField.addIndexEntries(Transaction.this, id, newSubField);
                    }

                    // Reset field value if needed. For complex fields, a "reset" is equivalent to removing the field.
                    if (reset)
                        oldField.deleteContent(Transaction.this, id);
                    return null;
                }

                @Override
                @SuppressWarnings("shadow")
                public Void caseCounterField(CounterField oldField) {

                    // Reset field value if needed
                    final CounterField newField = (CounterField)entry.getValue();
                    if (newField == null)
                        Transaction.this.kvt.remove(Field.buildKey(id, oldField.storageId));
                    return null;
                }
            });
        }

    //////// For fields that are new or were reset, initialize values and add index entries

        // Iterate over the new fields that are truly new or got reset
        for (Field<?> newField : newFieldsToReset) {
            newField.visit(new FieldSwitch<Void>() {

                @Override
                @SuppressWarnings("shadow")
                public <T> Void caseSimpleField(SimpleField<T> newField) {
                    if (newField.indexed)
                        Transaction.this.kvt.put(Transaction.buildSimpleIndexEntry(newField, id, null), ByteData.empty());
                    return null;
                }

                @Override
                @SuppressWarnings("shadow")
                public <E> Void caseComplexField(ComplexField<E> newField) {
                    return null;                // nothing to do!
                }

                @Override
                @SuppressWarnings("shadow")
                public Void caseCounterField(CounterField newField) {
                    final ByteData key = Field.buildKey(id, newField.storageId);
                    Transaction.this.kvt.put(key, Transaction.this.kvt.encodeCounter(0L));
                    return null;
                }
            });
        }

    //////// Add composite index entries for newly added composite indexes

        // Add index entries for composite indexes that are newly added
        newType.compositeIndexes.forEach((name, newIndex) -> {
            final Index oldIndex = oldType.compositeIndexes.get(name);
            if (oldIndex == null || !oldIndex.getSchemaId().equals(newIndex.getSchemaId()))
                this.kvt.put(this.buildCompositeIndexEntry(id, newIndex), ByteData.empty());
        });

    //////// Update object schema and corresponding schema index entry

        // Change object schema and update object info cache
        final int newSchemaIndex = newSchema.getSchemaIndex();
        this.updateObjInfo(id, newSchemaIndex, newSchema, newType);

        // Update object schema index entry
        final int oldSchemaIndex = oldSchema.getSchemaIndex();
        this.kvt.remove(Layout.buildSchemaIndexKey(id, oldSchemaIndex));
        this.kvt.put(Layout.buildSchemaIndexKey(id, newSchemaIndex), ByteData.empty());

    //////// Notify listeners

        // Lock down old field values map and notify listeners about schema change
        if (oldValueMap != null) {
            final SchemaId oldSchemaId = oldSchema.getSchemaId();
            final SchemaId newSchemaId = newSchema.getSchemaId();
            final NavigableMap<String, Object> immutableOldValueMap = Collections.unmodifiableNavigableMap(oldValueMap);
            for (SchemaChangeListener listener : new ArrayList<>(listeners))
                listener.onSchemaChange(this, id, oldSchemaId, newSchemaId, immutableOldValueMap);
        }
    }

    /**
     * Find storage ID's which are no longer allowed by a reference field when upgrading to the specified
     * schema and therefore need to be scrubbed during the upgrade.
     *
     * @return set of storage ID's that are no longer allowed and should be audited on upgrade
     */
    private Set<Integer> findRemovedTypes(ReferenceField oldField, ReferenceField newField) {

        // Check allowed storage IDs
        final Set<Integer> newObjectTypes = newField.getEncoding().getObjectTypeStorageIds();
        if (newObjectTypes == null)
            return Collections.emptySet();                                  // new field can refer to any type in any schema
        Set<Integer> oldObjectTypes = oldField.getEncoding().getObjectTypeStorageIds();
        if (oldObjectTypes == null)
            oldObjectTypes = this.getSchemaBundle().getObjTypeStorageIds(); // old field can refer to any type in any schema

        // Identify storage IDs which are were allowed by old field but are no longer allowed by new field
        final HashSet<Integer> removedObjectTypes = new HashSet<>(oldObjectTypes);
        removedObjectTypes.removeAll(newObjectTypes);
        return removedObjectTypes;
    }

    /**
     * Query objects by schema.
     *
     * <p>
     * This returns all objects in the database grouped by {@link Schema}. The keys in the returned
     * map are {@linkplain Schema#getSchemaIndex schema indexes}; use {@link #getSchemaBundle} and then
     * {@link SchemaBundle#getSchemasBySchemaIndex} to access the corresponding {@link Schema}s.
     *
     * @return read-only, real-time view of all database objects grouped by schema
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex1<Integer, ObjId> querySchemaIndex() {
        if (this.stale)
            throw new StaleTransactionException(this);
        return Layout.getSchemaIndex(this.kvt);
    }

// SchemaChangeListener's

    /**
     * Add a {@link SchemaChangeListener} to this transaction that listens for schema migrations
     * of the specified object type.
     *
     * @param storageId storage ID of the object type to listen to for schema changes
     * @param listener the listener to add
     * @throws UnknownTypeException if {@code storageId} specifies an unknown object type
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addSchemaChangeListener(int storageId, SchemaChangeListener listener) {
        this.validateListenerChange(listener);
        this.schemaBundle.getSchemaItem(storageId, ObjType.class);
        if (this.schemaChangeListeners == null)
            this.schemaChangeListeners = new LongMap<>();
        this.schemaChangeListeners.computeIfAbsent((long)storageId, i -> new HashSet<>(1)).add(listener);
    }

    /**
     * Remove a {@link SchemaChangeListener} from this transaction previously registered via
     * {@link addSchemaChangeListener addSchemaChangeListener()}.
     *
     * @param storageId storage ID of the object type to listen to for schema changes
     * @param listener the listener to remove
     * @throws UnknownTypeException if {@code storageId} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeSchemaChangeListener(int storageId, SchemaChangeListener listener) {
        this.validateListenerChange(listener);
        this.schemaBundle.getSchemaItem(storageId, ObjType.class);
        this.removeFromMappedSet(this.schemaChangeListeners, storageId, listener);
    }

// Object and Field Access

    /**
     * Get all objects in the database.
     *
     * <p>
     * The returned set includes objects from all schemas. Use {@link #querySchemaIndex} to access objects with a specific schema.
     *
     * <p>
     * The returned set is mutable, with the exception that {@link NavigableSet#add add()} is not supported.
     * Deleting an element results in {@linkplain #delete deleting} the corresponding object.
     *
     * @return a live view of all database objects
     * @throws StaleTransactionException if this transaction is no longer usable
     * @see #getAll(String)
     */
    public synchronized NavigableSet<ObjId> getAll() {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);

        // Return objects
        return new ObjTypeSet(this);
    }

    /**
     * Get all objects whose object type has the specified name.
     *
     * <p>
     * The returned set includes objects of the specified type from all schemas in the database.
     * The returned set is mutable, with the exception that {@link NavigableSet#add add()} is not supported.
     * Deleting an element results in {@linkplain #delete deleting} the corresponding object.
     *
     * @param typeName object type name
     * @return a live view of all database objects having the specified type
     * @throws UnknownTypeException if {@code typeName} does not correspond to any known object type
     * @throws IllegalArgumentException if {@code typeName} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @see #getAll()
     */
    public synchronized NavigableSet<ObjId> getAll(String typeName) {

        // Sanity check
        Preconditions.checkArgument(typeName != null, "null typeName");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Get storage ID
        final Integer storageId = this.schemaBundle.getStorageIdsByTypeName().get(typeName);
        if (storageId == null)
            throw new UnknownTypeException(typeName, null);

        // Return objects
        return new ObjTypeSet(this, storageId);
    }

    /**
     * Read the value of a {@link SimpleField} from an object, optionally migrating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary, prior to reading the field.
     *
     * @param id object ID of the object
     * @param name field name
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @return value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code name} exists in the object
     * @throws IllegalArgumentException if {@code id} is null
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     */
    public synchronized Object readSimpleField(ObjId id, String name, boolean migrateSchema) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        this.checkStaleFieldAccess(id, name);

        // Get object info
        final ObjInfo info = this.getObjInfo(id, migrateSchema);

        // Find field
        final SimpleField<?> field = info.getObjType().simpleFields.get(name);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), name, "simple field");

        // Read field
        final ByteData key = field.buildKey(id);
        final ByteData value = this.kvt.get(key);

        // Decode value
        return value != null ? field.encoding.read(value.newReader()) : field.encoding.getDefaultValue();
    }

    /**
     * Change the value of a {@link SimpleField} in an object, optionally updating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary, prior to writing the field.
     *
     * @param id object ID of the object
     * @param name field name
     * @param value new value for the field
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code name} exists in the object
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for the field
     * @throws IllegalArgumentException if {@code id} is null
     */
    public void writeSimpleField(final ObjId id, final String name, final Object value, final boolean migrateSchema) {
        this.mutateAndNotify(id, () -> this.doWriteSimpleField(id, name, value, migrateSchema));
    }

    private synchronized void doWriteSimpleField(ObjId id, String name, final Object newObj, boolean migrateSchema) {

        // Get object info
        Preconditions.checkArgument(name != null, "null name");
        final ObjInfo info = this.getObjInfo(id, migrateSchema);

        // Find field
        final SimpleField<?> field = info.getObjType().simpleFields.get(name);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), name, "simple field");

        // Check for deleted assignment
        if (field instanceof ReferenceField)
            this.checkDeletedAssignment(id, (ReferenceField)field, (ObjId)newObj);

        // Get new value
        final ByteData key = field.buildKey(id);
        final ByteData newValue = field.encode(newObj);

        // Before setting the new value, read the old value if one of the following is true:
        //  - The field is being monitored -> we need to filter out "changes" that don't actually change anything
        //  - The field is indexed -> we need the old value so we can remove the old index entry
        ByteData oldValue = null;
        if (field.indexed
          || field.compositeIndexMap != null
          || (!this.disableListenerNotifications && this.hasFieldMonitor(id, field.storageId))) {

            // Get old value
            oldValue = this.kvt.get(key);

            // Compare new to old value
            if (Objects.equals(oldValue, newValue))
                return;
        }

        // Update value
        if (newValue != null)
            this.kvt.put(key, newValue);
        else
            this.kvt.remove(key);

        // Update simple index, if any
        if (field.indexed) {
            this.kvt.remove(Transaction.buildSimpleIndexEntry(field, id, oldValue));
            this.kvt.put(Transaction.buildSimpleIndexEntry(field, id, newValue), ByteData.empty());
        }

        // Update affected composite indexes, if any
        if (field.compositeIndexMap != null) {
            for (CompositeIndex index : field.compositeIndexMap.keySet()) {

                // Build old composite index entry
                final ByteData.Writer oldWriter = ByteData.newWriter();
                UnsignedIntEncoder.write(oldWriter, index.storageId);
                int fieldStart = -1;
                int fieldEnd = -1;
                for (SimpleField<?> otherField : index.fields) {
                    final ByteData otherValue;
                    if (otherField == field) {                                      // end of index entry prefix
                        fieldStart = oldWriter.size();
                        otherValue = oldValue;
                    } else
                        otherValue = this.kvt.get(otherField.buildKey(id));         // can be null (if field has default value)
                    final ByteData otherValueEncoded = otherValue != null ? otherValue : otherField.encoding.getDefaultValueBytes();
                    oldWriter.write(otherValueEncoded);
                    if (otherField == field)                                        // start of index entry suffix
                        fieldEnd = oldWriter.size();
                }
                assert fieldStart != -1;
                assert fieldEnd != -1;
                id.writeTo(oldWriter);

                // Remove old composite index entry
                final ByteData oldIndexEntry = oldWriter.toByteData();
                this.kvt.remove(oldIndexEntry);

                // Capture the index entry's prefix and suffix which surround the field value
                final ByteData indexEntryPrefix = oldIndexEntry.substring(0, fieldStart);
                final ByteData indexEntrySuffix = oldIndexEntry.substring(fieldEnd);

                // Build the new composite index entry
                final ByteData newValueEncoded = newValue != null ? newValue : field.encoding.getDefaultValueBytes();
                final ByteData.Writer newWriter = ByteData.newWriter(
                  indexEntryPrefix.size() + newValueEncoded.size() + indexEntrySuffix.size());
                newWriter.write(indexEntryPrefix);
                newWriter.write(newValueEncoded);
                newWriter.write(indexEntrySuffix);

                // Add new composite index entry
                final ByteData newIndexEntry = newWriter.toByteData();
                this.kvt.put(newIndexEntry, ByteData.empty());
            }
        }

        // Notify monitors
        if (!this.disableListenerNotifications) {
            final Object oldObj = oldValue != null ? field.encoding.read(oldValue.newReader()) : field.encoding.getDefaultValue();
            this.addSimpleFieldChangeNotification(field, id, oldObj, newObj);
        }
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("unchecked")
    private <V> void addSimpleFieldChangeNotification(SimpleField<V> field, ObjId id, Object oldValue, Object newValue) {
        this.addFieldChangeNotification(new SimpleFieldChangeNotifier<>(field, id, (V)oldValue, (V)newValue));
    }

    /**
     * Check for an invalid assignment to the given reference field of a deleted object.
     *
     * @param id referring object
     * @param field reference field in referring object
     * @param targetId referred-to object that should exist
     * @throws DeletedObjectException if assignment is invalid
     */
    void checkDeletedAssignment(ObjId id, ReferenceField field, ObjId targetId) {
        assert Thread.holdsLock(this);

        // Allow null
        if (targetId == null)
            return;

        // It's possible for target to be the same object during a copy of a self-referencing object; allow it
        if (targetId.equals(id))
            return;

        // Is deleted assignment disallowed for this field?
        if (this instanceof DetachedTransaction || field.allowDeleted)
            return;

        // Is the target a deleted object?
        if (this.exists(targetId))
            return;

        // Are we copying? If so defer the check
        if (this.deletedAssignments != null) {
            this.deletedAssignments.put(targetId, field);
            return;
        }

        // Not allowed
        throw new DeletedObjectException(targetId, String.format(
          "illegal assignment to %s of reference to deleted %s",
          this.getFieldDescription(id, field.name), this.getObjDescription(targetId)));
    }

    /**
     * Build a simple index entry for the given field, object ID, and field value.
     *
     * @param field simple field
     * @param id ID of object containing the field
     * @param value encoded field value, or null for default value
     * @return index key
     */
    private static ByteData buildSimpleIndexEntry(SimpleField<?> field, ObjId id, ByteData value) {
        if (value == null)
            value = field.encoding.getDefaultValueBytes();
        final int storageId = field.storageId;
        final int entryLength = UnsignedIntEncoder.encodeLength(storageId) + value.size() + ObjId.NUM_BYTES;
        final ByteData.Writer writer = ByteData.newWriter(entryLength);
        UnsignedIntEncoder.write(writer, storageId);
        writer.write(value);
        id.writeTo(writer);
        return writer.toByteData();
    }

    /**
     * Get a description of the given object's type.
     *
     * @param id object ID
     * @return textual description of the specified object's type
     * @throws IllegalArgumentException if {@code id} is null
     */
    public String getTypeDescription(ObjId id) {
        final int storageId = id.getStorageId();
        final String typeName = this.getSchemaBundle().getTypeName(storageId);
        return typeName != null ? "type \"" + typeName + "\"" : "unknown type #" + storageId;
    }

    /**
     * Get a description of the given object.
     *
     * @param id object ID
     * @return textual description of the specified object
     * @throws IllegalArgumentException if {@code id} is null
     */
    public String getObjDescription(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return "object " + id + " (" + this.getTypeDescription(id) + ")";
    }

    /**
     * Get a description of the given object field.
     *
     * @param id object ID
     * @param name field's name
     * @return textual description of the specified object field
     * @throws IllegalArgumentException if {@code id} or {@code name} is null
     */
    public String getFieldDescription(ObjId id, String name) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        return "field \"" + name + "\" in " + this.getObjDescription(id);
    }

    private void checkStaleFieldAccess(ObjId id, String name) {
        assert Thread.holdsLock(this);
        if (this.stale) {
            throw new StaleTransactionException(this, String.format(
              "can't access %s: %s", this.getFieldDescription(id, name), StaleTransactionException.DEFAULT_MESSAGE));
        }
    }

    /**
     * Read the value of a {@link CounterField} from an object, optionally updating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary, prior to reading the field.
     *
     * @param id object ID of the object
     * @param name field name
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @return value of the counter in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link CounterField} corresponding to {@code name} exists in the object
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized long readCounterField(ObjId id, String name, boolean migrateSchema) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        this.checkStaleFieldAccess(id, name);

        // Get object info
        final ObjInfo info = this.getObjInfo(id, migrateSchema);

        // Find field
        final CounterField field = info.getObjType().counterFields.get(name);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), name, "counter field");

        // Read field
        final ByteData key = field.buildKey(id);
        final ByteData value = this.kvt.get(key);

        // Decode value
        return value != null ? this.kvt.decodeCounter(value) : 0;
    }

    /**
     * Set the value of a {@link CounterField} in an object, optionally updating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary, prior to writing the field.
     *
     * @param id object ID of the object
     * @param name field name
     * @param value new counter value
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link CounterField} corresponding to {@code name} exists in the object
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized void writeCounterField(final ObjId id, final String name, final long value, final boolean migrateSchema) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        this.checkStaleFieldAccess(id, name);

        // Get object info
        final ObjInfo info = this.getObjInfo(id, migrateSchema);

        // Find field
        final CounterField field = info.getObjType().counterFields.get(name);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), name, "counter field");

        // Set value
        final ByteData key = field.buildKey(id);
        this.kvt.put(key, this.kvt.encodeCounter(value));
    }

    /**
     * Adjust the value of a {@link CounterField} in an object by some amount, optionally updating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary, prior to adjusting the field.
     *
     * @param id object ID of the object
     * @param name field name
     * @param offset offset value to add to counter value
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link CounterField} corresponding to {@code name} exists in the object
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized void adjustCounterField(ObjId id, String name, long offset, boolean migrateSchema) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        this.checkStaleFieldAccess(id, name);

        // Optimize away non-change
        if (offset == 0)
            return;

        // Get object info
        final ObjInfo info = this.getObjInfo(id, migrateSchema);

        // Find field
        final CounterField field = info.getObjType().counterFields.get(name);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), name, "counter field");

        // Adjust counter value
        this.kvt.adjustCounter(field.buildKey(id), offset);
    }

    /**
     * Access a {@link SetField} associated with an object, optionally updating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary.
     *
     * @param id object ID of the object
     * @param name field name
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @return set field value
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link SetField} corresponding to {@code name} exists in the object
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public NavigableSet<?> readSetField(ObjId id, String name, boolean migrateSchema) {
        return this.readComplexField(id, "set field", name, migrateSchema, SetField.class, NavigableSet.class);
    }

    /**
     * Access a {@link ListField} associated with an object, optionally updating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary.
     *
     * @param id object ID of the object
     * @param name field name
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @return list field value
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link ListField} corresponding to {@code name} exists in the object
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public List<?> readListField(ObjId id, String name, boolean migrateSchema) {
        return this.readComplexField(id, "list field", name, migrateSchema, ListField.class, List.class);
    }

    /**
     * Access a {@link MapField} associated with an object, optionally updating the object's schema.
     *
     * <p>
     * If {@code migrateSchema} is true, the object will be automatically migrated to match
     * {@linkplain #getSchema() the schema associated with this transaction}, if necessary.
     *
     * @param id object ID of the object
     * @param name field name
     * @param migrateSchema true to first automatically migrate the object's schema, false to not change it
     * @return map field value
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link MapField} corresponding to {@code name} exists in the object
     * @throws TypeNotInSchemaException {@code migrateSchema} is true and the object's schema could not be migrated because
     *   the object's type does not exist in this transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public NavigableMap<?, ?> readMapField(ObjId id, String name, boolean migrateSchema) {
        return this.readComplexField(id, "map field", name, migrateSchema, MapField.class, NavigableMap.class);
    }

    /**
     * Get the key prefix in the underlying key/value store corresponding to the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>This method does not check whether the object actually exists.</li>
     *  <li>Objects utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param id object ID
     * @return the {@link KVDatabase} key corresponding to {@code id}
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws IllegalArgumentException if {@code id} is null
     * @see Field#getKey(ObjId) Field.getKey()
     * @see io.permazen.PermazenTransaction#getKey(io.permazen.PermazenObject) PermazenTransaction.getKey()
     */
    public synchronized ByteData getKey(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        this.schemaBundle.getSchemaItem(id.getStorageId(), ObjType.class);

        // Done
        return id.getBytes();
    }

    synchronized boolean hasDefaultValue(ObjId id, SimpleField<?> field) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(id != null, "null id");

        // Verify object exists
        if (!this.exists(id))
            throw new DeletedObjectException(this, id);

        // Check whether non-default value stored in field
        return this.kvt.get(field.buildKey(id)) == null;
    }

    private synchronized <F, V> V readComplexField(ObjId id, String description,
      String name, boolean migrateSchema, Class<F> fieldClass, Class<V> valueType) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        this.checkStaleFieldAccess(id, name);

        // Get object info
        final ObjInfo info = this.getObjInfo(id, migrateSchema);

        // Get field
        final ComplexField<?> field = info.getObjType().complexFields.get(name);
        if (!fieldClass.isInstance(field))
            throw new UnknownFieldException(info.getObjType(), name, description);

        // Return view
        return valueType.cast(field.getValueInternal(this, id));
    }

    /**
     * If an object exists, read in its meta-data, also migrating its schema it in the process if requested.
     *
     * @param id object ID of the object
     * @param update true to migrate object's schema to match this transaction, false to leave it alone
     * @return object info if object exists and can be updated (if requested), otherwise null
     * @throws IllegalArgumentException if {@code id} is null
     */
    private ObjInfo getObjInfoIfExists(ObjId id, boolean update) {
        assert Thread.holdsLock(this);
        try {
            return this.getObjInfo(id, update);
        } catch (DeletedObjectException | UnknownTypeException e) {
            return null;
        }
    }

    /**
     * Read an object's meta-data, migrating its schema in the process if requested.
     *
     * @param id object ID of the object
     * @param update true to migrate object's schema to match this transaction, false to leave it alone
     * @return object info
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws IllegalArgumentException if {@code id} is null
     */
    private synchronized ObjInfo getObjInfo(ObjId id, boolean update) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");

        // Load object info into cache, if not already there
        ObjInfo info = this.objInfoCache.get(id);
        if (info == null) {

            // Verify that the object type encoded within the object ID is valid
            this.schemaBundle.getSchemaItem(id.getStorageId(), ObjType.class);

            // Load the object's info into the cache (if object doesn't exist, we'll get an exception here)
            info = this.loadIntoCache(id);
        }

        // Is a schema update required?
        if (!update || info.getSchemaIndex() == this.schema.getSchemaIndex())
            return info;

        // Migrate schema
        final ObjInfo info2 = info;
        this.mutateAndNotify(() -> this.migrateSchema(info2, this.getSchema()));

        // Load (updated) object info into cache
        return this.loadIntoCache(id);
    }

    /**
     * Get the specified object's info from the object info cache, loading it if necessary.
     *
     * @throws DeletedObjectException if object does not exist
     */
    private ObjInfo loadIntoCache(ObjId id) {
        assert Thread.holdsLock(this);
        ObjInfo info = this.objInfoCache.get(id);
        if (info == null) {

            // Create info; we'll get an exception here if object does not exist
            info = new ObjInfo(this, id);

            // Add object info to the cache
            if (this.objInfoCache.size() >= MAX_OBJ_INFO_CACHE_ENTRIES)
                this.objInfoCache.removeOne();
            this.objInfoCache.put(id, info);
        }
        return info;
    }

    /**
     * Update an object's meta-data in the key/value store and in the cache.
     */
    private ObjInfo updateObjInfo(ObjId id, int schemaIndex, Schema schema, ObjType objType) {
        assert Thread.holdsLock(this);
        ObjInfo.write(this, id, schemaIndex);
        if (this.objInfoCache.size() >= MAX_OBJ_INFO_CACHE_ENTRIES)
            this.objInfoCache.removeOne();
        final ObjInfo info = new ObjInfo(this, id, schemaIndex, schema, objType);
        this.objInfoCache.put(id, info);
        return info;
    }

// Field Change Notifications

    /**
     * Monitor for changes within this transaction of the value of the given field, as seen through a path of references.
     *
     * <p>
     * When the specified field is changed in some object T, a listener notification will be delivered for each object R
     * that refers to object T through the specified path of reference fields (if {@code path} is empty, then R = T).
     * Notifications are delivered at the end of the mutation operation just prior to returning to the caller. If the listener
     * method performs additional mutation(s) which are themselves being listened for, those notifications will also be delivered
     * prior to the returning to the original caller. Therefore, care must be taken to avoid change notification dependency
     * loops when listeners can themselves mutate fields, to avoid infinite loops.
     *
     * <p>
     * The {@code filters}, if any, are applied to {@link ObjId}'s at the corresponding steps in the path: {@code filters[0]}
     * is applied to the starting objects R, {@code filters[1]} is applied to the objects reachable from R via {@code path[0]},
     * etc., up to {@code filters[path.length]}, which applies to the target objects T. {@code filters} or any element
     * therein may be null to indicate no restriction.
     *
     * <p>
     * A referring object R may refer to the changed object T through more than one sequence of objects matching {@code path};
     * if so, R will still appear only once in the {@link NavigableSet} provided to the listener (this is of course required
     * by set semantics).
     *
     * <p>
     * Although the reference back-tracking algorithm does consolidate multiple paths between the same two objects,
     * be careful to avoid an explosion of notifications when objects in the {@code path} have a high degree of fan-in.
     *
     * <p>
     * When a {@link ReferenceField} in {@code path} also happens to be the field being changed, then there is ambiguity
     * about how the set of referring objects is calculated: does it use the field's value before or after the change?
     * This API guarantees that the answer is "after the change"; however, if another listener on the same field is
     * invoked before {@code listener} and mutates any reference field(s) in {@code path}, then whether that additional
     * change is also be included in the calculation is undefined.
     *
     * <p>
     * Therefore, for consistency, avoid changing any {@link ReferenceField} from within a listener callback when that
     * field is also in some other listener's reference path, and both listeners are watching the same field.
     *
     * <p>
     * Permazen allows a field's type to change across schemas, therefore some schema may exist in which the field associated
     * with {@code storageId} is not a {@link SimpleField}. In such cases, {@code listener} will receive notifications about
     * those changes if it also happens to implement the other listener interface. In other words, this method delegates
     * directly to {@link #addFieldChangeListener addFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void addSimpleFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, SimpleFieldChangeListener listener) {
        this.addFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Monitor for changes within this transaction to the specified {@link SetField} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * <p>
     * Permazen allows a field's type to change across schemas, therefore some schema may exist in which the field associated
     * with {@code storageId} is not a {@link SetField}. In such cases, {@code listener} will receive notifications about
     * those changes if it also happens to implement the other listener interface. In other words, this method delegates
     * directly to {@link #addFieldChangeListener addFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void addSetFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, SetFieldChangeListener listener) {
        this.addFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Monitor for changes within this transaction to the specified {@link ListField} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * <p>
     * Permazen allows a field's type to change across schemas, therefore some schema may exist in which the field associated
     * with {@code storageId} is not a {@link ListField}. In such cases, {@code listener} will receive notifications about
     * those changes if it also happens to implement the other listener interface. In other words, this method delegates
     * directly to {@link #addFieldChangeListener addFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void addListFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, ListFieldChangeListener listener) {
        this.addFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Monitor for changes within this transaction to the specified {@link MapField} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * <p>
     * Permazen allows a field's type to change across schemas, therefore some schema may exist in which the field associated
     * with {@code storageId} is not a {@link MapField}. In such cases, {@code listener} will receive notifications about
     * those changes if it also happens to implement the other listener interface. In other words, this method delegates
     * directly to {@link #addFieldChangeListener addFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void addMapFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, MapFieldChangeListener listener) {
        this.addFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Monitor for changes within this transaction to the specified {@link Field} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * <p>
     * Permazen allows a field's type to change across schemas, therefore in different schemas the specified field may have
     * different types. The {@code listener} will receive notifications about a field change if it implements the interface
     * appropriate for the field's current type (i.e., {@link SimpleFieldChangeListener}, {@link ListFieldChangeListener},
     * {@link SetFieldChangeListener}, or {@link MapFieldChangeListener}) at the time of the change.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, Object listener) {
        this.validateListenerChange(listener, path);
        this.getFieldMonitorsForField(storageId, true).add(new FieldMonitor<>(storageId, path, filters, listener));
    }

    /**
     * Remove a field monitor previously added via {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()}
     * (or {@link #addFieldChangeListener addFieldChangeListener()}).
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void removeSimpleFieldChangeListener(int storageId, int[] path, KeyRanges[] filters,
      SimpleFieldChangeListener listener) {
        this.removeFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Remove a field monitor previously added via {@link #addSetFieldChangeListener addSetFieldChangeListener()}
     * (or {@link #addFieldChangeListener addFieldChangeListener()}).
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void removeSetFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, SetFieldChangeListener listener) {
        this.removeFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Remove a field monitor previously added via {@link #addListFieldChangeListener addListFieldChangeListener()}
     * (or {@link #addFieldChangeListener addFieldChangeListener()}).
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void removeListFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, ListFieldChangeListener listener) {
        this.removeFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Remove a field monitor previously added via {@link #addMapFieldChangeListener addMapFieldChangeListener()}
     * (or {@link #addFieldChangeListener addFieldChangeListener()}).
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void removeMapFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, MapFieldChangeListener listener) {
        this.removeFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Remove a field monitor previously added via {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()},
     * {@link #addSetFieldChangeListener addSetFieldChangeListener()},
     * {@link #addListFieldChangeListener addListFieldChangeListener()},
     * {@link #addMapFieldChangeListener addMapFieldChangeListener()},
     * or {@link #addFieldChangeListener addFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field;
     *  negated values denote inverse traversal of the field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, Object listener) {
        this.validateListenerChange(listener, path);
        final Set<FieldMonitor<?>> monitors = this.getFieldMonitorsForField(storageId);
        if (monitors != null)
            monitors.remove(new FieldMonitor<>(storageId, path, filters, listener));
    }

    private void validateListenerChange(Object listener, int... path) {
        assert Thread.holdsLock(this);
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(listener != null, "null listener");
        Preconditions.checkState(this.monitorCache == null, "ListenerSet installed");
        this.verifyReferencePath(path);
    }

    private Set<FieldMonitor<?>> getFieldMonitorsForField(int storageId) {
        return this.getFieldMonitorsForField(storageId, false);
    }

    private synchronized Set<FieldMonitor<?>> getFieldMonitorsForField(int storageId, boolean create) {
        Preconditions.checkArgument(storageId > 0, "invalid storageId");
        Set<FieldMonitor<?>> monitors;
        if (this.fieldMonitors == null) {
            if (!create)
                return null;
            this.fieldMonitors = new TreeMap<>();
            monitors = null;
        } else
            monitors = this.fieldMonitors.get(storageId);
        if (monitors == null) {
            if (!create)
                return null;
            monitors = new HashSet<>(1);
            this.fieldMonitors.put(storageId, monitors);
        }
        return monitors;
    }

    /**
     * Add a pending notification for any {@link FieldMonitor}s watching the specified field in the specified object.
     * This method assumes only the appropriate type of monitor is registered as a listener on the field
     * and that the provided old and new values have the appropriate types.
     *
     * <p>
     * This method should only be invoked if this.disableListenerNotifications is false.
     */
    void addFieldChangeNotification(FieldChangeNotifier<?, ?> notifier) {
        assert Thread.holdsLock(this);
        assert !this.disableListenerNotifications;

        // Get info
        final ObjId id = notifier.id;
        final int fieldStorageId = notifier.field.storageId;

        // Does anybody care?
        if (!this.hasFieldMonitor(id, fieldStorageId))
            return;

        // Add a pending field monitor notification for the specified field
        this.pendingFieldChangeNotifications.get().computeIfAbsent(fieldStorageId, i -> new ArrayList<>(2)).add(notifier);
    }

    /**
     * Determine if there are any {@link FieldMonitor}s watching the specified field in the specified object.
     */
    synchronized boolean hasFieldMonitor(ObjId id, int fieldStorageId) {

        // Do quick check, if possible
        if (this.fieldMonitors == null)
            return false;
        if (this.monitorCache != null)
            return this.monitorCache.hasFieldMonitor(id.getStorageId(), fieldStorageId);

        // Do slow check
        final Set<FieldMonitor<?>> monitorsForField = this.getFieldMonitorsForField(fieldStorageId);
        if (monitorsForField == null)
            return false;
        return monitorsForField.stream().anyMatch(new FieldMonitorPredicate(id.getStorageId(), fieldStorageId));
    }

    /**
     * Determine if there are any {@link FieldMonitor}s watching any field in the specified type.
     */
    synchronized boolean hasFieldMonitor(ObjType objType) {

        // Do quick check, if possible
        if (this.fieldMonitors == null)
            return false;
        if (this.monitorCache != null)
            return this.monitorCache.hasFieldMonitor(objType.storageId);

        // Do slow check
        final NavigableSet<Integer> fieldStorageIds = NavigableSets.intersection(
          objType.fieldsByStorageId.navigableKeySet(), this.fieldMonitors.navigableKeySet());
        final ByteData objTypeBytes = ObjId.getMin(objType.storageId).getBytes();
        for (int fieldStorageId : fieldStorageIds) {
            if (this.fieldMonitors.get(fieldStorageId).stream().anyMatch(new FieldMonitorPredicate(objTypeBytes, fieldStorageId)))
                return true;
        }
        return false;
    }

    /**
     * Verify the given object exists before proceeding with the given mutation via {@link #mutateAndNotify(Supplier)}.
     *
     * @param id object containing the mutated field; will be validated
     * @param mutation change to apply
     * @return result of mutation
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    synchronized <V> V mutateAndNotify(ObjId id, Supplier<V> mutation) {

        // Verify object exists
        Preconditions.checkArgument(id != null, "null id");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (!this.exists(id))
            throw new DeletedObjectException(this, id);

        // Perform mutation
        return this.mutateAndNotify(mutation);
    }

    synchronized void mutateAndNotify(ObjId id, Runnable mutation) {
        this.mutateAndNotify(id, () -> {
            mutation.run();
            return null;
        });
    }

    /**
     * Perform some action and, when entirely done (including re-entrant invocation), issue pending notifications to monitors.
     *
     * @param mutation change to apply
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws NullPointerException if {@code mutation} is null
     */
    private synchronized <V> V mutateAndNotify(Supplier<V> mutation) {

        // Validate transaction
        if (this.stale)
            throw new StaleTransactionException(this);

        // If re-entrant invocation, we're already set up
        if (this.pendingFieldChangeNotifications.get() != null)
            return mutation.get();

        // Set up pending report list, perform mutation, and then issue reports
        this.pendingFieldChangeNotifications.set(new TreeMap<>());
        try {
            return mutation.get();
        } finally {
            try {
                final TreeMap<Integer, ArrayList<FieldChangeNotifier<?, ?>>> pending = this.pendingFieldChangeNotifications.get();
                while (!pending.isEmpty()) {

                    // Get the next field with pending notifications
                    final Map.Entry<Integer, ArrayList<FieldChangeNotifier<?, ?>>> entry = pending.pollFirstEntry();
                    final int storageId = entry.getKey();

                    // For all pending notifications, back-track references and notify all field monitors for the field
                    for (FieldChangeNotifier<?, ?> notifier : entry.getValue()) {
                        assert notifier.field.storageId == storageId;
                        final Set<FieldMonitor<?>> monitors = this.getFieldMonitorsForField(storageId);
                        if (monitors == null || monitors.isEmpty())
                            continue;
                        this.monitorNotify(notifier, NavigableSets.singleton(notifier.id), new ArrayList<>(monitors));
                    }
                }
            } finally {
                this.pendingFieldChangeNotifications.remove();
            }
        }
    }

    private synchronized void mutateAndNotify(Runnable mutation) {
        this.mutateAndNotify(() -> {
            mutation.run();
            return null;
        });
    }

    // For each monitor, back-track references in path and notify the monitors when we reach the beginning of the path
    private void monitorNotify(Notifier<?> notifier, NavigableSet<ObjId> objects, ArrayList<Monitor<?>> monitorList) {
        this.monitorNotify(notifier, objects, monitorList, 0);
    }

    private void monitorNotify(Notifier<?> notifier, NavigableSet<ObjId> objects, ArrayList<Monitor<?>> monitorList, int step) {

        // Find the monitors for whom we have completed all the steps in their (inverse) path,
        // and group the remaining monitors by their next inverted reference path step.
        final HashMap<Integer, ArrayList<Monitor<?>>> remainingMonitorsMap = new HashMap<>();
        for (Monitor<?> monitor : monitorList) {

            // Apply the monitor's type filter on the target object, if any
            if (step == 0) {
                final KeyRanges filter = monitor.getTargetFilter();
                if (filter != null && !filter.contains(notifier.id.getBytes()))
                    continue;
            }

            // Issue notification callback if we have back-tracked through the whole path
            if (monitor.path.length == step) {
                this.monitorNotify(notifier, monitor, objects);
                continue;
            }

            // Group the unfinished monitors by their next (i.e., previous) reference field
            remainingMonitorsMap.computeIfAbsent(monitor.getStorageId(step), i -> new ArrayList<>()).add(monitor);
        }

        // Invert references for each group of remaining monitors and recurse
        for (Map.Entry<Integer, ArrayList<Monitor<?>>> entry : remainingMonitorsMap.entrySet()) {
            final int storageId = entry.getKey();
            final ArrayList<Monitor<?>> monitors = entry.getValue();
            assert monitors != null;
            final ArrayList<NavigableSet<ObjId>> refsList = new ArrayList<>(monitors.size());
            for (Monitor<?> monitor : monitors)
                refsList.addAll(this.traverseReference(objects, -storageId, monitor.getFilter(step + 1)));
            if (!refsList.isEmpty())
                this.monitorNotify(notifier, NavigableSets.union(refsList), monitors, step + 1);
        }
    }

    // Notify listener, if it has the appropriate type
    private <L> void monitorNotify(Notifier<L> notifier, Monitor<?> monitor, NavigableSet<ObjId> objects) {
        final L listener;
        try {
            listener = notifier.getListenerType().cast(monitor.listener);
        } catch (ClassCastException e) {
            return;
        }
        notifier.notify(this, listener, monitor.path, objects);
    }

// Reference Path Queries

    /**
     * Find all objects referred to by any object in the given start set through the specified path of references.
     *
     * <p>
     * Each value in {@code path} represents a reference field traversed in the path to some target object(s); if a
     * value in {@code path} is negated, then the field is traversed in the inverse direction.
     *
     * <p>
     * If {@code path} is empty, then the contents of {@code startObjects} is returned.
     *
     * <p>
     * The {@code filters}, if any, are applied to {@link ObjId}'s at the corresponding steps in the path: {@code filters[0]}
     * is applied to {@code startObjects}, {@code filters[1]} is applied to the objects reachable from {@code startObjects}
     * via {@code path[0]}, etc., up to {@code filters[path.length]}, which applies to the final target objects. {@code filters}
     * or any element therein may be null to indicate no restriction.
     *
     * @param startObjects starting objects
     * @param path path of zero or more reference fields (represented by storage IDs) through which to reach the target objects;
     *  negated values denote an inverse traversal of the corresponding reference field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @return read-only set of objects referred to by the {@code startObjects} via {@code path} restricted by {@code filters}
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code startObjects} or {@code path} is null
     * @throws IllegalArgumentException if {@code filters} is not null and does not have length {@code path.length + 1}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public NavigableSet<ObjId> followReferencePath(Stream<? extends ObjId> startObjects, int[] path, KeyRanges[] filters) {

        // Sanity check
        Preconditions.checkArgument(startObjects != null, "null startObjects");
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(filters == null || filters.length == path.length + 1, "invalid filters length");

        // Perform initial filtering
        final ObjIdSet startIds = new ObjIdSet();
        final KeyRanges firstFilter = filters != null ? filters[0] : null;
        if (firstFilter != null)
            startObjects = startObjects.filter(id -> firstFilter.contains(id.getBytes()));
        startObjects.iterator().forEachRemaining(startIds::add);
        if (path.length == 0)
            return startIds.sortedSnapshot();

        // Traverse each reference in the path
        Set<ObjId> ids = startIds;
        for (int i = 0; i < path.length; i++) {
            final int pathId = path[i];
            final KeyRanges filter = filters != null ? filters[i + 1] : null;

            // Traverse reference
            final ArrayList<NavigableSet<ObjId>> refsList = this.traverseReference(ids, pathId, filter);
            if (refsList.isEmpty())
                return NavigableSets.empty(Encodings.OBJ_ID);

            // Recurse on the union of the resulting object sets
            ids = NavigableSets.union(refsList);
        }

        // Done
        return (NavigableSet<ObjId>)ids;
    }

    /**
     * Find all objects that refer to any object in the given target set through the specified path of references.
     *
     * <p>
     * Each value in {@code path} represents a reference field traversed in the path to the target object(s); if a
     * value in {@code path} is negated, then the field is traversed in the inverse direction.
     *
     * <p>
     * If {@code path} is empty, then the contents of {@code targetObjects} is returned.
     *
     * <p>
     * The {@code filters}, if any, are applied to {@link ObjId}'s at the corresponding steps in the path:
     * {@code filters[path.length]} is applied to {@code targetObjects}, {@code filters[path.length - 1]} is applied to the
     * objects referring to {@code targetObjects} via {@code path[path.length - 1]}, etc., down to {@code filters[0]}, which
     * applies to the objects at the start of the path being inverted. {@code filters} or any element therein may be null to
     * indicate no restriction.
     *
     * @param path path of zero or more reference fields (represented by storage IDs) through which to reach the target objects;
     *  negated values denote an inverse traversal of the corresponding reference field
     * @param filters if not null, an array of length {@code path.length + 1} containing optional filters to be applied
     *  to object ID's after the corresponding steps in the path
     * @param targetObjects target objects
     * @return read-only set of objects that refer to the {@code targetObjects} via {@code path} restricted by {@code filters}
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code targetObjects} or {@code path} is null
     * @throws IllegalArgumentException if {@code filters} is not null and does not have length {@code path.length + 1}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public NavigableSet<ObjId> invertReferencePath(int[] path, KeyRanges[] filters, Stream<? extends ObjId> targetObjects) {

        // Invert path
        final int[] invertedPath = new int[path.length];
        int i = 0;
        int j = path.length;
        while (i < path.length)
            invertedPath[i++] = -path[--j];

        // Invert filters
        final KeyRanges[] invertedFilters;
        if (filters != null) {
            invertedFilters = new KeyRanges[filters.length];
            i = 0;
            j = invertedFilters.length;
            while (i < invertedFilters.length)
                invertedFilters[i++] = filters[--j];
        } else
            invertedFilters = null;

        // Follow inverted path
        return this.followReferencePath(targetObjects, invertedPath, invertedFilters);
    }

    private ArrayList<NavigableSet<ObjId>> traverseReference(Set<ObjId> objects, int referenceId, KeyRanges filter) {
        assert objects != null;

        // Check forward vs. inverse and get storage info
        final boolean inverse = referenceId < 0;
        final int storageId = inverse ? -referenceId : referenceId;
        final ReferenceField field = this.verifyReferenceField(storageId);      // just a representative
        final SimpleIndex<ObjId> fieldIndex = field.getIndex();

        // Traverse reference from each object
        final ArrayList<NavigableSet<ObjId>> refsList = new ArrayList<>();
        if (inverse) {

            // Get index and apply filter, if any
            CoreIndex1<ObjId, ObjId> index = fieldIndex.getIndex(this);
            if (filter != null)
                index = index.filter(1, filter);
            final NavigableMap<ObjId, NavigableSet<ObjId>> indexMap = index.asMap();

            // Query for each ID in the index
            for (ObjId id : objects) {
                final NavigableSet<ObjId> refs = indexMap.get(id);
                if (refs != null)
                    refsList.add(refs);
            }
        } else {
            final ObjIdSet refs = new ObjIdSet();
            final Predicate<ObjId> idFilter = filter != null ? id -> filter.contains(id.getBytes()) : null;
            for (ObjId id : objects)
                fieldIndex.readAllNonNull(this, id, refs, idFilter);
            if (!refs.isEmpty())
                refsList.add(refs.sortedSnapshot());
        }

        // Done
        return refsList;
    }

    // Verify all fields in the path are reference fields
    private void verifyReferencePath(int[] path) {
        for (int pathId : path) {
            final int storageId = pathId < 0 ? -pathId : pathId;
            this.verifyReferenceField(storageId);
        }
    }

    private ReferenceField verifyReferenceField(int storageId) {
        return this.getSchemaBundle().getSchemaItem(storageId, ReferenceField.class);
    }

// Index Queries

    /**
     * Query any simple or composite index.
     *
     * <p>
     * The returned view will have type {@link CoreIndex1}, {@link CoreIndex2}, {@link CoreIndex3}, etc.,
     * corresponding to the number of fields in the index.
     *
     * @param storageId the storage ID associated with the field (if simple) or composite index
     * @return read-only, real-time view of the index
     * @throws UnknownIndexException if no such index exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public AbstractCoreIndex<ObjId> queryIndex(int storageId) {
        return this.findIndex(storageId, Index.class).getIndex(this);
    }

    /**
     * Query a {@link SimpleIndex} to find all values stored in some field and, for each value,
     * the set of all objects having that value in the field.
     *
     * <p>
     * Use this method to acquire a plain {@link CoreIndex1} on complex sub-fields.
     *
     * @param storageId the storage ID associated with the field
     * @return read-only, real-time view of the index
     * @throws UnknownIndexException if no such index exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex1<?, ObjId> querySimpleIndex(int storageId) {
        return (CoreIndex1<?, ObjId>)this.findIndex(storageId, SimpleIndex.class).getIndex(this);
    }

    /**
     * Query a {@link ListElementIndex} to find all values stored in some list field and, for each value,
     * the set of all objects having that value as an element in the list and the corresponding list index.
     *
     * @param storageId the storage ID associated with the field
     * @return read-only, real-time view of the index
     * @throws UnknownIndexException if no such index exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex2<?, ObjId, Integer> queryListElementIndex(int storageId) {
        return (CoreIndex2<?, ObjId, Integer>)this.findIndex(storageId, ListElementIndex.class).getElementIndex(this);
    }

    /**
     * Query a {@link MapValueIndex} to find all values stored in some map field and, for each value,
     * the set of all objects having that value as a value in the map and the corresponding key.
     *
     * @param storageId the storage ID associated with the field
     * @return read-only, real-time view of the index
     * @throws UnknownIndexException if no such index exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex2<?, ObjId, ?> queryMapValueIndex(int storageId) {
        return (CoreIndex2<?, ObjId, ?>)this.findIndex(storageId, MapValueIndex.class).getValueIndex(this);
    }

    /**
     * Query a {@link CompositeIndex} on two fields to find all value tuples stored in the corresponding
     * field tuple and, for each value tuple, the set of all objects having those values in those fields.
     *
     * @param storageId the storage ID associated with the composite index
     * @return read-only, real-time view of the index
     * @throws UnknownIndexException if no such index exists
     * @throws UnknownIndexException if the index is not on two fields
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex2<?, ?, ObjId> queryCompositeIndex2(int storageId) {
        return (CoreIndex2<?, ?, ObjId>)this.findCompositeIndex(storageId, 2, CoreIndex2.class);
    }

    /**
     * Query a {@link CompositeIndex} on three fields to find all value tuples stored in the corresponding
     * field tuple and, for each value tuple, the set of all objects having those values in those fields.
     *
     * @param storageId the storage ID associated with the composite index
     * @return read-only, real-time view of the index
     * @throws UnknownIndexException if no such index exists
     * @throws UnknownIndexException if the index is not on three fields
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex3<?, ?, ?, ObjId> queryCompositeIndex3(int storageId) {
        return (CoreIndex3<?, ?, ?, ObjId>)this.findCompositeIndex(storageId, 3, CoreIndex3.class);
    }

    /**
     * Query a {@link CompositeIndex} on four fields to find all value tuples stored in the corresponding
     * field tuple and, for each value tuple, the set of all objects having those values in those fields.
     *
     * @param storageId the storage ID associated with the composite index
     * @return read-only, real-time view of the index
     * @throws UnknownIndexException if no such index exists
     * @throws UnknownIndexException if the index is not on four fields
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex4<?, ?, ?, ?, ObjId> queryCompositeIndex4(int storageId) {
        return (CoreIndex4<?, ?, ?, ?, ObjId>)this.findCompositeIndex(storageId, 4, CoreIndex4.class);
    }

    // COMPOSITE-INDEX

    private <CI extends AbstractCoreIndex<ObjId>> CI findCompositeIndex(int storageId, int numFields, Class<CI> coreIndexType) {
        final CompositeIndex index = this.findIndex(storageId, CompositeIndex.class);
        final AbstractCoreIndex<ObjId> coreIndex = index.getIndex(this);
        try {
            return coreIndexType.cast(coreIndex);
        } catch (ClassCastException e) {
            throw new UnknownIndexException(
              String.format("storage ID %d", storageId),
              String.format("the composite index \"%s\" is on %d != %d fields",
                index.getName(), index.getFields().size(), numFields));
        }
    }

    private synchronized <I extends Index> I findIndex(int storageId, Class<I> indexType) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final Index index = SimpleIndex.class.isAssignableFrom(indexType) ?
          this.schemaBundle.getSchemaItem(storageId, SimpleField.class).getIndex() :
          this.schemaBundle.getSchemaItem(storageId, Index.class);
        try {
            return indexType.cast(index);
        } catch (ClassCastException e) {
            throw new UnknownIndexException(String.format("storage ID %d", storageId),
              String.format("%s is not a %s", index, SchemaBundle.getDescription(indexType)));
        }
    }

// Internal Methods

    /**
     * Find all objects that refer to the given target object through the/any reference field with the specified
     * {@link DeleteAction}.
     *
     * <p>
     * Because different schemas can have different {@link DeleteAction}'s configured for the same field,
     * we have to iterate through each schema separately.
     *
     * @param target referred-to object
     * @param inverseDelete {@link DeleteAction} to match
     * @return mapping from reference field storage ID to the set of objects referring to {@code target}
     *  through a reference field whose {@link DeleteAction} matches {@code inverseDelete}.
     */
    @SuppressWarnings("unchecked")
    private TreeMap<Integer, NavigableSet<ObjId>> findReferrers(ObjId target, DeleteAction inverseDelete) {
        assert Thread.holdsLock(this);

        // Get target object type storage ID
        final int targetStorageId = target.getStorageId();

        // Determine which schemas actually have objects that exist; if there's only one we can slightly optimize below
        final ArrayList<Map.Entry<Integer, NavigableSet<ObjId>>> schemaList = new ArrayList<>(5);
        schemaList.addAll(this.querySchemaIndex().asMap().entrySet());
        final boolean multipleSchemas = schemaList.size() > 1;

        // Search for objects one schema at a time, and group them by reference field
        final TreeMap<Integer, Object> result = new TreeMap<>();
        for (Map.Entry<Integer, NavigableSet<ObjId>> schemaListEntry : schemaList) {
            final int schemaIndex = schemaListEntry.getKey();
            final NavigableSet<ObjId> schemaRefs = schemaListEntry.getValue();

            // Get corresponding Schema object
            final Schema nextSchema = this.schemaBundle.getSchema(schemaIndex);
            assert nextSchema != null;

            // Iterate over reference fields in this schema that have the configured DeleteAction in some object type
            nextSchema.getDeleteActionKeyRanges().get(inverseDelete).forEach((field, keyRanges) -> {

                // Do a quick check to see whether this field can possibly refer to the target object
                final Set<Integer> targetTypes = field.getEncoding().getObjectTypeStorageIds();
                if (targetTypes != null && !targetTypes.contains(targetStorageId))
                    return;

                // Build the key prefix for the target object ID in this field's index
                assert field.getIndex().storageId == field.storageId;
                final int fieldStorageId = field.storageId;
                final int prefixLength = UnsignedIntEncoder.encodeLength(fieldStorageId) + ObjId.NUM_BYTES;
                final ByteData.Writer writer = ByteData.newWriter(prefixLength);
                UnsignedIntEncoder.write(writer, fieldStorageId);
                target.writeTo(writer);
                final ByteData prefix = writer.toByteData();

                // Query the index to get all objects referring to the target object through this field (in any schema)
                final IndexSet<ObjId> indexSet = new IndexSet<>(this.kvt, Encodings.OBJ_ID, true, prefix);

                // Now restrict those referrers to only those object types where the field's DeleteAction matches (if necessary)
                NavigableSet<ObjId> referrers = keyRanges != null ? indexSet.filterKeys(keyRanges) : indexSet;

                // Anything there?
                if (referrers.isEmpty())
                    return;

                // Add these referrers, restricted to the current schema, to our list of referrers for this field
                if (multipleSchemas) {
                    ((ArrayList<NavigableSet<ObjId>>)result
                      .computeIfAbsent(fieldStorageId, i -> new ArrayList<NavigableSet<ObjId>>(schemaList.size())))
                      .add(NavigableSets.intersection(schemaRefs, referrers));
                } else
                    result.put(fieldStorageId, referrers);              // no schema restriction necessary; no list needed
            });
        }

        // If there were multiple schemas, for each reference field, take the union of the sets from each schema
        if (multipleSchemas) {
            for (Map.Entry<Integer, Object> entry : result.entrySet()) {
                final ArrayList<NavigableSet<ObjId>> list = (ArrayList<NavigableSet<ObjId>>)entry.getValue();
                final NavigableSet<ObjId> union = list.size() == 1 ? list.get(0) : NavigableSets.union(list);
                entry.setValue(union);
            }
        }

        // Return referrer sets grouped by reference field
        return (TreeMap<Integer, NavigableSet<ObjId>>)(Object)result;
    }

    private ByteData buildCompositeIndexEntry(ObjId id, CompositeIndex index) {
        return Transaction.buildCompositeIndexEntry(this, id, index);
    }

    private static ByteData buildDefaultCompositeIndexEntry(ObjId id, CompositeIndex index) {
        return Transaction.buildCompositeIndexEntry(null, id, index);
    }

    private static ByteData buildCompositeIndexEntry(Transaction tx, ObjId id, CompositeIndex index) {
        final ByteData.Writer writer = ByteData.newWriter();
        UnsignedIntEncoder.write(writer, index.storageId);
        for (SimpleField<?> field : index.fields) {
            final ByteData value = tx != null ? tx.kvt.get(field.buildKey(id)) : null;
            writer.write(value != null ? value : field.encoding.getDefaultValueBytes());
        }
        id.writeTo(writer);
        return writer.toByteData();
    }

// Listener snapshots

    /**
     * Create a read-only snapshot of all ({@link CreateListener}s, {@link DeleteListener}s, {@link SchemaChangeListener}s,
     * {@link SimpleFieldChangeListener}s, {@link SetFieldChangeListener}s, {@link ListFieldChangeListener}s, and
     * {@link MapFieldChangeListener}s currently registered on this instance.
     *
     * <p>
     * The snapshot can be applied to other transactions having compatible schemas via {@link #setListeners setListeners()}.
     * Use of a {@link ListenerSet} also allows certain internal optimizations.
     *
     * @return snapshot of listeners associated with this instance
     * @see #setListeners setListeners()
     */
    public synchronized ListenerSet snapshotListeners() {
        return new ListenerSet(this);
    }

    /**
     * Apply a snapshot created via {@link #snapshotListeners} to this instance.
     *
     * <p>
     * Any currently registered listeners are unregistered and replaced by the listeners in {@code listeners}.
     * This method may be invoked multiple times; however, once this method has been invoked, any subsequent
     * attempts to register or unregister individual listeners will result in an {@link IllegalStateException}.
     *
     * @param listeners listener set created by {@link #snapshotListeners}
     * @throws IllegalArgumentException if {@code listeners} was created from a transaction with an incompatible schema
     * @throws IllegalArgumentException if {@code listeners} is null
     */
    public synchronized void setListeners(ListenerSet listeners) {
        Preconditions.checkArgument(listeners != null, "null listeners");

        // Verify monitors are compatible with this transaction
        if ((listeners.fieldMonitors != null || listeners.deleteMonitors != null)
          && !listeners.schemaBundle.matches(this.schemaBundle))
            throw new IllegalArgumentException("listener set was created from a transaction having an incompatible schema");

        // Apply listeners to this instance
        this.schemaChangeListeners = listeners.schemaChangeListeners;
        this.createListeners = listeners.createListeners;
        this.deleteMonitors = listeners.deleteMonitors;
        this.fieldMonitors = listeners.fieldMonitors;
        this.monitorCache = listeners.monitorCache;
    }

// User Object

    /**
     * Associate an arbitrary object with this instance.
     *
     * @param obj user object
     */
    public synchronized void setUserObject(Object obj) {
        this.userObject = obj;
    }

    /**
     * Get the object with this instance by {@link #setUserObject setUserObject()}, if any.
     *
     * @return the associated user object, or null if none has been set
     */
    public synchronized Object getUserObject() {
        return this.userObject;
    }

// Callback

    /**
     * Callback interface for notification of transaction completion events.
     * Callbacks are registered with a transaction via {@link Transaction#addCallback Transaction.addCallback()},
     * and are executed in the order registered, in the same thread that just committed (or rolled back) the transaction.
     *
     * <p>
     * Modeled after Spring's {@link org.springframework.transaction.support.TransactionSynchronization} interface.
     *
     * @see Transaction#addCallback Transaction.addCallback()
     */
    public interface Callback {

        /**
         * Invoked before transaction commit (and before {@link #beforeCompletion}).
         * This method is invoked when a transaction is intended to be committed; it may however still be rolled back.
         *
         * <p>
         * Any exceptions thrown will result in a transaction rollback and be propagated to the caller.
         *
         * @param readOnly true if the transaction {@linkplain Transaction#isReadOnly is marked read-only}
         */
        void beforeCommit(boolean readOnly);

        /**
         * Invoked before transaction completion in any case (but after any {@link #beforeCommit beforeCommit()}).
         * This method is invoked whether the transaction is going to be committed or rolled back,
         * and is invoked even if {@link #beforeCommit beforeCommit()} throws an exception.
         * Typically used to clean up resources before transaction completion.
         *
         * <p>
         * Any exceptions thrown will be logged but will <b>not</b> propagate to the caller.
         */
        void beforeCompletion();

        /**
         * Invoked after successful transaction commit (and before {@link #afterCompletion afterCompletion()}).
         *
         * <p>
         * Any exceptions thrown will propagate to the caller.
         */
        void afterCommit();

        /**
         * Invoked after transaction completion (but after any {@link #afterCommit}).
         * This method is invoked in any case, whether the transaction was committed or rolled back.
         * Typically used to clean up resources after transaction completion.
         *
         * <p>
         * Any exceptions thrown will be logged but will <b>not</b> propagate to the caller.
         *
         * @param committed true if transaction was commited, false if transaction was rolled back
         */
        void afterCompletion(boolean committed);
    }

    /**
     * Adapter class for {@link Callback}.
     *
     * <p>
     * All the implementations in this class do nothing.
     */
    public static class CallbackAdapter implements Callback {

        @Override
        public void beforeCommit(boolean readOnly) {
        }

        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCommit() {
        }

        @Override
        public void afterCompletion(boolean committed) {
        }
    }

// Listeners

    /**
     * A fixed collection of listeners ({@link CreateListener}s, {@link DeleteListener}s, {@link SchemaChangeListener}s,
     * {@link SimpleFieldChangeListener}s, {@link SetFieldChangeListener}s, {@link ListFieldChangeListener}s, and
     * {@link MapFieldChangeListener}s) that can be efficiently registered on a {@link Transaction} all at once.
     *
     * <p>
     * To create an instance of this class, use {@link Transaction#snapshotListeners} after registering the desired
     * set of listeners. Once created, the instance can be used repeatedly to configure the same set of listeners
     * on any other compatible {@link Transaction} via {@link Transaction#setListeners Transaction.setListeners()},
     * where "compatible" means having the same {@link SchemaBundle}.
     */
    public static final class ListenerSet {

        final LongMap<Set<SchemaChangeListener>> schemaChangeListeners;
        final LongMap<Set<CreateListener>> createListeners;
        final LongMap<Set<DeleteMonitor>> deleteMonitors;
        final NavigableMap<Integer, Set<FieldMonitor<?>>> fieldMonitors;
        final MonitorCache monitorCache;
        final SchemaBundle schemaBundle;

        private ListenerSet(Transaction tx) {
            assert Thread.holdsLock(tx);
            this.schemaChangeListeners = this.deepCopyReadOnly(tx.schemaChangeListeners);
            this.createListeners = this.deepCopyReadOnly(tx.createListeners);
            this.deleteMonitors = this.deepCopyReadOnly(tx.deleteMonitors);
            this.fieldMonitors = this.deepCopyReadOnly(tx.fieldMonitors);
            this.monitorCache = tx.buildMonitorCache();
            this.schemaBundle = tx.schemaBundle;
        }

        private <K, E> NavigableMap<K, Set<E>> deepCopyReadOnly(NavigableMap<K, Set<E>> map) {
            if (map == null)
                return null;
            final TreeMap<K, Set<E>> copy = new TreeMap<>(map.comparator());
            for (Map.Entry<K, Set<E>> entry : map.entrySet())
                copy.put(entry.getKey(), this.deepCopyReadOnly(entry.getValue()));
            return new ImmutableNavigableMap<>(copy);
        }

        private <E> LongMap<Set<E>> deepCopyReadOnly(LongMap<Set<E>> map) {
            if (map == null)
                return null;
            final LongMap<Set<E>> copy = map.clone();
            copy.entrySet().stream().forEach(entry -> entry.setValue(this.deepCopyReadOnly(entry.getValue())));
            return copy;
        }

        private <E> Set<E> deepCopyReadOnly(Set<E> set) {
            if (set == null)
                return null;
            return Collections.unmodifiableSet(new HashSet<>(set));
        }
    }

// MonitorPredicate

    private abstract static class MonitorPredicate<M extends Monitor<?>> implements Predicate<M> {

        private final ByteData objTypeBytes;

        MonitorPredicate(ByteData objTypeBytes) {
            this.objTypeBytes = objTypeBytes;
        }

        MonitorPredicate(int objTypeStorageId) {
            this(ObjId.getMin(objTypeStorageId).getBytes());
        }

        @Override
        public boolean test(M monitor) {
            final KeyRanges filter = monitor.getTargetFilter();
            return filter == null || filter.contains(this.objTypeBytes);
        }
    }

// FieldMonitorPredicate

    // Matches FieldMonitors who monitor the specified field in the specified object type
    private static final class FieldMonitorPredicate extends MonitorPredicate<FieldMonitor<?>> {

        private final int fieldStorageId;

        FieldMonitorPredicate(ByteData objTypeBytes, int fieldStorageId) {
            super(objTypeBytes);
            this.fieldStorageId = fieldStorageId;
        }

        FieldMonitorPredicate(int objTypeStorageId, int fieldStorageId) {
            super(objTypeStorageId);
            this.fieldStorageId = fieldStorageId;
        }

        @Override
        public boolean test(FieldMonitor<?> monitor) {
            return monitor.storageId == this.fieldStorageId && super.test(monitor);
        }
    }

// DeleteMonitorPredicate

    // Matches DeleteMonitors who monitor the specified object type
    private static final class DeleteMonitorPredicate extends MonitorPredicate<DeleteMonitor> {

        DeleteMonitorPredicate(ByteData objTypeBytes) {
            super(objTypeBytes);
        }

        DeleteMonitorPredicate(int objTypeStorageId) {
            super(objTypeStorageId);
        }
    }

// MonitorCache

    //
    // This provides a way to do a quick check for the existence of field and delete monitors.
    // Each long flag in the set is split decoded as two 32-bit integers as follows:
    //
    //  [Hi Bits, Lo Bits]          Decode                      Meaning
    //  ---------------------       -------                     -------
    //  [ Positive, Positive ]      Hi = ObjType, Lo = Field    FieldMonitor exists for that type and that field
    //  [ Positive, Zero     ]      Hi = ObjType                FieldMonitor exists for that type and some field
    //  [ Positive, -1       ]      Hi = ObjType                DeleteMonitor exists for that type
    //
    @SuppressWarnings("serial")
    private static class MonitorCache extends LongSet {

        public boolean hasFieldMonitor(int objTypeStorageId) {
            return this.contains(this.buildKey(objTypeStorageId, 0));
        }

        public boolean hasFieldMonitor(int objTypeStorageId, int fieldStorageId) {
            return this.contains(this.buildKey(objTypeStorageId, fieldStorageId));
        }

        public boolean hasDeleteMonitor(int objTypeStorageId) {
            return this.contains(this.buildKey(objTypeStorageId, -1));
        }

        public void addDeleteMonitor(int objTypeStorageId) {
            this.add(this.buildKey(objTypeStorageId, -1));
        }

        public void addFieldMonitor(int objTypeStorageId, int fieldStorageId) {
            this.add(this.buildKey(objTypeStorageId, fieldStorageId));
            this.add(this.buildKey(objTypeStorageId, 0));
        }

        private long buildKey(int objTypeStorageId, int fieldStorageId) {
            assert objTypeStorageId > 0;
            assert fieldStorageId >= -1;
            return ((long)objTypeStorageId << 32) | ((long)fieldStorageId & 0xffffffffL);
        }
    }

    /**
     * Build a {@link MonitorCache} based on this transaction's current monitors.
     */
    private synchronized MonitorCache buildMonitorCache() {
        final MonitorCache cache = new MonitorCache();
        for (Schema otherSchema : this.schemaBundle.getSchemasBySchemaId().values()) {
            for (ObjType objType : otherSchema.getObjTypes().values()) {
                final int objTypeStorageId = objType.storageId;
                final ByteData objTypeBytes = ObjId.getMin(objTypeStorageId).getBytes();

                // Add flags for FieldMonitors
                for (Field<?> field : objType.fieldsAndSubFields.values()) {
                    final int fieldStorageId = field.storageId;
                    final Set<FieldMonitor<?>> monitors = this.getFieldMonitorsForField(fieldStorageId);
                    if (monitors != null && monitors.stream().anyMatch(new FieldMonitorPredicate(objTypeBytes, fieldStorageId)))
                        cache.addFieldMonitor(objTypeStorageId, fieldStorageId);
                }

                // Add flags for DeleteMonitors
                if (Optional.ofNullable(this.deleteMonitors)
                  .map(map -> map.get(objTypeStorageId))
                  .filter(map -> !map.isEmpty())
                  .isPresent())
                    cache.addDeleteMonitor(objTypeStorageId);
            }
        }
        return cache;
    }
}
