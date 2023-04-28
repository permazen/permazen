
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.core.type.ReferenceFieldType;
import io.permazen.core.util.ObjIdMap;
import io.permazen.core.util.ObjIdSet;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.util.NavigableMapKVStore;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
import io.permazen.util.CloseableIterator;
import io.permazen.util.NavigableSets;
import io.permazen.util.UnsignedIntEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Permazen {@link Database} transaction.
 *
 * <p>
 * Note: this is the lower level, core API for {@link io.permazen.Permazen}. In most cases this API
 * will only be used indirectly through the higher level {@link io.permazen.Permazen}, {@link io.permazen.JTransaction},
 * and {@link io.permazen.JObject} APIs.
 *
 * <p>
 * Methods in this class can be divided into the following categories:
 *
 * <p>
 * <b>Transaction Meta-Data</b>
 * <ul>
 *  <li>{@link #getDatabase getDatabase()} - Get the associated {@link Database}</li>
 *  <li>{@link #getKVTransaction getKVTransaction()} -  Get the underlying key/value store transaction.</li>
 *  <li>{@link #getSchemas getSchemas()} - Get the database {@link Schemas}, as seen by this transaction</li>
 *  <li>{@link #getSchema() getSchema()} - Get the {@link Schema} that will be used by this transaction</li>
 *  <li>{@link #deleteSchemaVersion deleteSchemaVersion()} - Delete a schema version that is no longer being used</li>
 *  <li>{@link #getUserObject} - Get user object associated with this instance</li>
 *  <li>{@link #setUserObject} - Set user object associated with this instance</li>
 * </ul>
 *
 * <p>
 * <b>Transaction Lifecycle</b>
 * <ul>
 *  <li>{@link #commit commit()} - Commit transaction</li>
 *  <li>{@link #rollback rollback()} - Roll back transaction</li>
 *  <li>{@link #isValid isValid()} - Test transaction validity</li>
 *  <li>{@link #setTimeout setTimeout()} - Set transaction timeout</li>
 *  <li>{@link #setReadOnly setReadOnly()} - Set transaction to read-only</li>
 *  <li>{@link #setRollbackOnly setRollbackOnly()} - Set transaction for rollack only</li>
 *  <li>{@link #addCallback addCallback()} - Register a {@link Callback} on transaction completion</li>
 *  <li>{@link #createSnapshotTransaction createSnapshotTransaction()} - Create a empty, in-memory copy of this transaction</li>
 *  <li>{@link #isSnapshot} - Determine whether this transaction is a snapshot transaction</li>
 * </ul>
 *
 * <p>
 * <b>Object Lifecycle</b>
 * <ul>
 *  <li>{@link #create(int) create()} - Create a database object</li>
 *  <li>{@link #delete delete()} - Delete a database object</li>
 *  <li>{@link #exists exists()} - Test whether a database object exists</li>
 *  <li>{@link #copy copy()} - Copy an object into a (possibly different) transaction</li>
 *  <li>{@link #addCreateListener addCreateListener()} - Register a {@link CreateListener} for notifications about new objects</li>
 *  <li>{@link #removeCreateListener removeCreateListener()} - Unregister a {@link CreateListener}</li>
 *  <li>{@link #addDeleteListener addDeleteListener()} - Register a {@link DeleteListener} for notifications
 *      about object deletions</li>
 *  <li>{@link #removeDeleteListener removeDeleteListener()} - Unregister a {@link DeleteListener}</li>
 * </ul>
 *
 * <p>
 * <b>Object Versioning</b>
 * <ul>
 *  <li>{@link #getSchemaVersion(ObjId) getSchemaVersion()} - Inspect an object's schema version</li>
 *  <li>{@link #updateSchemaVersion updateSchemaVersion()} - Update an object's schema version to match this transaction</li>
 *  <li>{@link #queryVersion queryVersion()} - Locate objects by schema version</li>
 *  <li>{@link #addVersionChangeListener addVersionChangeListener()} - Register a {@link VersionChangeListener}
 *      for notifications about object version changes</li>
 *  <li>{@link #removeVersionChangeListener removeVersionChangeListener()} - Unregister a {@link VersionChangeListener}</li>
 * </ul>
 *
 * <p>
 * <b>Object and Field Access</b>
 * <ul>
 *  <li>{@link #getAll getAll(int)} - Get all objects, or all objects of a specific type</li>
 *  <li>{@link #readSimpleField readSimpleField()} - Read the value of a {@link SimpleField} in an object</li>
 *  <li>{@link #writeSimpleField writeSimpleField()} - Write the value of a {@link SimpleField} in an object</li>
 *  <li>{@link #readCounterField readCounterField()} - Read the value of a {@link CounterField} in an object</li>
 *  <li>{@link #writeCounterField writeCounterField()} - Write the value of a {@link CounterField} in an object</li>
 *  <li>{@link #adjustCounterField adjustCounterField()} - Adjust the value of a {@link CounterField} in an object</li>
 *  <li>{@link #readSetField readSetField()} - Access a {@link SetField} in an object as a {@link NavigableSet}</li>
 *  <li>{@link #readListField readListField()} - Access a {@link ListField} in an object as a {@link List}</li>
 *  <li>{@link #readMapField readMapField()} - Access a {@link MapField} in an object as a {@link NavigableMap}</li>
 *  <li>{@link #getKey getKey(ObjId)} - Get the {@link io.permazen.kv.KVDatabase} key corresponding to an object</li>
 *  <li>{@link #getKey getKey(ObjId, int)} - Get the {@link io.permazen.kv.KVDatabase}
 *      key corresponding to a field in an object</li>
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
 * <b>Index Queries</b>
 * <ul>
 *  <li>{@link #queryIndex queryIndex()} - Query the index associated with a {@link SimpleField}
 *      to identify all values and all objects having those values</li>
 *  <li>{@link #queryListElementIndex queryListElementIndex()} - Query the index associated with a {@link ListField}
 *      element sub-field to identify all list elements, all objects having those elements in the list,
 *      and their corresponding indices</li>
 *  <li>{@link #queryMapValueIndex queryMapValueIndex()} - Query the index associated with a {@link MapField}
 *      value sub-field to identify all map values, all objects having those values in the map, and the corresponding keys</li>
 *  <li>{@link #queryCompositeIndex queryCompositeIndex()} - Query any composite index</li>
 *  <li>{@link #queryCompositeIndex2 queryCompositeIndex2()} - Query a composite index on two fields</li>
 *  <li>{@link #queryCompositeIndex3 queryCompositeIndex3()} - Query a composite index on three fields</li>
 *  <li>{@link #queryCompositeIndex3 queryCompositeIndex4()} - Query a composite index on four fields</li>
 *  <!-- COMPOSITE-INDEX -->
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
 * {@link NavigableSets#symmetricDifference symmetric difference} of {@link NavigableSet}s containing the same element type,
 * thereby providing the equivalent of traditional database joins.
 */
@ThreadSafe
public class Transaction {

    private static final int MAX_GENERATED_KEY_ATTEMPTS
      = Integer.parseInt(System.getProperty(Transaction.class.getName() + ".MAX_GENERATED_KEY_ATTEMPTS", "64"));
    private static final int MAX_OBJ_INFO_CACHE_ENTRIES
      = Integer.parseInt(System.getProperty(Transaction.class.getName() + ".MAX_OBJ_INFO_CACHE_ENTRIES", "1000"));

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    // Meta-data
    final Database db;
    final KVTransaction kvt;
    final Schemas schemas;
    final Schema schema;

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
    private Set<VersionChangeListener> versionChangeListeners;
    @GuardedBy("this")
    private Set<CreateListener> createListeners;
    @GuardedBy("this")
    private Set<DeleteListener> deleteListeners;
    @GuardedBy("this")
    private NavigableMap<Integer, Set<FieldMonitor>> monitorMap;                    // key is field's storage ID
    @GuardedBy("this")
    private NavigableSet<Long> hasFieldMonitorCache;                                // optimization for hasFieldMonitor()
    @GuardedBy("this")
    private boolean listenerSetInstalled;

    // Callbacks
    @GuardedBy("this")
    private LinkedHashSet<Callback> callbacks;

    // Misc
    @GuardedBy("this")
    private final ThreadLocal<TreeMap<Integer, ArrayList<FieldChangeNotifier<?>>>> pendingNotifications = new ThreadLocal<>();
    @GuardedBy("this")
    private final ObjIdMap<ObjInfo> objInfoCache = new ObjIdMap<>();
    @GuardedBy("this")
    private Object userObject;

    // Recording of deleted assignments used during a copy() operation (otherwise should be null)
    private ObjIdMap<ReferenceField> deletedAssignments;
    private ObjIdMap<ObjId> copyIdMap;

// Constructors

    Transaction(Database db, KVTransaction kvt, Schemas schemas) {
        this(db, kvt, schemas, schemas.versions.lastKey());
    }

    Transaction(Database db, KVTransaction kvt, Schemas schemas, int versionNumber) {
        this(db, kvt, schemas, schemas.getVersion(versionNumber));
    }

    Transaction(Database db, KVTransaction kvt, Schemas schemas, Schema schema) {
        assert db != null;
        assert kvt != null;
        assert schemas != null;
        assert schema != null;
        assert schema == schemas.getVersion(schema.versionNumber);
        this.db = db;
        this.kvt = kvt;
        this.schemas = schemas;
        this.schema = schema;
    }

// Transaction Meta-Data

    /**
     * Get the database with which this transaction is associated.
     *
     * @return associated database
     */
    public Database getDatabase() {
        return this.db;
    }

    /**
     * Get the database schema versions known to this transaction.
     * This reflects all schema versions currently recorded in the database.
     *
     * @return associated schemas
     */
    public Schemas getSchemas() {
        return this.schemas;
    }

    /**
     * Get the database schema version associated with this transaction.
     * This is the schema version used for newly created objects, and the target schema version when
     * {@linkplain #updateSchemaVersion upgrading} objects.
     *
     * @return associated schema
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Delete a schema version that is no longer being used. There must be no objects with the given version
     * in the database, and {@code version} must not be the version being used by this transaction.
     *
     * @param version schema version to remove
     * @return true if schema version was found and removed, false if schema version does not exist in database
     * @throws InvalidSchemaException if one or more objects with schema version {@code version} still exist
     * @throws InvalidSchemaException if {@code version} is equal to this transaction's version
     * @throws IllegalArgumentException if {@code version} is zero or negative
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean deleteSchemaVersion(int version) {

        // Sanity check
        Preconditions.checkArgument(version > 0, "invalid non-positive schema version");
        Preconditions.checkArgument(version != this.schema.getVersionNumber(), "version is this transaction's version");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.queryVersion().asMap().containsKey(version))
            throw new InvalidSchemaException("one or more version " + version + " objects still exist in database");

        // Delete schema version
        if (!this.schemas.deleteVersion(version))
            return false;
        this.kvt.remove(Layout.getSchemaKey(version));
        return true;
    }

    /**
     * Get the underlying key/value store transaction.
     *
     * <p>
     * <b>Warning:</b> making changes to the key/value store directly is not supported. If any changes
     * are made, all future behavior is undefined.
     *
     * @return the associated key/value transaction
     */
    public KVTransaction getKVTransaction() {
        return this.kvt;
    }

// Transaction Lifecycle

    /**
     * Commit this transaction.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws io.permazen.kv.RetryTransactionException from {@link KVTransaction#commit KVTransaction.commit()}
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
     * Determine whether this transaction is still valid. If not, all other methods in this class
     * will throw {@link StaleTransactionException}.
     *
     * @return true if this instance is still usable
     */
    public synchronized boolean isValid() {
        return !this.stale;
    }

    /**
     * Determine whether this transaction is read-only.
     *
     * <p>
     * This method just invokes {@link KVTransaction#isReadOnly} on the wrapped key/value transaction.
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
     * This method just invokes {@link KVTransaction#setReadOnly} on the wrapped key/value transaction.
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
     * Mark this transaction for rollback only. A subsequent attempt to {@link #commit} will throw an exception.
     */
    public synchronized void setRollbackOnly() {
        this.rollbackOnly = true;
    }

    /**
     * Change the timeout for this transaction from its default value (optional operation).
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
     * Callbacks will be invoked in the order they are registered, but <i>duplicate registrations are ignored</i>.
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
     * Create an empty, in-memory "snapshot" transaction.
     *
     * <p>
     * The snapshot transaction will be initialized with the same schema meta-data as this instance but will be otherwise empty
     * (i.e., contain no objects). It can be used as a destination for "snapshot" copies of objects made via {@link #copy copy()}.
     *
     * <p>
     * The returned {@link SnapshotTransaction} does not support {@link #commit}, {@link #rollback},
     * or {@link #addCallback addCallback()}, and can be used indefinitely after this transaction closes.
     *
     * @return empty in-memory snapshot transaction with compatible schema information
     * @see Database#createSnapshotTransaction Database.createSnapshotTransaction()
     */
    public SnapshotTransaction createSnapshotTransaction() {
        final NavigableMapKVStore kvstore = new NavigableMapKVStore();
        Layout.copyMetaData(this.kvt, kvstore);
        return new SnapshotTransaction(this.db, kvstore, this.schemas, this.schema);
    }

    /**
     * Determine whether this instance is a {@link SnapshotTransaction}.
     *
     * @return true if this instance is a {@link SnapshotTransaction}, otherwise false
     */
    public boolean isSnapshot() {
        return false;
    }

// Object Lifecycle

    /**
     * Create a new object with the given object ID, if it doesn't already exist. If it does exist, nothing happens.
     *
     * <p>
     * If the object doesn't already exist, all fields are set to their default values and the object's
     * schema version is set to {@linkplain #getSchema() the version associated with this transaction}.
     *
     * @param id object ID
     * @return true if the object did not exist and was created, false if the object already existed
     * @throws UnknownTypeException if {@code id} does not correspond to a known object type in this transaction's schema version
     * @throws IllegalArgumentException if {@code id} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public boolean create(ObjId id) {
        return this.create(id, this.schema.versionNumber);
    }

    /**
     * Create a new object with the given object ID, if it doesn't already exist. If it does exist, nothing happens.
     *
     * <p>
     * If the object doesn't already exist, all fields are set to their default values and the object's
     * schema version is set to the specified version.
     *
     * @param id object ID
     * @param versionNumber schema version number to use for newly created object
     * @return true if the object did not exist and was created, false if the object already existed
     * @throws UnknownTypeException if {@code id} does not correspond to a known object type in the specified schema version
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code versionNumber} is invalid or unknown
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean create(ObjId id, int versionNumber) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Does object already exist?
        if (this.exists(id))
            return false;

        // Initialize object
        final Schema objSchema = versionNumber == this.schema.versionNumber ? this.schema : this.schemas.getVersion(versionNumber);
        final ObjType objType = objSchema.getObjType(id.getStorageId());
        this.createObjectData(id, versionNumber, objSchema, objType);

        // Done
        return true;
    }

    /**
     * Create a new object with a randomly assigned object ID and having the given type.
     *
     * <p>
     * All fields will be set to their default values.
     * The object's schema version will be set to {@linkplain #getSchema() the version associated with this transaction}.
     *
     * @param storageId object type storage ID
     * @return object id of newly created object
     * @throws UnknownTypeException if {@code storageId} does not correspond to a known object type in this transaction's schema
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public ObjId create(int storageId) {
        return this.create(storageId, this.schema.versionNumber);
    }

    /**
     * Create a new object with a randomly assigned object ID and having the given type and schema version.
     *
     * <p>
     * All fields will be set to their default values.
     * The object's schema version will be set to the specified version.
     *
     * @param storageId object type storage ID
     * @param versionNumber schema version number to use for newly created object
     * @return object id of newly created object
     * @throws UnknownTypeException if {@code storageId} does not correspond to a known object type in the specified schema version
     * @throws IllegalArgumentException if {@code versionNumber} is invalid or unknown
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized ObjId create(int storageId, int versionNumber) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        final Schema objSchema = versionNumber == this.schema.versionNumber ? this.schema : this.schemas.getVersion(versionNumber);
        final ObjType objType = objSchema.getObjType(storageId);

        // Generate object ID
        final ObjId id = this.generateIdValidated(storageId);

        // Initialize object
        this.createObjectData(id, versionNumber, objSchema, objType);

        // Done
        return id;
    }

    /**
     * Generate a random, unused {@link ObjId} for the given storage ID.
     *
     * @param storageId object type storage ID
     * @return random unassigned object id
     * @throws UnknownTypeException if {@code storageId} does not correspond to any known object type
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized ObjId generateId(int storageId) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        final ObjTypeStorageInfo info = this.schemas.verifyStorageInfo(storageId, ObjTypeStorageInfo.class);

        // Generate ID
        return this.generateIdValidated(info.storageId);
    }

    private /*synchronized*/ ObjId generateIdValidated(int storageId) {
        assert Thread.holdsLock(this);

        // Create a new, unique key
        final ByteWriter keyWriter = new ByteWriter();
        for (int attempts = 0; attempts < MAX_GENERATED_KEY_ATTEMPTS; attempts++) {
            final ObjId id = new ObjId(storageId);
            id.writeTo(keyWriter);
            if (this.kvt.get(keyWriter.getBytes()) == null)
                return id;
            keyWriter.reset(0);
        }

        // Give up
        throw new DatabaseException("could not find a new, unused object ID after "
          + MAX_GENERATED_KEY_ATTEMPTS + " attempts; is our source of randomness truly random?");
    }

    /**
     * Initialize key/value pairs for a new object. The object must not already exist.
     */
    private synchronized void createObjectData(ObjId id, int versionNumber, Schema schema, ObjType objType) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        assert this.kvt.get(id.getBytes()) == null;
        assert this.objInfoCache.get(id) == null;

        // Write object meta-data and update object info cache
        ObjInfo.write(this, id, versionNumber, false);
        if (this.objInfoCache.size() >= MAX_OBJ_INFO_CACHE_ENTRIES)
            this.objInfoCache.removeOne();
        this.objInfoCache.put(id, new ObjInfo(this, id, versionNumber, false, schema, objType));

        // Write object version index entry
        this.kvt.put(Layout.buildVersionIndexKey(id, objType.schema.versionNumber), ByteUtil.EMPTY);

        // Initialize counters to zero
        if (!objType.counterFields.isEmpty()) {
            for (CounterField field : objType.counterFields.values())
                this.kvt.put(field.buildKey(id), this.kvt.encodeCounter(0));
        }

        // Write simple field index entries
        objType.indexedSimpleFields
          .forEach(field -> this.kvt.put(Transaction.buildSimpleIndexEntry(field, id, null), ByteUtil.EMPTY));

        // Write composite index entries
        for (CompositeIndex index : objType.compositeIndexes.values())
            this.kvt.put(Transaction.buildDefaultCompositeIndexEntry(id, index), ByteUtil.EMPTY);

        // Notify listeners
        if (!this.disableListenerNotifications && this.createListeners != null) {
            for (CreateListener listener : this.createListeners.toArray(new CreateListener[this.createListeners.size()]))
                listener.onCreate(this, id);
        }
    }

    /**
     * Delete an object. Does nothing if object does not exist (e.g., has already been deleted).
     *
     * <p>
     * This method does <i>not</i> change the object's schema version if it has a different version that this transaction.
     *
     * <p><b>Secondary Deletions</b></p>
     *
     * <p>
     * Deleting an object can trigger additional secondary deletions. Specifically,
     * (a) if the object contains reference fields with {@linkplain ReferenceField#cascadeDelete delete cascade} enabled,
     * any objects referred to through those fields will also be deleted, and (b) if the object is referred to by any other objects
     * through fields configured for {@link DeleteAction#DELETE}, those referring objects will be deleted.
     *
     * <p>
     * In any case, deletions occur one at a time, and only when an object is actually deleted are any associated secondary
     * deletions added to an internal deletion queue. However, the order in which objects on this deletion queue are
     * processed is unspecified. For an example of where this ordering matters, consider an object {@code A} referring to objects
     * {@code B} and {@code C} with delete cascading references, where B also refers to C with a {@link DeleteAction#EXCEPTION}
     * reference. Then if {@code A} is deleted, it's indeterminate whether a {@link ReferencedObjectException} will be thrown,
     * as that depends on whether {@code B} or {@code C} is deleted first (with the answer being, respectively, no and yes).
     *
     * @param id object ID of the object to delete
     * @return true if object was found and deleted, false if object was not found,
     *  or if {@code id} specifies an unknown object type
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

        // Does object exist?
        if (!this.exists(id))
            return false;

        // Handle delete cascade and recurive DeleteAction.DELETE without hogging Java stack
        final ObjIdSet deletables = new ObjIdSet();
        deletables.add(id);
        boolean found = false;
        while (!deletables.isEmpty())
            found |= this.doDelete(deletables.iterator().next(), deletables);

        // Done
        return found;
    }

    private synchronized boolean doDelete(final ObjId id, ObjIdSet deletables) {

        // Loop here to handle any mutations within delete notification listener callbacks
        ObjInfo info;
        while (true) {

            // Get object info
            try {
                info = this.getObjectInfo(id, false);
            } catch (DeletedObjectException e) {                    // possibly due to a cycle of DeleteAction.DELETE references
                deletables.remove(id);
                return false;
            } catch (UnknownTypeException e) {
                throw new InconsistentDatabaseException("encountered reference with unknown type during delete cascade: " + id, e);
            }

            // Determine if any EXCEPTION reference fields refer to the object (from some other object); if so, throw exception
            for (Map.Entry<Integer, NavigableSet<ObjId>> entry : this.findReferrers(id, DeleteAction.EXCEPTION).entrySet()) {
                for (ObjId referrer : entry.getValue()) {
                    if (!referrer.equals(id))
                        throw new ReferencedObjectException(this, id, referrer, entry.getKey());
                }
            }

            // Do we need to issue delete notifications for the object being deleted?
            if (info.isDeleteNotified() || this.deleteListeners == null || this.deleteListeners.isEmpty())
                break;

            // Set "delete notified" flag and update object info cache
            ObjInfo.write(this, id, info.getVersion(), true);
            this.objInfoCache.put(id, new ObjInfo(this, id, info.getVersion(), true, info.schema, info.objType));

            // Issue delete notifications and retry
            if (!this.disableListenerNotifications) {
                for (DeleteListener listener : this.deleteListeners.toArray(new DeleteListener[this.deleteListeners.size()]))
                    listener.onDelete(this, id);
            }
        }

        // Find all objects referred to by a reference field with cascadeDelete = true and add them to deletables
        for (ReferenceField field : info.getObjType().referenceFieldsAndSubFields.values()) {
            if (!field.cascadeDelete)
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
        deletables.remove(id);

        // Find all UNREFERENCE references and unreference them
        for (Map.Entry<Integer, NavigableSet<ObjId>> entry : this.findReferrers(id, DeleteAction.UNREFERENCE).entrySet()) {
            final int storageId = entry.getKey();
            final NavigableSet<ObjId> referrers = entry.getValue();
            final SimpleFieldStorageInfo<?> fieldInfo = this.schemas.verifyStorageInfo(storageId, SimpleFieldStorageInfo.class);
            fieldInfo.unreferenceAll(this, id, referrers);
        }

        // Find all DELETE references and mark the containing object for deletion (caller will call us back to actually delete)
        this.findReferrers(id, DeleteAction.DELETE).values().forEach(deletables::addAll);

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
        final byte[] minKey = info.getId().getBytes();
        final byte[] maxKey = ByteUtil.getKeyAfterPrefix(minKey);
        this.kvt.removeRange(minKey, maxKey);

        // Delete object schema version entry
        this.kvt.remove(Layout.buildVersionIndexKey(id, info.getVersion()));

        // Update ObjInfo cache
        this.objInfoCache.remove(id);
    }

    /**
     * Determine if an object exists.
     *
     * <p>
     * This method does <i>not</i> change the object's schema version if it exists
     * and has a different version that this transaction.
     *
     * @param id object ID of the object to find
     * @return true if object was found, false if object was not found, or if {@code id} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized boolean exists(ObjId id) {
        return this.getObjectInfoIfExists(id, false) != null;
    }

    /**
     * Copy an object into a (possibly different) transaction.
     *
     * <p>
     * This copies the object, including all of its field data, to {@code dest}. If the object already exists in {@code dest},
     * the existing copy is completely replaced.
     *
     * <p>
     * Only the object itself is copied; any other objects it references are not copied. If the target object does not exist
     * in {@code dest}, it will be created first (and {@link CreateListener}s notified); otherwise, first the target object's
     * schema version will be upgraded if necessary to match {@code source} (and {@link VersionChangeListener}s notified).
     * Finally, as fields are copied, non-trivial changes to the target object's fields generate change listener notifications.
     *
     * <p>
     * If {@code updateVersion} is true, the {@code source} object is first upgraded to
     * {@linkplain #getSchema() the schema version associated with this transaction}.
     * In any case, the schema version associated with {@code source}, when copied, must be identical
     * in this transaction and {@code dest}.
     *
     * <p><b>Disabling Notifications</b></p>
     *
     * <p>
     * The {@code notifyListeners} flag controls whether notifications are delivered to {@link CreateListener}s
     * and field change listeners as objects are created and modified in {@code dest}. {@link VersionChangeListener}s
     * are always notified.
     *
     * <p><b>Deleted Assignments Handling</b></p>
     *
     * <p>
     * If a reference field configured to {@linkplain ReferenceField#isAllowDeleted disallow deleted assignments} is copied,
     * but the referenced object does not exist in {@code dest}, then a {@link DeletedObjectException} is thrown and no copy
     * is performed. However, this can present an impossible chicken-and-egg situation when multiple objects need to be copied
     * and there are cycles in the graph of references between objects.
     *
     * <p>
     * If {@code deletedAssignments} is non-null, then instead of triggering an exception, illegal references to deleted objects
     * are collected in {@code deletedAssignments}, which maps each deleted object to (some) referring field in the copied
     * object. This lets the caller to decide what to do about them.
     *
     * <p><b>Object ID Remapping</b></p>
     *
     * <p>
     * The optional {@code objectIdMap} parameter specifies how object ID's should be remapped as fields are copied into
     * {@code dest}. This remapping applies to all {@linkplain ReferenceField reference fields}, and also applies to
     * {@code source} itself, i.e., the target object ID may be different from {@code source}.
     *
     * <p><b>Return Value</b></p>
     *
     * <p>
     * If {@code dest} is this instance, and the {@code source} is not remapped (see below), no fields are changed and false is
     * returned; however, a schema update may still occur (if {@code updateVersion} is true), and deleted assignment checks are
     * applied.
     *
     * <p><b>Deadlocks</b></p>
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     *
     * @param source object ID of the source object in this transaction
     * @param dest destination for the copy of {@code source} (possibly this transaction)
     * @param updateVersion true to automatically update {@code source}'s schema version prior to the copy, false to not change it
     * @param notifyListeners whether to notify {@link CreateListener}s and field change listeners
     * @param deletedAssignments if not null, collect assignments to deleted objects here instead of throwing
     *  {@link DeletedObjectException}s, where the map key is the deleted object and the map value is some referring field
     * @param objectIdMap if not null, a remapping of object ID's in this transaction to object ID's in {@code dest}
     * @return false if the target object already existed in {@code dest}, true if it was newly created
     * @throws DeletedObjectException if no object with ID equal to {@code source} exists in this transaction
     * @throws DeletedObjectException if {@code deletedAssignments} is null, and a non-null reference field in {@code source}
     *  that disallows deleted assignments contains a reference to an object that does not exist in {@code dest}
     * @throws UnknownTypeException if {@code source} or an ID in {@code objectIdMap} specifies an unknown object type
     * @throws IllegalArgumentException if {@code objectIdMap} maps an object ID to null
     * @throws IllegalArgumentException if {@code objectIdMap} maps {@code source} to a different object type
     * @throws IllegalArgumentException if {@code objectIdMap} maps the value of a reference field to an incompatible object type
     * @throws IllegalArgumentException if any parameter is null
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws SchemaMismatchException if the schema version associated with {@code source} differs between
     *  this transaction and {@code dest}
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     */
    public synchronized boolean copy(ObjId source, final Transaction dest, final boolean updateVersion,
      final boolean notifyListeners, final ObjIdMap<ReferenceField> deletedAssignments, final ObjIdMap<ObjId> objectIdMap) {

        // Sanity check
        Preconditions.checkArgument(source != null, "null source");
        Preconditions.checkArgument(dest != null, "null dest");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Get source object info
        final ObjInfo srcInfo = this.getObjectInfo(source, updateVersion);

        // Do the copy while both transactions are locked
        synchronized (dest) {

            // Sanity check
            if (dest.stale)
                throw new StaleTransactionException(dest);

            // Copy fields
            return dest.mutateAndNotify(() -> {
                final ObjIdMap<ObjId> previousCopyIdMap = dest.copyIdMap;
                dest.copyIdMap = objectIdMap;
                final ObjIdMap<ReferenceField> previousCopyDeletedAssignments = dest.deletedAssignments;
                dest.deletedAssignments = deletedAssignments;
                final boolean previousDisableListenerNotifications = dest.disableListenerNotifications;
                dest.disableListenerNotifications = !notifyListeners;
                try {
                    return Transaction.doCopyFields(srcInfo, Transaction.this, dest, updateVersion);
                } finally {
                    dest.copyIdMap = previousCopyIdMap;
                    dest.deletedAssignments = previousCopyDeletedAssignments;
                    dest.disableListenerNotifications = previousDisableListenerNotifications;
                }
            });
        }
    }

    // This method assumes both transactions are locked
    private static boolean doCopyFields(ObjInfo srcInfo, Transaction srcTx, Transaction dstTx, boolean updateVersion) {

        // Sanity check
        assert Thread.holdsLock(srcTx);
        assert Thread.holdsLock(dstTx);

        // Get destination object ID and verify it is sensible
        final ObjId srcId = srcInfo.getId();
        final ObjId dstId = dstTx.copyIdMap != null && dstTx.copyIdMap.containsKey(srcId) ? dstTx.copyIdMap.get(srcId) : srcId;
        if (dstId == null)
            throw new IllegalArgumentException("can't copy " + srcId + " because " + srcId + " is remapped to null");
        final int typeStorageId = srcId.getStorageId();
        if (dstId.getStorageId() != typeStorageId) {
            throw new IllegalArgumentException("can't copy " + srcId + " to " + dstId
              + " due to non-equal storage ID's (" + typeStorageId + " != " + dstId.getStorageId() + ")");
        }

        // Upgrade source object if necessary
        if (updateVersion && srcInfo.getVersion() != srcTx.schema.versionNumber) {
            srcTx.changeVersion(srcInfo, srcTx.schema);
            srcInfo = srcTx.loadIntoCache(srcId);
        }
        final Schema srcSchema = srcInfo.getSchema();

        // Find and verify the expected schema version in the destination transaction
        final int objectVersion = srcSchema.versionNumber;
        final Schema dstSchema;
        try {
            dstSchema = dstTx.schemas.getVersion(objectVersion);
        } catch (IllegalArgumentException e) {
            throw new SchemaMismatchException("destination transaction has no schema version " + objectVersion);
        }
        if (!Arrays.equals(srcSchema.encodedXML, dstSchema.encodedXML)
          && !srcSchema.schemaModel.isCompatibleWith(dstSchema.schemaModel)) {
            throw new SchemaMismatchException("destination transaction schema version "
              + objectVersion + " does not match source schema version " + objectVersion + "\n"
              + dstSchema.schemaModel.differencesFrom(srcSchema.schemaModel));
        }

        // Determine if destination object already exists, and if so get info about it
        ObjInfo dstInfo = dstTx.getObjectInfoIfExists(dstId, false);
        final boolean existed = dstInfo != null;

        // If destination object already exists and has upgrade listeners, go through the normal upgrade process first
        if (existed && dstInfo.getVersion() != objectVersion
          && dstTx.versionChangeListeners != null && !dstTx.versionChangeListeners.isEmpty()) {
            dstTx.changeVersion(dstInfo, dstSchema);
            dstInfo = dstTx.loadIntoCache(dstId);
        }

        // Do field-by-field copy if there are change listeners or remapping, otherwise do fast copy of key/value pairs directly
        final ObjType srcType = srcSchema.getObjType(typeStorageId);
        final ObjType dstType = dstSchema.getObjType(typeStorageId);
        if (dstTx.copyIdMap != null || (!dstTx.disableListenerNotifications && dstTx.hasFieldMonitor(dstType))) {

            // Create destination object if it does not exist
            if (!existed)
                dstTx.createObjectData(dstId, objectVersion, dstSchema, dstType);

            // Copy fields
            for (Field<?> field : srcType.fields.values())
                field.copy(srcId, dstId, srcTx, dstTx, dstTx.copyIdMap);
        } else {
            assert srcType.schema.versionNumber == dstType.schema.versionNumber;

            // Check for any deleted reference assignments
            for (ReferenceField field : dstType.referenceFieldsAndSubFields.values())
                field.findAnyDeletedAssignments(srcTx, dstTx, dstId);

            // We can short circuit here if source and target are the same object in the same transaction
            if (srcId.equals(dstId) && srcTx.equals(dstTx))
                return !existed;

            // Nuke previous destination object, if any
            if (dstInfo != null)
                dstTx.deleteObjectData(dstInfo);

            // Add schema version index entry
            dstTx.kvt.put(Layout.buildVersionIndexKey(dstId, objectVersion), ByteUtil.EMPTY);

            // Copy object meta-data and all field content in one key range sweep
            final KeyRange srcKeyRange = KeyRange.forPrefix(srcId.getBytes());
            final ByteWriter dstWriter = new ByteWriter();
            dstWriter.write(dstId.getBytes());
            final int dstMark = dstWriter.mark();
            try (CloseableIterator<KVPair> i = srcTx.kvt.getRange(srcKeyRange)) {
                while (i.hasNext()) {
                    final KVPair kv = i.next();
                    assert srcKeyRange.contains(kv.getKey());
                    final ByteReader srcReader = new ByteReader(kv.getKey());
                    srcReader.skip(ObjId.NUM_BYTES);
                    dstWriter.reset(dstMark);
                    dstWriter.write(srcReader);
                    dstTx.kvt.put(dstWriter.getBytes(), kv.getValue());
                }
            }

            // Create object's simple field index entries
            dstType.indexedSimpleFields
              .forEach(field -> {
                final byte[] fieldValue = dstTx.kvt.get(field.buildKey(dstId));     // can be null (if field has default value)
                final byte[] indexKey = Transaction.buildSimpleIndexEntry(field, dstId, fieldValue);
                dstTx.kvt.put(indexKey, ByteUtil.EMPTY);
            });

            // Create object's composite index entries
            for (CompositeIndex index : dstType.compositeIndexes.values())
                dstTx.kvt.put(Transaction.buildCompositeIndexEntry(dstTx, dstId, index), ByteUtil.EMPTY);

            // Create object's complex field index entries
            for (ComplexField<?> field : dstType.complexFields.values()) {
                field.getSubFields().stream()
                  .filter(subField -> subField.indexed)
                  .forEach(subField -> field.addIndexEntries(dstTx, dstId, subField));
            }
        }

        // Done
        return !existed;
    }

    /**
     * Add a {@link CreateListener} to this transaction.
     *
     * @param listener the listener to add
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addCreateListener(CreateListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");
        if (this.createListeners == null)
            this.createListeners = new HashSet<>(1);
        this.createListeners.add(listener);
    }

    /**
     * Remove an {@link CreateListener} from this transaction.
     *
     * @param listener the listener to remove
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeCreateListener(CreateListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");
        if (this.createListeners == null)
            return;
        this.createListeners.remove(listener);
    }

    /**
     * Add a {@link DeleteListener} to this transaction.
     *
     * @param listener the listener to add
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addDeleteListener(DeleteListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");
        if (this.deleteListeners == null)
            this.deleteListeners = new HashSet<>(1);
        this.deleteListeners.add(listener);
    }

    /**
     * Remove an {@link DeleteListener} from this transaction.
     *
     * @param listener the listener to remove
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeDeleteListener(DeleteListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");
        if (this.deleteListeners == null)
            return;
        this.deleteListeners.remove(listener);
    }

// Object Versioning

    /**
     * Read the current schema version of the given object. Does not change the object's version.
     *
     * @param id object id
     * @return object's current schema version
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized int getSchemaVersion(ObjId id) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(id != null, "null id");

        // Get object version
        return this.getObjectInfo(id, false).getVersion();
    }

    /**
     * Change the schema version of the specified object, if necessary, so that its version matches
     * {@linkplain #getSchema() the schema version associated with this transaction}.
     *
     * <p>
     * If a schema change occurs, any registered {@link VersionChangeListener}s will be notified prior
     * to this method returning.
     *
     * @param id object ID of the object to delete
     * @return true if the object schema version was changed, otherwise false
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws IllegalArgumentException if {@code id} is null
     * @throws TypeNotInSchemaVersionException if the object version could not be updated because the object's type
     *   does not exist in the schema version associated with this transaction
     * @see #getSchemaVersion(ObjId)
     */
    public synchronized boolean updateSchemaVersion(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, false);
        if (info.getVersion() == this.schema.versionNumber)
            return false;

        // Update schema version
        this.mutateAndNotify(new Mutation<Void>() {
            @Override
            public Void mutate() {
                Transaction.this.changeVersion(info, Transaction.this.schema);
                return null;
            }
        });

        // Done
        return true;
    }

    /**
     * Migrate object's schema to the version specified and notify listeners. This assumes we are locked.
     *
     * @param info original object info
     * @param targetVersion version to change to
     */
    private void changeVersion(final ObjInfo info, final Schema targetVersion) {

        // Get version numbers
        final ObjId id = info.getId();
        final int oldVersion = info.getVersion();
        final int newVersion = targetVersion.versionNumber;

        // Sanity check
        assert Thread.holdsLock(this);
        assert this.schemas.getVersion(targetVersion.versionNumber) == targetVersion;
        Preconditions.checkArgument(newVersion != oldVersion, "object already at version");

        // Get old and new types
        final ObjType oldType = info.getObjType();
        final ObjType newType;
        try {
            newType = targetVersion.getObjType(id.getStorageId());
        } catch (UnknownTypeException e) {
            throw (TypeNotInSchemaVersionException)new TypeNotInSchemaVersionException(id, newVersion).initCause(e);
        }

        // Gather removed fields' values here for user migration if any VersionChangeListeners are registered
        final TreeMap<Integer, Object> oldValueMap
          = this.versionChangeListeners != null && !this.versionChangeListeners.isEmpty() ? new TreeMap<>() : null;

    //////// Remove the index entries corresponding to removed composite indexes

        // Remove index entries for composite indexes that are going away
        oldType.compositeIndexes.values().stream()
          .filter(index -> !newType.compositeIndexes.containsKey(index.storageId))
          .forEach(index -> this.kvt.remove(this.buildCompositeIndexEntry(id, index)));

    //////// Determine Field Compatibility

        // Build a mapping from old field to compatible new field, or null if none
        final HashMap<Field<?>, Field<?>> compatibleFieldMap = new HashMap<>(oldType.fields.size());
        for (Map.Entry<Integer, Field<?>> entry : oldType.fields.entrySet()) {
            final Integer storageId = entry.getKey();
            final Field<?> oldField = entry.getValue();
            final Field<?> newField = newType.fields.get(storageId);
            final boolean compatible = newField != null && newField.isUpgradeCompatible(oldField);
            compatibleFieldMap.put(oldField, compatible ? newField : null);
        }

        // Build a list of remaining new fields not compatible with some old field
        final ArrayList<Field<?>> incompatibleNewFields = new ArrayList<>(newType.fields.size());
        for (Map.Entry<Integer, Field<?>> entry : newType.fields.entrySet()) {
            final Integer storageId = entry.getKey();
            final Field<?> newField = entry.getValue();
            final Field<?> oldField = oldType.fields.get(storageId);
            if (oldField == null || compatibleFieldMap.get(oldField) == null)
                incompatibleNewFields.add(newField);
            else
                assert compatibleFieldMap.get(oldField) == newField;
        }

    //////// Process old fields

        // Iterate over all the fields that existed in the old schema version
        for (Map.Entry<Field<?>, Field<?>> entry : compatibleFieldMap.entrySet()) {
            final Field<?> oldField = entry.getKey();

            // Copy the old field's original value for the version change notification, if any
            if (oldValueMap != null) {
                oldField.visit(new FieldSwitch<Void>() {

                    @Override
                    @SuppressWarnings("shadow")
                    public <T> Void caseSimpleField(SimpleField<T> oldField) {
                        final byte[] key = Field.buildKey(id, oldField.storageId);
                        final byte[] oldValue = Transaction.this.kvt.get(key);
                        oldValueMap.put(oldField.storageId, oldValue != null ?
                          oldField.fieldType.read(new ByteReader(oldValue)) : oldField.fieldType.getDefaultValueObject());
                        return null;
                    }

                    @Override
                    @SuppressWarnings("shadow")
                    public <T> Void caseComplexField(ComplexField<T> oldField) {
                        oldValueMap.put(oldField.storageId, oldField.getValueReadOnlyCopy(Transaction.this, id));
                        return null;
                    }

                    @Override
                    @SuppressWarnings("shadow")
                    public Void caseCounterField(CounterField oldField) {
                        final byte[] key = Field.buildKey(id, oldField.storageId);
                        final byte[] oldValue = Transaction.this.kvt.get(key);
                        oldValueMap.put(oldField.storageId, oldValue != null ? Transaction.this.kvt.decodeCounter(oldValue) : 0L);
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
                        final SortedSet<Integer> xtypes = Transaction.this.findRemovedTypes(oldField, newField);
                        if (!xtypes.isEmpty()) {
                            final ObjId ref = oldField.getValue(Transaction.this, id);
                            if (ref != null && xtypes.contains(ref.getStorageId())) {

                                // Change new field to be incompatible, so it will get reset
                                entry.setValue(null);
                                incompatibleNewFields.add(newField);
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
                    final byte[] key = Field.buildKey(id, oldField.storageId);
                    if (oldField.indexed && (reset || !newField.indexed)) {
                        final byte[] value = Transaction.this.kvt.get(key);
                        Transaction.this.kvt.remove(Transaction.buildSimpleIndexEntry(oldField, id, value));
                    }
                    if (newField != null && newField.indexed && (reset || !oldField.indexed)) {
                        final byte[] value = !reset ? Transaction.this.kvt.get(key) : null;
                        Transaction.this.kvt.put(Transaction.buildSimpleIndexEntry(newField, id, value), ByteUtil.EMPTY);
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
                            final SortedSet<Integer> xtypes = Transaction.this.findRemovedTypes(oldRefField, newRefField);
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

                    // Reset field?
                    final boolean reset = entry.getValue() == null;

                    // Reset field value if needed
                    if (reset)
                        Transaction.this.kvt.remove(Field.buildKey(id, oldField.storageId));
                    return null;
                }
            });
        }

    //////// For fields that are new or were reset, initialize values and add index entries

        // Iterate over the new fields that are truly new or got reset
        for (Field<?> newField : incompatibleNewFields) {
            newField.visit(new FieldSwitch<Void>() {

                @Override
                @SuppressWarnings("shadow")
                public <T> Void caseSimpleField(SimpleField<T> newField) {
                    if (newField.indexed)
                        Transaction.this.kvt.put(Transaction.buildSimpleIndexEntry(newField, id, null), ByteUtil.EMPTY);
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
                    final byte[] key = Field.buildKey(id, newField.storageId);
                    Transaction.this.kvt.put(key, Transaction.this.kvt.encodeCounter(0L));
                    return null;
                }
            });
        }

    //////// Add composite index entries for newly added composite indexes

        // Add index entries for composite indexes that are newly added
        newType.compositeIndexes.values().stream()
          .filter(index -> !oldType.compositeIndexes.containsKey(index.storageId))
          .forEach(index -> this.kvt.put(this.buildCompositeIndexEntry(id, index), ByteUtil.EMPTY));

    //////// Update object version and corresponding index entry

        // Change object version and update object info cache
        ObjInfo.write(this, id, newVersion, info.isDeleteNotified());
        this.objInfoCache.put(id, new ObjInfo(this, id, newVersion, info.isDeleteNotified(), targetVersion, newType));

        // Update object version index entry
        this.kvt.remove(Layout.buildVersionIndexKey(id, oldVersion));
        this.kvt.put(Layout.buildVersionIndexKey(id, newVersion), ByteUtil.EMPTY);

    //////// Notify listeners

        // Lock down old field values map
        final NavigableMap<Integer, Object> readOnlyOldValuesMap = oldValueMap != null ?
          Collections.unmodifiableNavigableMap(oldValueMap) : null;

        // Notify about version update
        if (this.versionChangeListeners != null) {
            for (VersionChangeListener listener : this.versionChangeListeners)
                listener.onVersionChange(this, id, oldVersion, newVersion, readOnlyOldValuesMap);
        }
    }

    /**
     * Find storage ID's which are no longer allowed by a reference field when upgrading to the specified
     * schema version and therefore need to be scrubbed during the upgrade.
     *
     * @return set of storage ID's that are no longer allowed and should be audited on upgrade
     */
    private SortedSet<Integer> findRemovedTypes(ReferenceField oldField, ReferenceField newField) {

        // Check allowed storage IDs
        final SortedSet<Integer> newObjectTypes = newField.getObjectTypes();
        if (newObjectTypes == null)
            return Collections.emptySortedSet();                        // new field can refer to any type in any schema version
        SortedSet<Integer> oldObjectTypes = oldField.getObjectTypes();
        if (oldObjectTypes == null)
            oldObjectTypes = this.schemas.objTypeStorageIds;            // old field can refer to any type in any schema version

        // Identify storage IDs which are were allowed by old field but are no longer allowed by new field
        final TreeSet<Integer> removedObjectTypes = new TreeSet<>(oldObjectTypes);
        removedObjectTypes.removeAll(newObjectTypes);
        return removedObjectTypes;
    }

    /**
     * Query objects by schema version.
     *
     * @return read-only, real-time view of all database objects indexed by schema version
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex<Integer, ObjId> queryVersion() {
        if (this.stale)
            throw new StaleTransactionException(this);
        return Layout.getVersionIndex(this.kvt);
    }

    /**
     * Add an {@link VersionChangeListener} to this transaction.
     *
     * @param listener the listener to add
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addVersionChangeListener(VersionChangeListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");
        if (this.versionChangeListeners == null)
            this.versionChangeListeners = new HashSet<>(1);
        this.versionChangeListeners.add(listener);
    }

    /**
     * Remove an {@link VersionChangeListener} from this transaction.
     *
     * @param listener the listener to remove
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeVersionChangeListener(VersionChangeListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");
        if (this.versionChangeListeners == null)
            return;
        this.versionChangeListeners.remove(listener);
    }

// Object and Field Access

    /**
     * Get all objects in the database.
     *
     * <p>
     * The returned set includes objects from all schema versions. Use {@link #queryVersion} to
     * find objects with a specific schema version.
     *
     * <p>
     * The returned set is mutable, with the exception that {@link NavigableSet#add add()} is not supported.
     * Deleting an element results in {@linkplain #delete deleting} the corresponding object.
     *
     * @return a live view of all database objects
     * @throws StaleTransactionException if this transaction is no longer usable
     * @see #getAll(int)
     */
    public synchronized NavigableSet<ObjId> getAll() {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);

        // Return objects
        return new ObjTypeSet(this);
    }

    /**
     * Get all objects whose object type has the specified storage ID.
     *
     * <p>
     * The returned set is mutable, with the exception that {@link NavigableSet#add add()} is not supported.
     * Deleting an element results in {@linkplain #delete deleting} the corresponding object.
     *
     * @param storageId object type storage ID
     * @return a live view of all database objects having the specified storage ID
     * @throws UnknownTypeException if {@code storageId} does not correspond to any known object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @see #getAll()
     */
    public synchronized NavigableSet<ObjId> getAll(int storageId) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        this.schemas.verifyStorageInfo(storageId, ObjTypeStorageInfo.class);

        // Return objects
        return new ObjTypeSet(this, storageId);
    }

    /**
     * Read the value of a {@link SimpleField} from an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary, prior to
     * reading the field.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link SimpleField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code storageId} exists in the object
     * @throws IllegalArgumentException if {@code id} is null
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     */
    public synchronized Object readSimpleField(ObjId id, int storageId, boolean updateVersion) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        this.checkStaleFieldAccess(id, storageId);

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, updateVersion);

        // Find field
        final SimpleField<?> field = info.getObjType().simpleFields.get(storageId);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), storageId, "simple field");

        // Read field
        final byte[] key = field.buildKey(id);
        final byte[] value = this.kvt.get(key);

        // Decode value
        return value != null ? field.fieldType.read(new ByteReader(value)) : field.fieldType.getDefaultValueObject();
    }

    /**
     * Change the value of a {@link SimpleField} in an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary, prior to
     * writing the field.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link SimpleField}
     * @param value new value for the field
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code storageId} exists in the object
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for the field
     * @throws IllegalArgumentException if {@code id} is null
     */
    public void writeSimpleField(final ObjId id, final int storageId, final Object value, final boolean updateVersion) {
        this.mutateAndNotify(id, () -> {
            this.doWriteSimpleField(id, storageId, value, updateVersion);
            return null;
        });
    }

    private synchronized void doWriteSimpleField(ObjId id, int storageId, final Object newObj, boolean updateVersion) {

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, updateVersion);

        // Find field
        final SimpleField<?> field = info.getObjType().simpleFields.get(storageId);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), storageId, "simple field");

        // Check for deleted assignment
        if (field instanceof ReferenceField)
            this.checkDeletedAssignment(id, (ReferenceField)field, (ObjId)newObj);

        // Get new value
        final byte[] key = field.buildKey(id);
        final byte[] newValue = field.encode(newObj);

        // Before setting the new value, read the old value if one of the following is true:
        //  - The field is being monitored -> we need to filter out "changes" that don't actually change anything
        //  - The field is indexed -> we need the old value so we can remove the old index entry
        // If neither of the above is true, then there's no need to read the old value.
        byte[] oldValue = null;
        if (field.indexed || field.compositeIndexMap != null
          || (!this.disableListenerNotifications && this.hasFieldMonitor(id, field.storageId))) {

            // Get old value
            oldValue = this.kvt.get(key);

            // Compare new to old value
            if (oldValue != null ? newValue != null && Arrays.equals(oldValue, newValue) : newValue == null)
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
            this.kvt.put(Transaction.buildSimpleIndexEntry(field, id, newValue), ByteUtil.EMPTY);
        }

        // Update affected composite indexes, if any
        if (field.compositeIndexMap != null) {
            for (CompositeIndex index : field.compositeIndexMap.keySet()) {

                // Build old composite index entry
                final ByteWriter oldWriter = new ByteWriter();
                UnsignedIntEncoder.write(oldWriter, index.storageId);
                int fieldStart = -1;
                int fieldEnd = -1;
                for (SimpleField<?> otherField : index.fields) {
                    final byte[] otherValue;
                    if (otherField == field) {
                        fieldStart = oldWriter.getLength();
                        otherValue = oldValue;
                    } else
                        otherValue = this.kvt.get(otherField.buildKey(id));         // can be null (if field has default value)
                    oldWriter.write(otherValue != null ? otherValue : otherField.fieldType.getDefaultValue());
                    if (otherField == field)
                        fieldEnd = oldWriter.getLength();
                }
                assert fieldStart != -1;
                assert fieldEnd != -1;
                id.writeTo(oldWriter);

                // Remove old composite index entry
                final byte[] oldIndexEntry = oldWriter.getBytes();
                this.kvt.remove(oldIndexEntry);

                // Patch in new field value to create new composite index entry
                final ByteWriter newWriter = new ByteWriter(oldIndexEntry.length);
                newWriter.write(oldIndexEntry, 0, fieldStart);
                newWriter.write(newValue != null ? newValue : field.fieldType.getDefaultValue());
                newWriter.write(oldIndexEntry, fieldEnd, oldIndexEntry.length - fieldEnd);

                // Add new composite index entry
                this.kvt.put(newWriter.getBytes(), ByteUtil.EMPTY);
            }
        }

        // Notify monitors
        if (!this.disableListenerNotifications) {
            final Object oldObj = oldValue != null ?
              field.fieldType.read(new ByteReader(oldValue)) : field.fieldType.getDefaultValueObject();
            this.addFieldChangeNotification(new SimpleFieldChangeNotifier(field, id) {
                @Override
                @SuppressWarnings("unchecked")
                public void notify(Transaction tx, SimpleFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                    listener.onSimpleFieldChange(tx, this.id, (SimpleField<Object>)field, path, referrers, oldObj, newObj);
                }
            });
        }
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

        // Allow null
        if (targetId == null)
            return;

        // It's possible for target to be the same object during a copy of a self-referencing object; allow it
        if (targetId.equals(id))
            return;

        // Is deleted assignment disallowed for this field?
        if ((this instanceof SnapshotTransaction) ? field.allowDeletedSnapshot : field.allowDeleted)
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
        throw new DeletedObjectException(targetId, "illegal assignment to " + field + " in " + this.getObjDescription(id)
          + " of reference to deleted object " + this.getObjDescription(targetId));
    }

    /**
     * Build a simple index entry for the given field, object ID, and field value.
     *
     * @param field simple field
     * @param id ID of object containing the field
     * @param value encoded field value, or null for default value
     * @return index key
     */
    private static byte[] buildSimpleIndexEntry(SimpleField<?> field, ObjId id, byte[] value) {
        if (value == null)
            value = field.fieldType.getDefaultValue();
        final ByteWriter writer = new ByteWriter(UnsignedIntEncoder.encodeLength(field.storageId) + value.length + ObjId.NUM_BYTES);
        UnsignedIntEncoder.write(writer, field.storageId);
        writer.write(value);
        id.writeTo(writer);
        return writer.getBytes();
    }

    /**
     * Get a description of the given object's type.
     *
     * @param id object ID
     * @return textual description of the specified object's type
     * @throws IllegalArgumentException if {@code id} is null
     */
    public String getTypeDescription(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        final int storageId = id.getStorageId();
        final ObjType type = this.schema.objTypeMap.get(id.getStorageId());
        return type != null ? "type `" + type.getName() + "'" : "type #" + storageId;
    }

    String getObjDescription(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return "object " + id + " (" + this.getTypeDescription(id) + ")";
    }

    String getFieldDescription(ObjId id, int storageId) {
        Preconditions.checkArgument(id != null, "null id");
        final ObjType type = this.schema.objTypeMap.get(id.getStorageId());
        if (type == null)
            return "field #" + storageId;
        final Field<?> field;
        try {
            field = type.getField(storageId, true);
        } catch (UnknownFieldException e) {
            return "field #" + storageId;
        }
        return "field `" + field.getName() + "'";
    }

    private void checkStaleFieldAccess(ObjId id, int storageId) {
        assert Thread.holdsLock(this);
        if (this.stale) {
            throw new StaleTransactionException(this, "can't access " + this.getFieldDescription(id, storageId)
              + " of " + this.getObjDescription(id) + ": " + StaleTransactionException.DEFAULT_MESSAGE);
        }
    }

    /**
     * Read the value of a {@link CounterField} from an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary, prior to
     * reading the field.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link CounterField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return value of the counter in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link CounterField} corresponding to {@code storageId} exists in the object
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized long readCounterField(ObjId id, int storageId, boolean updateVersion) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        this.checkStaleFieldAccess(id, storageId);

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, updateVersion);

        // Find field
        final CounterField field = info.getObjType().counterFields.get(storageId);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), storageId, "counter field");

        // Read field
        final byte[] key = field.buildKey(id);
        final byte[] value = this.kvt.get(key);

        // Decode value
        return value != null ? this.kvt.decodeCounter(value) : 0;
    }

    /**
     * Set the value of a {@link CounterField} in an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary, prior to
     * writing the field.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link CounterField}
     * @param value new counter value
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link CounterField} corresponding to {@code storageId} exists in the object
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized void writeCounterField(final ObjId id, final int storageId, final long value, final boolean updateVersion) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        this.checkStaleFieldAccess(id, storageId);

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, updateVersion);

        // Find field
        final CounterField field = info.getObjType().counterFields.get(storageId);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), storageId, "counter field");

        // Set value
        final byte[] key = field.buildKey(id);
        this.kvt.put(key, this.kvt.encodeCounter(value));
    }

    /**
     * Adjust the value of a {@link CounterField} in an object by some amount, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary, prior to
     * writing the field.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link CounterField}
     * @param offset offset value to add to counter value
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link CounterField} corresponding to {@code storageId} exists in the object
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized void adjustCounterField(ObjId id, int storageId, long offset, boolean updateVersion) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        this.checkStaleFieldAccess(id, storageId);

        // Optimize away non-change
        if (offset == 0)
            return;

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, updateVersion);

        // Find field
        final CounterField field = info.getObjType().counterFields.get(storageId);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), storageId, "counter field");

        // Adjust counter value
        this.kvt.adjustCounter(field.buildKey(id), offset);
    }

    /**
     * Access a {@link SetField} associated with an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link SetField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return set field value
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link SetField} corresponding to {@code storageId} exists in the object
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     * @throws IllegalArgumentException if {@code id} is null
     */
    public NavigableSet<?> readSetField(ObjId id, int storageId, boolean updateVersion) {
        return this.readComplexField(id, storageId, updateVersion, SetField.class, NavigableSet.class);
    }

    /**
     * Access a {@link ListField} associated with an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link ListField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return list field value
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link ListField} corresponding to {@code storageId} exists in the object
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     * @throws IllegalArgumentException if {@code id} is null
     */
    public List<?> readListField(ObjId id, int storageId, boolean updateVersion) {
        return this.readComplexField(id, storageId, updateVersion, ListField.class, List.class);
    }

    /**
     * Access a {@link MapField} associated with an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchema() the schema version associated with this transaction}, if necessary.
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link MapField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return map field value
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws UnknownFieldException if no {@link MapField} corresponding to {@code storageId} exists in the object
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     * @throws IllegalArgumentException if {@code id} is null
     */
    public NavigableMap<?, ?> readMapField(ObjId id, int storageId, boolean updateVersion) {
        return this.readComplexField(id, storageId, updateVersion, MapField.class, NavigableMap.class);
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>This method does not check whether {@code id} is valid or the object actually exists.</li>
     *  <li>Objects utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link io.permazen.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param id object ID
     * @return the {@link io.permazen.kv.KVDatabase} key corresponding to {@code id}
     * @throws IllegalArgumentException if {@code id} is null
     * @see io.permazen.JTransaction#getKey(io.permazen.JObject) JTransaction.getKey()
     */
    public byte[] getKey(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return id.getBytes();
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified field in the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>This method does not check whether {@code id} is valid, the object exists,
     *      or the field actually exists in the object's current schema version.</li>
     *  <li>Complex fields utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link io.permazen.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param id object ID
     * @param storageId field storage ID
     * @return the {@link io.permazen.kv.KVDatabase} key of the field in the specified object
     * @throws IllegalArgumentException if {@code storageId} is less than or equal to zero
     * @throws IllegalArgumentException if {@code id} is null
     * @see io.permazen.JTransaction#getKey(io.permazen.JObject, String) JTransaction.getKey()
     * @see io.permazen.kv.KVTransaction#watchKey KVTransaction.watchKey()
     */
    public byte[] getKey(ObjId id, int storageId) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(storageId > 0, "non-positive storageId");

        // Build key
        final ByteWriter writer = new ByteWriter();
        id.writeTo(writer);
        UnsignedIntEncoder.write(writer, storageId);
        return writer.getBytes();
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

    private synchronized <F, V> V readComplexField(ObjId id,
      int storageId, boolean updateVersion, Class<F> fieldClass, Class<V> valueType) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        this.checkStaleFieldAccess(id, storageId);

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, updateVersion);

        // Get field
        final ComplexField<?> field = info.getObjType().complexFields.get(storageId);
        if (!fieldClass.isInstance(field))
            throw new UnknownFieldException(info.getObjType(), storageId, fieldClass.getSimpleName());

        // Return view
        return valueType.cast(field.getValueInternal(this, id));
    }

    /**
     * If an object exists, read in its meta-data, also updating its schema version it in the process if requested.
     *
     * @param id object ID of the object
     * @param update true to update object's schema version to match this transaction, false to leave it alone
     * @return object info if object exists, otherwise null
     * @throws IllegalArgumentException if {@code id} is null
     */
    private ObjInfo getObjectInfoIfExists(ObjId id, boolean update) {
        assert Thread.holdsLock(this);
        try {
            return this.getObjectInfo(id, update);
        } catch (DeletedObjectException | UnknownTypeException e) {
            return null;
        }
    }

    /**
     * Read an object's meta-data, updating its schema version it in the process if requested.
     *
     * @param id object ID of the object
     * @param update true to update object's schema version to match this transaction, false to leave it alone
     * @return object info
     * @throws UnknownTypeException if object ID specifies an unknown object type
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws IllegalArgumentException if {@code id} is null
     */
    private ObjInfo getObjectInfo(ObjId id, boolean update) {

        // Sanity check
        assert Thread.holdsLock(this);

        // Load object info into cache, if not already there
        ObjInfo info = this.objInfoCache.get(id);
        if (info == null) {

            // Verify that the object type encoded within the object ID is valid
            this.schemas.verifyStorageInfo(id.getStorageId(), ObjTypeStorageInfo.class);

            // Load the object's info into the cache (if object doesn't exist, we'll get an exception here)
            info = this.loadIntoCache(id);
        }

        // Is a schema update required?
        if (!update || info.getVersion() == this.schema.versionNumber)
            return info;

        // Update schema version
        final ObjInfo info2 = info;
        this.mutateAndNotify(new Mutation<Void>() {
            @Override
            public Void mutate() {
                Transaction.this.changeVersion(info2, Transaction.this.schema);
                return null;
            }
        });

        // Load (updated) object info into cache
        return this.loadIntoCache(id);
    }

    /**
     * Get the specified object's info from the object info cache, loading it if necessary.
     *
     * @throws DeletedObjectException if object does not exist
     */
    private ObjInfo loadIntoCache(ObjId id) {
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
     * Permazen allows a field's type to change across schema versions, therefore some schema version may exist in
     * which the field associated with {@code storageId} is not a {@link SimpleField}. In such cases, {@code listener}
     * will receive notifications about those changes if it also happens to implement the other listener interface.
     * In other words, this method delegates directly to {@link #addFieldChangeListener addFieldChangeListener()}.
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public void addSimpleFieldChangeListener(int storageId, int[] path, KeyRanges[] filters,
      SimpleFieldChangeListener listener) {
        this.addFieldChangeListener(storageId, path, filters, listener);
    }

    /**
     * Monitor for changes within this transaction to the specified {@link SetField} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * <p>
     * Permazen allows a field's type to change across schema versions, therefore some schema version may exist in
     * which the field associated with {@code storageId} is not a {@link SetField}. In such cases, {@code listener}
     * will receive notifications about those changes if it also happens to implement the other listener interface.
     * In other words, this method delegates directly to {@link #addFieldChangeListener addFieldChangeListener()}.
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
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
     * Permazen allows a field's type to change across schema versions, therefore some schema version may exist in
     * which the field associated with {@code storageId} is not a {@link ListField}. In such cases, {@code listener}
     * will receive notifications about those changes if it also happens to implement the other listener interface.
     * In other words, this method delegates directly to {@link #addFieldChangeListener addFieldChangeListener()}.
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
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
     * Permazen allows a field's type to change across schema versions, therefore some schema version may exist in
     * which the field associated with {@code storageId} is not a {@link MapField}. In such cases, {@code listener}
     * will receive notifications about those changes if it also happens to implement the other listener interface.
     * In other words, this method delegates directly to {@link #addFieldChangeListener addFieldChangeListener()}.
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
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
     * Permazen allows a field's type to change across schema versions, therefore in different schema versions the
     * specified field may have different types. The {@code listener} will receive notifications about a field change
     * if it implements the interface appropriate for the field's current type (i.e., {@link SimpleFieldChangeListener},
     * {@link ListFieldChangeListener}, {@link SetFieldChangeListener}, or {@link MapFieldChangeListener}) at the time
     * of the change.
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, Object listener) {
        this.validateChangeListener(path, listener);
        this.getMonitorsForField(storageId, true).add(new FieldMonitor(storageId, path, filters, listener));
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
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
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeFieldChangeListener(int storageId, int[] path, KeyRanges[] filters, Object listener) {
        this.validateChangeListener(path, listener);
        final Set<FieldMonitor> monitors = this.getMonitorsForField(storageId);
        if (monitors != null)
            monitors.remove(new FieldMonitor(storageId, path, filters, listener));
    }

    private void validateChangeListener(int[] path, Object listener) {
        assert Thread.holdsLock(this);
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");
        this.verifyReferencePath(path);
    }

    private Set<FieldMonitor> getMonitorsForField(int storageId) {
        return this.getMonitorsForField(storageId, false);
    }

    private synchronized Set<FieldMonitor> getMonitorsForField(int storageId, boolean create) {
        Set<FieldMonitor> monitors;
        if (this.monitorMap == null) {
            if (!create)
                return null;
            this.monitorMap = new TreeMap<>();
            monitors = null;
        } else
            monitors = this.monitorMap.get(storageId);
        if (monitors == null) {
            if (!create)
                return null;
            monitors = new HashSet<>(1);
            this.monitorMap.put(storageId, monitors);
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
    void addFieldChangeNotification(FieldChangeNotifier<?> notifier) {
        assert Thread.holdsLock(this);
        assert !this.disableListenerNotifications;

        // Get info
        final ObjId id = notifier.getId();
        final int fieldStorageId = notifier.getStorageId();

        // Does anybody care?
        if (!this.hasFieldMonitor(id, fieldStorageId))
            return;

        // Add a pending field monitor notification for the specified field
        this.pendingNotifications.get().computeIfAbsent(fieldStorageId, i -> new ArrayList<>(2)).add(notifier);
    }

    /**
     * Determine if there are any monitors watching the specified field in the specified object.
     */
    boolean hasFieldMonitor(ObjId id, int fieldStorageId) {
        assert Thread.holdsLock(this);

        // Do quick check, if possible
        if (this.monitorMap == null)
            return false;
        final int objTypeStorageId = id.getStorageId();
        if (this.hasFieldMonitorCache != null)
            return this.hasFieldMonitorCache.contains(this.buildHasFieldMonitorCacheKey(objTypeStorageId, fieldStorageId));

        // Do slow check
        final Set<FieldMonitor> monitorsForField = this.getMonitorsForField(fieldStorageId);
        if (monitorsForField == null)
            return false;
        return monitorsForField.stream().anyMatch(new MonitoredPredicate(objTypeStorageId, fieldStorageId));
    }

    /**
     * Determine if there are any monitors watching any field in the specified type.
     */
    boolean hasFieldMonitor(ObjType objType) {
        assert Thread.holdsLock(this);

        // Do quick check, if possible
        if (this.monitorMap == null)
            return false;
        final int objTypeStorageId = objType.storageId;
        if (this.hasFieldMonitorCache != null) {
            final long minKey = this.buildHasFieldMonitorCacheKey(objTypeStorageId, 0);
            if (objTypeStorageId == Integer.MAX_VALUE)
                return this.hasFieldMonitorCache.ceiling(minKey) != null;
            final long maxKey = this.buildHasFieldMonitorCacheKey(objTypeStorageId + 1, 0);
            return !this.hasFieldMonitorCache.subSet(minKey, maxKey).isEmpty();
        }

        // Do slow check
        for (int fieldStorageId : NavigableSets.intersection(objType.fields.navigableKeySet(), this.monitorMap.navigableKeySet())) {
            if (this.monitorMap.get(fieldStorageId).stream().anyMatch(new MonitoredPredicate(objTypeStorageId, fieldStorageId)))
                return true;
        }
        return false;
    }

    private long buildHasFieldMonitorCacheKey(int objTypeStorageId, int fieldStorageId) {
        return ((long)objTypeStorageId << 32) | (long)fieldStorageId & 0xffffffffL;
    }

    /**
     * Build a data structure that to optimize checking whether a field in an object type is being monitored.
     */
    private synchronized NavigableSet<Long> buildHasFieldMonitorCache() {
        if (this.monitorMap == null)
            return Collections.emptyNavigableSet();
        final TreeSet<Long> set = new TreeSet<>();
        for (Schema otherSchema : this.schemas.versions.values()) {
            for (ObjType objType : otherSchema.objTypeMap.values()) {
                final int objTypeStorageId = objType.storageId;
                for (Field<?> field : objType.fieldsAndSubFields) {
                    final int fieldStorageId = field.storageId;
                    final Set<FieldMonitor> monitors = this.getMonitorsForField(fieldStorageId);
                    if (monitors != null && monitors.stream().anyMatch(new MonitoredPredicate(objTypeStorageId, fieldStorageId)))
                        set.add(this.buildHasFieldMonitorCacheKey(objTypeStorageId, fieldStorageId));
                }
            }
        }
        return set;
    }

    /**
     * Verify the given object exists before proceeding with the given mutation via {@link #mutateAndNotify(Mutation)}.
     *
     * @param id object containing the mutated field; will be validated
     * @param mutation change to apply
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    synchronized <V> V mutateAndNotify(ObjId id, Mutation<V> mutation) {

        // Verify object exists
        Preconditions.checkArgument(id != null, "null id");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (!this.exists(id))
            throw new DeletedObjectException(this, id);

        // Perform mutation
        return this.mutateAndNotify(mutation);
    }

    /**
     * Perform some action and, when entirely done (including re-entrant invocation), issue pending notifications to monitors.
     *
     * @param mutation change to apply
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws NullPointerException if {@code mutation} is null
     */
    private synchronized <V> V mutateAndNotify(Mutation<V> mutation) {

        // Validate transaction
        if (this.stale)
            throw new StaleTransactionException(this);

        // If re-entrant invocation, we're already set up
        if (this.pendingNotifications.get() != null)
            return mutation.mutate();

        // Set up pending report list, perform mutation, and then issue reports
        this.pendingNotifications.set(new TreeMap<>());
        try {
            return mutation.mutate();
        } finally {
            try {
                final TreeMap<Integer, ArrayList<FieldChangeNotifier<?>>> pendingNotificationMap = this.pendingNotifications.get();
                while (!pendingNotificationMap.isEmpty()) {

                    // Get the next field with pending notifications
                    final Map.Entry<Integer, ArrayList<FieldChangeNotifier<?>>> entry = pendingNotificationMap.pollFirstEntry();
                    final int storageId = entry.getKey();

                    // For all pending notifications, back-track references and notify all field monitors for the field
                    for (FieldChangeNotifier<?> notifier : entry.getValue()) {
                        assert notifier.getStorageId() == storageId;
                        final Set<FieldMonitor> monitors = this.getMonitorsForField(storageId);
                        if (monitors == null || monitors.isEmpty())
                            continue;
                        this.notifyFieldMonitors(notifier, NavigableSets.singleton(notifier.getId()), new ArrayList<>(monitors), 0);
                    }
                }
            } finally {
                this.pendingNotifications.remove();
            }
        }
    }

    // Recursively back-track references along monitor paths and notify monitors when we reach the end (i.e., beginning)
    private void notifyFieldMonitors(FieldChangeNotifier<?> notifier,
      NavigableSet<ObjId> objects, ArrayList<FieldMonitor> monitorList, int step) {

        // Find the monitors for whom we have completed all the steps in their (inverse) path,
        // and group the remaining monitors by their next inverted reference path step.
        final HashMap<Integer, ArrayList<FieldMonitor>> remainingMonitorsMap = new HashMap<>();
        for (FieldMonitor monitor : monitorList) {

            // Apply the monitor's type filter on the target object, if any
            if (step == 0) {
                final KeyRanges filter = monitor.getTargetFilter();
                if (filter != null && !filter.contains(notifier.getId().getBytes()))
                    continue;
            }

            // Issue notification callback if we have back-tracked through the whole path
            if (monitor.path.length == step) {
                this.notifyFieldChangeListener(notifier, monitor, objects);
                continue;
            }

            // Sort the unfinished monitors by their next (i.e., previous) reference field
            final int storageId = monitor.getStorageId(step);
            remainingMonitorsMap.computeIfAbsent(storageId, i -> new ArrayList<>()).add(monitor);
        }

        // Invert references for each group of remaining monitors and recurse
        for (Map.Entry<Integer, ArrayList<FieldMonitor>> entry : remainingMonitorsMap.entrySet()) {
            final int storageId = entry.getKey();
            final ArrayList<FieldMonitor> monitors = entry.getValue();
            assert monitors != null;
            final ArrayList<NavigableSet<ObjId>> refsList = new ArrayList<>(monitors.size());
            for (FieldMonitor monitor : entry.getValue())
                refsList.addAll(this.traverseReference(objects, -storageId, monitor.getFilter(step + 1)));
            if (!refsList.isEmpty())
                this.notifyFieldMonitors(notifier, NavigableSets.union(refsList), monitors, step + 1);
        }
    }

    // Notify listener, if it has the appropriate type
    private <T> void notifyFieldChangeListener(FieldChangeNotifier<T> notifier, FieldMonitor monitor, NavigableSet<ObjId> objects) {
        final T listener;
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
        startObjects.forEach(startIds::add);
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
                return NavigableSets.empty(FieldTypeRegistry.OBJ_ID);

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
        final SimpleFieldStorageInfo<ObjId> info = this.verifyReferenceFieldStorageInfo(storageId);

        // Traverse reference from each object
        final ArrayList<NavigableSet<ObjId>> refsList = new ArrayList<>();
        if (inverse) {

            // Get index and apply filter, if any
            CoreIndex<ObjId, ObjId> index = info.getIndex(this);
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
                info.readAllNonNull(this, id, refs, idFilter);
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
            this.verifyReferenceFieldStorageInfo(storageId);
        }
    }

    @SuppressWarnings("unchecked")
    private SimpleFieldStorageInfo<ObjId> verifyReferenceFieldStorageInfo(int storageId) {
        final SimpleFieldStorageInfo<?> info = this.schemas.verifyStorageInfo(storageId, SimpleFieldStorageInfo.class);
        if (!(info.fieldType instanceof ReferenceFieldType))
            throw new IllegalArgumentException(info + " is not a reference field");
        return (SimpleFieldStorageInfo<ObjId>)info;
    }

// Index Queries

    /**
     * Find all values stored in the specified {@link SimpleField} and, for each value,
     * the set of all objects having that value in the field.
     *
     * <p>
     * The {@code storageId} may refer to any {@link SimpleField}, whether part of an object or a sub-field
     * of a {@link ComplexField}.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the field is indexed;
     * this method does not check whether any such schema versions exist.
     *
     * @param storageId {@link SimpleField}'s storage ID
     * @return read-only, real-time view of field values mapped to sets of objects with the value in the field
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex<?, ObjId> queryIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final SimpleFieldStorageInfo<?> info = this.schemas.verifyStorageInfo(storageId, SimpleFieldStorageInfo.class);
        return info.getIndex(this);
    }

    /**
     * Find all values stored as an element in the specified {@link ListField} and, for each such value,
     * the set of all objects having that value as an element in the list and the corresponding list index.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the list element field is indexed;
     * this method does not check whether any such schema versions exist.
     *
     * @param storageId {@link ListField}'s element sub-field storage ID
     * @return read-only, real-time view of list element values, objects with the value in the list, and corresponding indices
     * @throws UnknownFieldException if no {@link ListField} element sub-field corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex2<?, ObjId, Integer> queryListElementIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final ListElementStorageInfo<?> info = this.schemas.verifyStorageInfo(storageId, ListElementStorageInfo.class);
        return info.getElementIndex(this);
    }

    /**
     * Find all values stored as a value in the specified {@link MapField} and, for each such value,
     * the set of all objects having that value as a value in the map and the corresponding key.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the map value field is indexed;
     * this method does not check whether any such schema versions exist.
     *
     * @param storageId {@link MapField}'s value sub-field storage ID
     * @return read-only, real-time view of map values, objects with the value in the map, and corresponding keys
     * @throws UnknownFieldException if no {@link MapField} value sub-field corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex2<?, ObjId, ?> queryMapValueIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final MapValueStorageInfo<?, ?> info = this.schemas.verifyStorageInfo(storageId, MapValueStorageInfo.class);
        return info.getValueIndex(this);
    }

    /**
     * Access a composite index on two fields.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions in which the composite index is defined.
     *
     * @param storageId composite index's storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws UnknownIndexException if {@code storageID} is unknown or does not correspond to a composite index on two fields
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex2<?, ?, ObjId> queryCompositeIndex2(int storageId) {
        final CompositeIndexStorageInfo indexInfo = this.schemas.verifyStorageInfo(storageId, CompositeIndexStorageInfo.class);
        final Object index = indexInfo.getIndex(this);
        if (!(index instanceof CoreIndex2)) {
            throw new UnknownIndexException(storageId, "the composite index with storage ID " + storageId
              + " is on " + indexInfo.storageIds.size() + " != 2 fields");
        }
        return (CoreIndex2<?, ?, ObjId>)index;
    }

    /**
     * Access a composite index on three fields.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions in which the composite index is defined.
     *
     * @param storageId composite index's storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws UnknownIndexException if {@code storageID} is unknown or does not correspond to a composite index on two fields
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex3<?, ?, ?, ObjId> queryCompositeIndex3(int storageId) {
        final CompositeIndexStorageInfo indexInfo = this.schemas.verifyStorageInfo(storageId, CompositeIndexStorageInfo.class);
        final Object index = indexInfo.getIndex(this);
        if (!(index instanceof CoreIndex3)) {
            throw new UnknownIndexException(storageId, "the composite index with storage ID " + storageId
              + " is on " + indexInfo.storageIds.size() + " != 3 fields");
        }
        return (CoreIndex3<?, ?, ?, ObjId>)index;
    }

    /**
     * Access a composite index on four fields.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions in which the composite index is defined.
     *
     * @param storageId composite index's storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws UnknownIndexException if {@code storageID} is unknown or does not correspond to a composite index on two fields
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex4<?, ?, ?, ?, ObjId> queryCompositeIndex4(int storageId) {
        final CompositeIndexStorageInfo indexInfo = this.schemas.verifyStorageInfo(storageId, CompositeIndexStorageInfo.class);
        final Object index = indexInfo.getIndex(this);
        if (!(index instanceof CoreIndex4)) {
            throw new UnknownIndexException(storageId, "the composite index with storage ID " + storageId
              + " is on " + indexInfo.storageIds.size() + " != 4 fields");
        }
        return (CoreIndex4<?, ?, ?, ?, ObjId>)index;
    }

    /**
     * Access any composite index by storage ID, regardless of the number of fields indexed.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions in which the composite index is defined.
     *
     * @param storageId composite index's storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws UnknownIndexException if {@code storageID} is unknown or does not correspond to a composite index
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public Object queryCompositeIndex(int storageId) {
        final CompositeIndexStorageInfo indexInfo = this.schemas.verifyStorageInfo(storageId, CompositeIndexStorageInfo.class);
        return indexInfo.getIndex(this);
    }

    /**
     * Find all objects that refer to the given target object through the/any reference field with the specified
     * {@link DeleteAction}.
     *
     * <p>
     * Because different schema versions can have different {@link DeleteAction}'s configured for the
     * same field, we have to iterate through each schema version separately.
     *
     * @param target referred-to object
     * @param onDelete {@link DeleteAction} to match
     * @return mapping from reference field storage ID to set of objects referring to {@code target} through a field whose
     *  {@link DeleteAction} matches {@code onDelete}
     */
    @SuppressWarnings("unchecked")
    private TreeMap<Integer, NavigableSet<ObjId>> findReferrers(ObjId target, DeleteAction onDelete) {
        assert Thread.holdsLock(this);

        // Get target object type storage ID
        final int targetStorageId = target.getStorageId();

        // Determine which schema versions actually have objects that exist; if there's only one we can slightly optimize below
        final ArrayList<Map.Entry<Integer, NavigableSet<ObjId>>> versionList = new ArrayList<>(5);
        versionList.addAll(this.queryVersion().asMap().entrySet());
        final boolean multipleVersions = versionList.size() > 1;

        // Search for objects one schema version at a time, and group them by reference field
        final TreeMap<Integer, Object> result = new TreeMap<>();
        for (Map.Entry<Integer, NavigableSet<ObjId>> versionListEntry : versionList) {
            final int schemaVersionNumber = versionListEntry.getKey();
            final NavigableSet<ObjId> schemaVersionRefs = versionListEntry.getValue();

            // Get corresponding Schema object
            final Schema schemaVersion = this.schemas.versions.get(schemaVersionNumber);
            if (schemaVersion == null)
                throw new InconsistentDatabaseException("encountered objects with unknown schema version " + schemaVersionNumber);

            // Iterate over reference fields in this schema version that have the configured DeleteAction in some object type
            for (Map.Entry<ReferenceField, KeyRanges> fieldRangeEntry :
              schemaVersion.deleteActionKeyRanges.get(onDelete.ordinal()).entrySet()) {
                final ReferenceField field = fieldRangeEntry.getKey();
                final KeyRanges keyRanges = fieldRangeEntry.getValue();

                // Do a quick check to see whether this field can possibly refer to the target object
                final SortedSet<Integer> targetTypes = field.getObjectTypes();
                if (targetTypes != null && !targetTypes.contains(targetStorageId))
                    continue;

                // Build the key prefix for the target object ID in this field's index
                final int fieldStorageId = field.storageId;
                final ByteWriter writer = new ByteWriter(UnsignedIntEncoder.encodeLength(fieldStorageId) + ObjId.NUM_BYTES);
                UnsignedIntEncoder.write(writer, fieldStorageId);
                target.writeTo(writer);
                final byte[] prefix = writer.getBytes();

                // Query the index to get all objects referring to the target object through this field (in any schema version)
                final IndexSet<ObjId> indexSet = new IndexSet<>(this.kvt, FieldTypeRegistry.OBJ_ID, true, prefix);

                // Now restrict those referrers to only those object types where the field's DeleteAction matches (if necessary)
                NavigableSet<ObjId> referrers = keyRanges != null ? indexSet.filterKeys(keyRanges) : indexSet;

                // Anything there?
                if (referrers.isEmpty())
                    continue;

                // Add these referrers, restricted to the current schema version, to our list of referrers for this field
                if (multipleVersions) {
                    ((ArrayList<NavigableSet<ObjId>>)result
                      .computeIfAbsent(fieldStorageId, i -> new ArrayList<NavigableSet<ObjId>>(versionList.size())))
                      .add(NavigableSets.intersection(schemaVersionRefs, referrers));
                } else
                    result.put(fieldStorageId, referrers);              // no schema version restriction necessary; no list needed
            }
        }

        // If there were multiple schema versions, for each reference field, take the union of the sets from each schema version
        if (multipleVersions) {
            for (Map.Entry<Integer, Object> entry : result.entrySet()) {
                final ArrayList<NavigableSet<ObjId>> list = (ArrayList<NavigableSet<ObjId>>)entry.getValue();
                final NavigableSet<ObjId> union = list.size() == 1 ? list.get(0) : NavigableSets.union(list);
                entry.setValue(union);
            }
        }

        // Return referrer sets grouped by reference field
        return (TreeMap<Integer, NavigableSet<ObjId>>)(Object)result;
    }

    private byte[] buildCompositeIndexEntry(ObjId id, CompositeIndex index) {
        return Transaction.buildCompositeIndexEntry(this, id, index);
    }

    private static byte[] buildDefaultCompositeIndexEntry(ObjId id, CompositeIndex index) {
        return Transaction.buildCompositeIndexEntry(null, id, index);
    }

    private static byte[] buildCompositeIndexEntry(Transaction tx, ObjId id, CompositeIndex index) {
        final ByteWriter writer = new ByteWriter();
        UnsignedIntEncoder.write(writer, index.storageId);
        for (SimpleField<?> field : index.fields) {
            final byte[] value = tx != null ? tx.kvt.get(field.buildKey(id)) : null;
            writer.write(value != null ? value : field.fieldType.getDefaultValue());
        }
        id.writeTo(writer);
        return writer.getBytes();
    }

// Listener snapshots

    /**
     * Create a read-only snapshot of all ({@link CreateListener}s, {@link DeleteListener}s, {@link VersionChangeListener}s,
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
     * attempts to register or unregister individual listeners will result in an {@link UnsupportedOperationException}.
     *
     * @param listeners listener set created by {@link #snapshotListeners}
     * @throws IllegalArgumentException if {@code listeners} was created from a transaction with an incompatible schema
     * @throws IllegalArgumentException if {@code listeners} is null
     */
    public synchronized void setListeners(ListenerSet listeners) {
        Preconditions.checkArgument(listeners != null, "null listeners");

        // Verify field change listeners are compatible with this transaction
        while (listeners.monitorMap != null) {

            // Do a quick check
            if (Arrays.equals(listeners.schema.encodedXML, this.schema.encodedXML))
                break;

            // Do a slow check
            if (listeners.schema.schemaModel.isCompatibleWith(this.schema.schemaModel))
                break;

            // Not compatible
            throw new IllegalArgumentException("listener set was created from a transaction having an incompatible schema");
        }

        // Apply listeners to this instance
        this.versionChangeListeners = listeners.versionChangeListeners;
        this.createListeners = listeners.createListeners;
        this.deleteListeners = listeners.deleteListeners;
        this.monitorMap = listeners.monitorMap;
        this.hasFieldMonitorCache = listeners.hasFieldMonitorCache;
        this.listenerSetInstalled = true;
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

// Mutation

    interface Mutation<V> {
        V mutate();
    }

// SimpleFieldChangeNotifier

    private abstract static class SimpleFieldChangeNotifier extends FieldChangeNotifier<SimpleFieldChangeListener> {

        SimpleFieldChangeNotifier(SimpleField<?> field, ObjId id) {
            super(SimpleFieldChangeListener.class, field.storageId, id);
        }
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
     * A fixed collection of listeners ({@link CreateListener}s, {@link DeleteListener}s, {@link VersionChangeListener}s,
     * {@link SimpleFieldChangeListener}s, {@link SetFieldChangeListener}s, {@link ListFieldChangeListener}s, and
     * {@link MapFieldChangeListener}s) that can be efficiently registered on a {@link Transaction} all at once.
     *
     * <p>
     * To create an instance of this class, use {@link Transaction#snapshotListeners} after registering the desired
     * set of listeners. Once created, the instance can be used repeatedly to configured the same set of listeners
     * on any other compatible {@link Transaction} via {@link Transaction#setListeners Transaction.setListeners()}.
     * Here "compatible" means field change listener paths and types are valid for the new transaction's schemas.
     */
    public static final class ListenerSet {

        final Set<VersionChangeListener> versionChangeListeners;
        final Set<CreateListener> createListeners;
        final Set<DeleteListener> deleteListeners;
        final NavigableMap<Integer, Set<FieldMonitor>> monitorMap;
        final NavigableSet<Long> hasFieldMonitorCache;

        final Schema schema;

        private ListenerSet(Transaction tx) {
            assert Thread.holdsLock(tx);
            this.versionChangeListeners = tx.versionChangeListeners != null ?
              Collections.unmodifiableSet(new HashSet<>(tx.versionChangeListeners)) : null;
            this.createListeners = tx.createListeners != null ?
              Collections.unmodifiableSet(new HashSet<>(tx.createListeners)) : null;
            this.deleteListeners = tx.deleteListeners != null ?
              Collections.unmodifiableSet(new HashSet<>(tx.deleteListeners)) : null;
            if (tx.monitorMap != null) {
                final TreeMap<Integer, Set<FieldMonitor>> monitorMapSnapshot = new TreeMap<>();
                for (Map.Entry<Integer, Set<FieldMonitor>> entry : tx.monitorMap.entrySet())
                    monitorMapSnapshot.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
                this.monitorMap = Collections.unmodifiableNavigableMap(monitorMapSnapshot);
            } else
                this.monitorMap = null;
            this.hasFieldMonitorCache = tx.buildHasFieldMonitorCache();
            this.schema = tx.schema;
        }
    }

// MonitoredPredicate

    // Matches FieldMonitors who monitor the specified field in the specified object type
    private static final class MonitoredPredicate implements Predicate<FieldMonitor> {

        private final byte[] objTypeBytes;
        private final int fieldStorageId;

        MonitoredPredicate(int objTypeStorageId, int fieldStorageId) {
            this.objTypeBytes = ObjId.getMin(objTypeStorageId).getBytes();
            this.fieldStorageId = fieldStorageId;
        }

        @Override
        public boolean test(FieldMonitor monitor) {
            assert monitor != null;
            if (monitor.storageId != this.fieldStorageId)
                return false;
            final KeyRanges filter = monitor.getTargetFilter();
            return filter == null || filter.contains(this.objTypeBytes);
        }
    }
}

