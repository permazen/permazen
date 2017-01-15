
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

import org.jsimpledb.core.util.ObjIdMap;
import org.jsimpledb.core.util.ObjIdSet;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.NavigableSets;
import org.jsimpledb.util.UnsignedIntEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JSimpleDB {@link Database} transaction.
 *
 * <p>
 * Note: this is the lower level, core API for {@link org.jsimpledb.JSimpleDB}. In most cases this API
 * will only be used indirectly through the higher level {@link org.jsimpledb.JSimpleDB}, {@link org.jsimpledb.JTransaction},
 * and {@link org.jsimpledb.JObject} APIs.
 *
 * <p>
 * Methods in this class can be divided into the following categories:
 *
 * <p>
 * <b>Transaction Meta-Data</b>
 * <ul>
 *  <li>{@link #getDatabase getDatabase()} - Get the associated {@link Database}</li>
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
 *  <li>{@link #copy(ObjId, ObjId, Transaction, boolean, boolean) copy()} - Copy an object's fields
 *      onto another object in a (possibly) different transaction</li>
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
 *  <li>{@link #getKey getKey(ObjId)} - Get the {@link org.jsimpledb.kv.KVDatabase} key corresponding to an object</li>
 *  <li>{@link #getKey getKey(ObjId, int)} - Get the {@link org.jsimpledb.kv.KVDatabase}
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
 * <b>Reference Inversion</b>
 * <ul>
 *  <li>{@link #invertReferencePath invertReferencePath()} - Find all objects that refer to any element in a given set
 *      of objects through a specified reference path</li>
 * </ul>
 *
 * <p>
 * <b>Index Queries</b>
 * <ul>
 *  <li>{@link #queryIndex queryIndex()} - Query the index associated with a {@link SimpleField}
 *      to identify all values and all objects having those values</li>
 *  <li>{@link #queryListElementIndex queryListElementIndex()} - Query the index associated with a {@link ListField}
 *      to identify all list elements, all objects having those elements in the list, and thier corresponding indicies</li>
 *  <li>{@link #queryMapValueIndex queryMapValueIndex()} - Query the index associated with a {@link MapField}
 *      to identify all map values, all objects having those values in the map, and the corresponding keys</li>
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
    boolean readOnly;
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
    private boolean listenerSetInstalled;

    // Callbacks
    @GuardedBy("this")
    private LinkedHashSet<Callback> callbacks;

    // Misc
    @GuardedBy("this")
    private final ThreadLocal<TreeMap<Integer, ArrayList<FieldChangeNotifier>>> pendingNotifications = new ThreadLocal<>();
    @GuardedBy("this")
    private final ObjIdMap<ObjInfo> objInfoCache = new ObjIdMap<>();
    @GuardedBy("this")
    private Object userObject;

    // Recording of deleted assignments used during a copy() operation (otherwise should be null)
    private ObjIdMap<ReferenceField> deletedAssignments;

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
        this.db.deleteSchema(this.kvt, version);
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
     * @throws org.jsimpledb.kv.RetryTransactionException from {@link KVTransaction#commit KVTransaction.commit()}
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
            this.log.debug("commit() invoked on transaction " + this + " marked rollback-only, rolling back");
            this.rollback();
            throw new RollbackOnlyTransactionException(this);
        }
        this.ending = true;

        // Do beforeCommit() and beforeCompletion() callbacks
        if (this.log.isTraceEnabled())
            this.log.trace("commit() invoked on" + (this.readOnly ? " read-only" : "") + " transaction " + this);
        Callback failedCallback = null;
        try {
            if (this.callbacks != null) {
                for (Callback callback : this.callbacks) {
                    failedCallback = callback;
                    if (this.log.isTraceEnabled())
                        this.log.trace("commit() invoking beforeCommit() on transaction " + this + " callback " + callback);
                    callback.beforeCommit(this.readOnly);
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
            if (this.readOnly)
                this.kvt.rollback();
            else
                this.kvt.commit();
            if (this.callbacks != null) {
                for (Callback callback : this.callbacks) {
                    failedCallback = callback;
                    if (this.log.isTraceEnabled())
                        this.log.trace("commit() invoking afterCommit() on transaction " + this + " callback " + callback);
                    callback.afterCommit();
                }
                failedCallback = null;
            }
        } finally {

            // Log the offending callback, if any
            if (failedCallback != null)
                this.log.warn("error invoking afterCommit() method on transaction " + this + " callback " + failedCallback);

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
            this.log.trace("rollback() invoked on" + (this.readOnly ? " read-only" : "") + " transaction " + this);

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
                this.log.trace("invoking beforeCompletion() on transaction " + this + " callback " + callback);
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
                this.log.trace("invoking afterCompletion() on transaction " + this + " callback " + callback);
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
     * @return true if this instance is read-only
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean isReadOnly() {
        if (this.stale)
            throw new StaleTransactionException(this);
        return this.readOnly;
    }

    /**
     * Enable or disaable read-only mode.
     *
     * <p>
     * Read-only transactions allow mutations, but all changes are discarded on {@link #commit},
     * as if {@link #rollback} were invoked. Registered {@link Callback}s are still processed normally.
     *
     * @param readOnly read-only setting
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void setReadOnly(boolean readOnly) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.readOnly = readOnly;
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
     * Note: if you are using Spring for transaction demarcation (via {@link org.jsimpledb.spring.JSimpleDBTransactionManager}),
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
        this.db.copyMetaData(this, kvstore);
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
        this.kvt.put(Database.buildVersionIndexKey(id, objType.schema.versionNumber), ByteUtil.EMPTY);

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
            for (ReferenceFieldStorageInfo fieldInfo :
              Iterables.filter(this.schemas.storageInfos.values(), ReferenceFieldStorageInfo.class)) {
                for (ObjId referrer : this.findReferrers(id, DeleteAction.EXCEPTION, fieldInfo.storageId)) {
                    if (!referrer.equals(id))
                        throw new ReferencedObjectException(id, referrer, fieldInfo.storageId);
                }
            }

            // Do we need to issue delete notifications for the object being deleted?
            if (info.isDeleteNotified() || this.deleteListeners == null || this.deleteListeners.isEmpty())
                break;

            // Set "delete notified" flag and update object info cache
            ObjInfo.write(this, id, info.getVersion(), true);
            this.objInfoCache.put(id, new ObjInfo(this, id, info.getVersion(), true, info.schema, info.objType));

            // Issue delete notifications and retry
            if (!this.disableListenerNotifications && this.deleteListeners != null) {
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
        for (ReferenceFieldStorageInfo fieldInfo :
          Iterables.filter(this.schemas.storageInfos.values(), ReferenceFieldStorageInfo.class)) {
            final NavigableSet<ObjId> referrers = this.findReferrers(id, DeleteAction.UNREFERENCE, fieldInfo.storageId);
            if (fieldInfo.isSubField()) {
                final ComplexFieldStorageInfo<?> superFieldInfo = this.schemas.verifyStorageInfo(
                  fieldInfo.superFieldStorageId, ComplexFieldStorageInfo.class);
                superFieldInfo.unreferenceAll(this, fieldInfo.storageId, id, referrers);
            } else {
                final int storageId = fieldInfo.storageId;
                for (ObjId referrer : referrers)
                    this.writeSimpleField(referrer, storageId, null, false);
            }
        }

        // Find all DELETE references and mark the containing object for deletion (caller will call us back to actually delete)
        deletables.addAll(this.findReferrers(id, DeleteAction.DELETE, -1));

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
        this.kvt.remove(Database.buildVersionIndexKey(id, info.getVersion()));

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
     * Copy all of an object's fields onto a target object in a (possibly) different transaction, replacing any previous values.
     *
     * <p>
     * If {@code updateVersion} is true, the {@code source} object is first upgraded to
     * {@linkplain #getSchema() the schema version associated with this transaction}.
     * In any case, the schema version associated with {@code source} when copied must be identical
     * in this transaction and {@code dest}.
     *
     * <p>
     * Only the object's fields are copied; any other objects they reference are not copied. If the {@code target} object
     * does not exist in {@code dest}, it will be created first (and {@link CreateListener}s notified); if {@code target}
     * does exist in {@code dest}, its schema version will be upgraded if necessary to match {@code source} (and any registered
     * {@link VersionChangeListener}s notified).
     * (Meaningful) changes to {@code target}'s fields generate change listener notifications.
     *
     * <p>
     * If {@code source} contains references to any objects that don't exist in {@code dest} through fields configured
     * to {@linkplain ReferenceField#isAllowDeleted disallow deleted assignments}, then a {@link DeletedObjectException}
     * is thrown and no copy is performed. To perform a copy that allows such deleted assignments, use
     * {@link #copy(ObjId, ObjId, Transaction, boolean, boolean, ObjIdMap)}.
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     *
     * <p>
     * If {@code dest} is this instance, and {@code source} equals {@code target}, no fields are changed and false is returned;
     * however, a schema update may occur (if {@code updateVersion} is true), and deleted assignment checks are applied.
     *
     * <p>
     * The {@code notifyListeners} flag controls whether notifications are delivered to {@link CreateListener}s
     * and field change listeners as objects are created and modified in {@code dest}.
     *
     * @param source object ID of the source object in this transaction
     * @param target object ID of the target object in {@code dest}
     * @param dest destination transaction containing {@code target} (possibly same as this transaction)
     * @param updateVersion true to first automatically update {@code source}'s schema version, false to not change it
     * @param notifyListeners whether to notify {@link CreateListener}s and field change listeners
     * @return false if object already existed in {@code dest}, true if {@code target} did not exist in {@code dest}
     * @throws DeletedObjectException if no object with ID equal to {@code source} is found in this transaction
     * @throws DeletedObjectException if a non-null reference field in {@code source} that disallows deleted assignments
     *  contains a reference to an object that does not exist in {@code dest}
     * @throws UnknownTypeException if {@code source} or {@code target} specifies an unknown object type
     * @throws IllegalArgumentException if {@code source} and {@code target} specify different object types
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if any {@code source} and {@code target} have different object types
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws SchemaMismatchException if the schema version associated with {@code source} differs between
     *  this transaction and {@code dest}
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     */
    public boolean copy(ObjId source, ObjId target, Transaction dest, boolean updateVersion, boolean notifyListeners) {
        return this.copy(source, target, dest, updateVersion, notifyListeners, null);
    }

    /**
     * Variant of {@link #copy(ObjId, ObjId, Transaction, boolean, boolean)} that allows, but tracks, deleted assignments,
     * and supports disabling create and change notifications.
     *
     * <p>
     * When multiple objects need to be copied, {@link #copy(ObjId, ObjId, Transaction, boolean, boolean) copy()} can throw a
     * {@link DeletedObjectException} if an object is copied before some other object that it references through a
     * {@link ReferenceField} with {@link ReferenceField#allowDeleted} set to false. If there are cycles in the
     * graph of references between objects, this situation is impossible to avoid.
     *
     * <p>
     * In this variant, instead of triggering an exception, illegal references to deleted objects are collected in
     * {@code deletedAssignments}, which maps each deleted object to (some) referring field in {@code target}.
     * This allows the caller to decide what to do about them.
     *
     * <p>
     * If a null {@code deletedAssignments} is given, and any illegal deleted assignment would occur, then an immediate
     * {@link DeletedObjectException} is thrown and no copy is performed.
     *
     * <p>
     * The {@code notifyListeners} flag controls whether notifications are delivered to {@link CreateListener}s
     * and field change listeners as objects are created and modified in {@code dest}.
     *
     * @param source object ID of the source object in this transaction
     * @param target object ID of the target object in {@code dest}
     * @param dest destination transaction containing {@code target} (possibly same as this transaction)
     * @param updateVersion true to first automatically update {@code source}'s schema version, false to not change it
     * @param notifyListeners whether to notify {@link CreateListener}s and field change listeners
     * @param deletedAssignments if not null, collect assignments to deleted objects here instead of throwing
     *  {@link DeletedObjectException}s, where the map key is the deleted object and the map value is some referring field
     * @return false if object already existed in {@code dest}, true if {@code target} did not exist in {@code dest}
     * @throws DeletedObjectException if no object with ID equal to {@code source} exists in this transaction
     * @throws DeletedObjectException if {@code deletedAssignments} is null, and a non-null reference field in {@code source}
     *  that disallows deleted assignments contains a reference to an object that does not exist in {@code dest}
     * @throws UnknownTypeException if {@code source} or {@code target} specifies an unknown object type
     * @throws IllegalArgumentException if {@code source} and {@code target} specify different object types
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if any {@code source} and {@code target} have different object types
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws SchemaMismatchException if the schema version associated with {@code source} differs between
     *  this transaction and {@code dest}
     * @throws TypeNotInSchemaVersionException {@code updateVersion} is true and the object could not be updated because
     *   the object's type does not exist in the schema version associated with this transaction
     */
    public synchronized boolean copy(ObjId source, final ObjId target, final Transaction dest, final boolean updateVersion,
      final boolean notifyListeners, final ObjIdMap<ReferenceField> deletedAssignments) {

        // Sanity check
        Preconditions.checkArgument(source != null, "null source");
        Preconditions.checkArgument(target != null, "null target");
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
                final ObjIdMap<ReferenceField> previousCopyDeletedAssignments = dest.deletedAssignments;
                dest.deletedAssignments = deletedAssignments;
                final boolean previousDisableListenerNotifications = dest.disableListenerNotifications;
                dest.disableListenerNotifications = !notifyListeners;
                try {
                    return Transaction.doCopyFields(srcInfo, target, Transaction.this, dest, updateVersion);
                } finally {
                    dest.deletedAssignments = previousCopyDeletedAssignments;
                    dest.disableListenerNotifications = previousDisableListenerNotifications;
                }
            });
        }
    }

    // This method assumes both transactions are locked
    private static boolean doCopyFields(ObjInfo srcInfo, ObjId dstId, Transaction srcTx, Transaction dstTx, boolean updateVersion) {

        // Sanity check
        assert Thread.holdsLock(srcTx);
        assert Thread.holdsLock(dstTx);

        // Verify objects have the same type
        final ObjId srcId = srcInfo.getId();
        final int typeStorageId = srcId.getStorageId();
        if (dstId.getStorageId() != typeStorageId) {
            throw new IllegalArgumentException("can't copy " + srcId + " to " + dstId
              + " due to non-equal object types (" + typeStorageId + " != " + dstId.getStorageId() + ")");
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
        if (!Arrays.equals(srcSchema.encodedXML, dstSchema.encodedXML)) {
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

        // Do field-by-field copy if there are change listeners, otherwise do fast copy of key/value pairs directly
        final ObjType srcType = srcSchema.getObjType(typeStorageId);
        final ObjType dstType = dstSchema.getObjType(typeStorageId);
        if (!dstTx.disableListenerNotifications && dstTx.hasFieldMonitor(dstType)) {

            // Create destination object if it does not exist
            if (!existed)
                dstTx.createObjectData(dstId, objectVersion, dstSchema, dstType);

            // Copy fields
            for (Field<?> field : srcType.fields.values())
                field.copy(srcId, dstId, srcTx, dstTx);
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
            dstTx.kvt.put(Database.buildVersionIndexKey(dstId, objectVersion), ByteUtil.EMPTY);

            // Copy object meta-data and all field content in one key range sweep
            final byte[] srcMinKey = srcId.getBytes();
            final byte[] srcMaxKey = ByteUtil.getKeyAfterPrefix(srcMinKey);
            final ByteWriter dstWriter = new ByteWriter();
            dstWriter.write(dstId.getBytes());
            final int dstMark = dstWriter.mark();
            final Iterator<KVPair> i = srcTx.kvt.getRange(srcMinKey, srcMaxKey, false);
            while (i.hasNext()) {
                final KVPair kv = i.next();
                assert new KeyRange(srcMinKey, srcMaxKey).contains(kv.getKey());
                final ByteReader srcReader = new ByteReader(kv.getKey());
                srcReader.skip(srcMinKey.length);
                dstWriter.reset(dstMark);
                dstWriter.write(srcReader);
                dstTx.kvt.put(dstWriter.getBytes(), kv.getValue());
            }
            Database.closeIfPossible(i);

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

    //////// Update counter fields

        // Get counter field storage IDs (old or new)
        final TreeSet<Integer> counterFieldStorageIds = new TreeSet<>();
        counterFieldStorageIds.addAll(oldType.counterFields.keySet());
        counterFieldStorageIds.addAll(newType.counterFields.keySet());

        // Update counter fields
        for (int storageId : counterFieldStorageIds) {

            // Get old and new counter fields having this storage ID
            final CounterField oldField = oldType.counterFields.get(storageId);
            final CounterField newField = newType.counterFields.get(storageId);

            // Save old field values for version change notification
            final byte[] key = Field.buildKey(id, storageId);
            if (oldField != null && oldValueMap != null) {
                final byte[] oldValue = this.kvt.get(key);
                final long value = oldValue != null ? this.kvt.decodeCounter(oldValue) : 0;
                oldValueMap.put(storageId, value);
            }

            // Remove old value if field has disappeared, otherwise leave alone
            if (newField == null)
                this.kvt.remove(key);

            // For newly created counter fields, we must initialize them to zero
            if (oldField == null)
                this.kvt.put(newField.buildKey(id), this.kvt.encodeCounter(0));
        }

    //////// Update simple fields and corresponding index entries

        // Get simple field storage IDs (old or new)
        final TreeSet<Integer> simpleFieldStorageIds = new TreeSet<>();
        simpleFieldStorageIds.addAll(oldType.simpleFields.keySet());
        simpleFieldStorageIds.addAll(newType.simpleFields.keySet());

        // Update simple fields
        for (int storageId : simpleFieldStorageIds) {

            // Get old and new simple fields having this storage ID
            final SimpleField<?> oldField = oldType.simpleFields.get(storageId);
            final SimpleField<?> newField = newType.simpleFields.get(storageId);

            // Save old field values for version change notification
            final byte[] key = Field.buildKey(id, storageId);
            final byte[] oldValue = oldField != null ? this.kvt.get(key) : null;
            if (oldField != null && oldValueMap != null) {
                final Object value = oldValue != null ?
                  oldField.fieldType.read(new ByteReader(oldValue)) : oldField.fieldType.getDefaultValueObject();
                oldValueMap.put(storageId, value);
            }

            // If field is being removed, discard the old value
            if (newField == null && oldValue != null)
                this.kvt.remove(key);

            // Remove old index entry if index removed in new version
            if (oldField != null && oldField.indexed && (newField == null || !newField.indexed))
                this.kvt.remove(Transaction.buildSimpleIndexEntry(oldField, id, oldValue));

            // Add new index entry if index added in new version
            if (newField != null && newField.indexed && (oldField == null || !oldField.indexed))
                this.kvt.put(Transaction.buildSimpleIndexEntry(newField, id, oldValue), ByteUtil.EMPTY);
        }

    //////// Add composite index entries for newly added composite indexes

        // Add index entries for composite indexes that are newly added
        newType.compositeIndexes.values().stream()
          .filter(index -> !oldType.compositeIndexes.containsKey(index.storageId))
          .forEach(index -> this.kvt.put(this.buildCompositeIndexEntry(id, index), ByteUtil.EMPTY));

    //////// Update complex fields and corresponding index entries

        // Get complex field storage IDs (old or new)
        final TreeSet<Integer> complexFieldStorageIds = new TreeSet<>();
        complexFieldStorageIds.addAll(oldType.complexFields.keySet());
        complexFieldStorageIds.addAll(newType.complexFields.keySet());

        // Notes:
        //
        // - The only changes we support are sub-field changes that don't affect the corresponding StorageInfo's
        // - New complex fields do not need to be explicitly initialized because their initial state is to have zero KV pairs
        //
        for (int storageId : complexFieldStorageIds) {

            // Get old and new complex fields having this storage ID
            final ComplexField<?> oldField = oldType.complexFields.get(storageId);
            final ComplexField<?> newField = newType.complexFields.get(storageId);

            // If there is no old field, new field and any associated indexes are already initialized (i.e., they're empty)
            if (oldField == null)
                continue;

            // Save old field's value
            if (oldValueMap != null)
                oldValueMap.put(storageId, oldField.getValueReadOnlyCopy(this, id));

            // If field is being removed, delete old field content, otherwise check if index entries should be added/removed
            if (newField == null) {
                oldField.removeIndexEntries(this, id);
                oldField.deleteContent(this, id);
            } else
                newField.updateSubFieldIndexes(this, oldField, id);
        }

    //////// Remove references that are no longer valid

        // Unreference any reference fields that refer to no-longer-valid object types
        for (int storageId : simpleFieldStorageIds) {

            // Get storage IDs that are no longer allowed, if any
            final TreeSet<Integer> removedObjectTypes = this.findRemovedObjectTypes(info, targetVersion, storageId);
            if (removedObjectTypes == null)
                continue;

            // Unreference field if needed
            final ReferenceField oldField = (ReferenceField)oldType.simpleFields.get(storageId);
            final ObjId ref = oldField.getValue(this, id);
            if (ref != null && removedObjectTypes.contains(ref.getStorageId()))
                this.writeSimpleField(id, oldField.storageId, null, false);
        }

        // Unreference any reference sub-fields of complex fields that refer to no-longer-valid object types
        for (int storageId : complexFieldStorageIds) {

            // Get old and new complex fields having this storage ID
            final ComplexField<?> oldField = oldType.complexFields.get(storageId);
            final ComplexField<?> newField = newType.complexFields.get(storageId);
            if (oldField == null || newField == null)
                continue;
            final List<? extends SimpleField<?>> oldSubFields = oldField.getSubFields();
            final List<? extends SimpleField<?>> newSubFields = newField.getSubFields();
            final int numSubFields = oldSubFields.size();
            assert numSubFields == newSubFields.size();

            // Iterate over subfields
            for (int i = 0; i < numSubFields; i++) {
                final SimpleField<?> oldSubField = oldSubFields.get(i);
                final SimpleField<?> newSubField = newSubFields.get(i);
                assert oldSubField.storageId == newSubField.storageId;
                final int subStorageId = oldSubField.storageId;

                // Get storage IDs that are no longer allowed, if any
                final TreeSet<Integer> removedObjectTypes = this.findRemovedObjectTypes(info, targetVersion, subStorageId);
                if (removedObjectTypes == null)
                    continue;

                // Unreference sub-fields as needed
                oldField.unreferenceRemovedObjectTypes(this, id, (ReferenceField)oldSubField, removedObjectTypes);
            }
        }

    //////// Update object version and corresponding index entry

        // Change object version and update object info cache
        ObjInfo.write(this, id, newVersion, info.isDeleteNotified());
        this.objInfoCache.put(id, new ObjInfo(this, id, newVersion, info.isDeleteNotified(), targetVersion, newType));

        // Update object version index entry
        this.kvt.remove(Database.buildVersionIndexKey(id, oldVersion));
        this.kvt.put(Database.buildVersionIndexKey(id, newVersion), ByteUtil.EMPTY);

    //////// Notify listeners

        // Lock down old field values map
        final NavigableMap<Integer, Object> readOnlyOldValuesMap = oldValueMap != null ?
          Maps.unmodifiableNavigableMap(oldValueMap) : null;

        // Notify about version update
        if (this.versionChangeListeners != null) {
            for (VersionChangeListener listener : this.versionChangeListeners)
                listener.onVersionChange(this, id, oldVersion, newVersion, readOnlyOldValuesMap);
        }
    }

    /**
     * Find storage ID's which are no longer allowed by a reference field when upgrading to the specified
     * schema version and therefore need to be scrubbed during the upgrade.
     */
    private TreeSet<Integer> findRemovedObjectTypes(ObjInfo info, Schema newVersion, int storageId) {

        // Get old and new object types
        final ObjType oldType = info.getObjType();
        final ObjType newType = newVersion.getObjType(info.getId().getStorageId());

        // Get old and new reference fields having specified storage ID
        final ReferenceField oldField = oldType.referenceFieldsAndSubFields.get(storageId);
        final ReferenceField newField = newType.referenceFieldsAndSubFields.get(storageId);
        if (oldField == null || newField == null)
            return null;

        // Check allowed storage IDs
        final SortedSet<Integer> newObjectTypes = newField.getObjectTypes();
        if (newObjectTypes == null)
            return null;                                                // new field can refer to any type in any schema version
        SortedSet<Integer> oldObjectTypes = oldField.getObjectTypes();
        if (oldObjectTypes == null)
            oldObjectTypes = this.schemas.objTypeStorageIds;            // old field can refer to any type in any schema version

        // Identify storage IDs which are were allowed by old field but are no longer allowed by new field
        final TreeSet<Integer> removedObjectTypes = new TreeSet<>(oldObjectTypes);
        removedObjectTypes.removeAll(newObjectTypes);
        return !removedObjectTypes.isEmpty() ? removedObjectTypes : null;
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
        return this.db.getVersionIndex(this);
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
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(id != null, "null id");

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
        this.mutateAndNotify(id, new Mutation<Void>() {
            @Override
            public Void mutate() {
                Transaction.this.doWriteSimpleField(id, storageId, value, updateVersion);
                return null;
            }
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
            for (Map.Entry<CompositeIndex, Integer> entry : field.compositeIndexMap.entrySet()) {
                final CompositeIndex index = entry.getKey();
                final int fieldIndexOffset = entry.getValue();

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
        throw new DeletedObjectException(targetId, "illegal assignment to " + field + " in object " + id
          + " (" + this.getTypeDescription(id) + ") of reference to deleted object " + targetId
          + " (" + this.getTypeDescription(targetId) + ")");
    }

    /**
     * Bulid a simple index entry for the given field, object ID, and field value.
     *
     * @param field simple field
     * @param id ID of object containing the field
     * @param value encoded field value, or null for default value
     * @return index key
     */
    private static byte[] buildSimpleIndexEntry(SimpleField<?> field, ObjId id, byte[] value) {
        final ByteWriter writer = new ByteWriter();
        UnsignedIntEncoder.write(writer, field.storageId);
        writer.write(value != null ? value : field.fieldType.getDefaultValue());
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
        return type != null ? "type " + type.getName() + "#" + storageId : "type with storage ID #" + storageId;
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
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(id != null, "null id");

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
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(id != null, "null id");

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
        if (this.stale)
            throw new StaleTransactionException(this);

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
     *  <li>Objects utilize mutiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link org.jsimpledb.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param id object ID
     * @return the {@link org.jsimpledb.kv.KVDatabase} key corresponding to {@code id}
     * @throws IllegalArgumentException if {@code id} is null
     * @see org.jsimpledb.JTransaction#getKey(org.jsimpledb.JObject) JTransaction.getKey()
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
     *  <li>Complex fields utilize mutiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link org.jsimpledb.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param id object ID
     * @param storageId field storage ID
     * @return the {@link org.jsimpledb.kv.KVDatabase} key of the field in the specified object
     * @throws UnknownFieldException if no field corresponding to {@code storageId} exists
     * @throws IllegalArgumentException if {@code storageId} corresponds to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code storageId} is less than or equal to zero
     * @throws IllegalArgumentException if {@code id} is null
     * @see org.jsimpledb.JTransaction#getKey(org.jsimpledb.JObject, String) JTransaction.getKey()
     * @see org.jsimpledb.kv.KVTransaction#watchKey KVTransaction.watchKey()
     */
    public byte[] getKey(ObjId id, int storageId) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(storageId > 0, "non-positive storageId");
        final FieldStorageInfo info = this.schemas.verifyStorageInfo(storageId, FieldStorageInfo.class);
        if (info.isSubField())
            throw new IllegalArgumentException("field is a sub-field of a complex field");

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
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(id != null, "null id");

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

        // Is a schema udpate required?
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
     * When the specified field is changed, a listener notification will be delivered for each referring object that
     * refers to the object containing the changed field through the specified path of references. Notifications are
     * delivered at the end of the mutation operation just prior to returning to the caller. If the listener method
     * performs additional mutation(s) which are themselves being listened for, those notifications will also be delivered
     * prior to the returning to the original caller. Therefore, care must be taken to avoid change notification dependency
     * loops when listeners can themselves mutate fields, to avoid infinite loops.
     *
     * <p>
     * If a non-null {@code types} is provided, then only objects whose types have one of the specified storage ID's
     * will trigger notifications to {@code listener}.
     *
     * <p>
     * A referring object may refer to the changed object through more than one actual path of references matching {@code path};
     * if so, it will still appear only once in the {@link NavigableSet} provided to the listener (this is of course required
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
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addSimpleFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      SimpleFieldChangeListener listener) {
        this.validateChangeListener(SimpleField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, true).add(new FieldMonitor(storageId, path, types, listener));
    }

    /**
     * Monitor for changes within this transaction to the specified {@link SetField} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link SetField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addSetFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      SetFieldChangeListener listener) {
        this.validateChangeListener(SetField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, true).add(new FieldMonitor(storageId, path, types, listener));
    }

    /**
     * Monitor for changes within this transaction to the specified {@link ListField} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link ListField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addListFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      ListFieldChangeListener listener) {
        this.validateChangeListener(ListField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, true).add(new FieldMonitor(storageId, path, types, listener));
    }

    /**
     * Monitor for changes within this transaction to the specified {@link MapField} as seen through a path of references.
     *
     * <p>
     * See {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()} for details on how notifications are delivered.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link MapField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void addMapFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      MapFieldChangeListener listener) {
        this.validateChangeListener(MapField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, true).add(new FieldMonitor(storageId, path, types, listener));
    }

    /**
     * Remove a monitor previously added via {@link #addSimpleFieldChangeListener addSimpleFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeSimpleFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      SimpleFieldChangeListener listener) {
        this.validateChangeListener(SimpleField.class, storageId, path, listener);
        final Set<FieldMonitor> monitors = this.getMonitorsForField(storageId);
        if (monitors != null)
            monitors.remove(new FieldMonitor(storageId, path, types, listener));
    }

    /**
     * Remove a monitor previously added via {@link #addSetFieldChangeListener addSetFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link SetField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeSetFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      SetFieldChangeListener listener) {
        this.validateChangeListener(SetField.class, storageId, path, listener);
        final Set<FieldMonitor> monitors = this.getMonitorsForField(storageId);
        if (monitors != null)
            monitors.remove(new FieldMonitor(storageId, path, types, listener));
    }

    /**
     * Remove a monitor previously added via {@link #addListFieldChangeListener addListFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link ListField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeListFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      ListFieldChangeListener listener) {
        this.validateChangeListener(ListField.class, storageId, path, listener);
        final Set<FieldMonitor> monitors = this.getMonitorsForField(storageId);
        if (monitors != null)
            monitors.remove(new FieldMonitor(storageId, path, types, listener));
    }

    /**
     * Remove a monitor previously added via {@link #addMapFieldChangeListener addMapFieldChangeListener()}.
     *
     * @param storageId storage ID of the field to no longer monitor
     * @param path path of reference fields (represented by storage IDs) through which to monitor field
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener callback for notifications on changes in value
     * @throws UnknownFieldException if no {@link MapField} corresponding to {@code storageId} exists
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code storageId} refers to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code path} or {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws UnsupportedOperationException if {@link #setListeners setListeners()} has been invoked on this instance
     */
    public synchronized void removeMapFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      MapFieldChangeListener listener) {
        this.validateChangeListener(MapField.class, storageId, path, listener);
        final Set<FieldMonitor> monitors = this.getMonitorsForField(storageId);
        if (monitors != null)
            monitors.remove(new FieldMonitor(storageId, path, types, listener));
    }

    private <T extends Field<?>> void validateChangeListener(Class<T> expectedFieldType,
      int storageId, int[] path, Object listener) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(listener != null, "null listener");
        if (this.listenerSetInstalled)
            throw new UnsupportedOperationException("ListenerSet installed");

        // Get target field info
        final FieldStorageInfo fieldInfo = this.schemas.verifyStorageInfo(storageId, SchemaItem.infoTypeFor(expectedFieldType));

        // Get object parent of target field, and make sure the field is not a sub-field
        if (fieldInfo.isSubField()) {
            throw new IllegalArgumentException("the field with storage ID "
              + storageId + " is a sub-field of a complex field; listeners can only be registered on regular object fields");
        }

        // Verify all fields in the path are reference fields
        for (int pathStorageId : path)
            this.schemas.verifyStorageInfo(pathStorageId, ReferenceFieldStorageInfo.class);
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
    void addFieldChangeNotification(FieldChangeNotifier notifier) {
        assert Thread.holdsLock(this);
        assert !this.disableListenerNotifications;

        // Get info
        final ObjId id = notifier.getId();
        final int fieldStorageId = notifier.getStorageId();

        // Does anybody care?
        final Set<FieldMonitor> monitors = this.getMonitorsForField(fieldStorageId, false);
        if (monitors == null || !monitors.stream().anyMatch(new MonitoredPredicate(id, fieldStorageId)))
            return;

        // Add a pending field monitor notification for the specified field
        this.pendingNotifications.get().computeIfAbsent(fieldStorageId, i -> new ArrayList<>(2)).add(notifier);
    }

    /**
     * Determine if there are any monitors watching the specified field in the specified object.
     */
    boolean hasFieldMonitor(ObjId id, int fieldStorageId) {
        assert Thread.holdsLock(this);
        final Set<FieldMonitor> monitors = this.getMonitorsForField(fieldStorageId, false);
        return monitors != null && monitors.stream().anyMatch(new MonitoredPredicate(id, fieldStorageId));
    }

    /**
     * Determine if there are any monitors watching any field in the specified type.
     */
    boolean hasFieldMonitor(ObjType objType) {
        assert Thread.holdsLock(this);
        if (this.monitorMap == null)
            return false;
        final ObjId minId = ObjId.getMin(objType.storageId);
        for (int storageId : NavigableSets.intersection(objType.fields.navigableKeySet(), this.monitorMap.navigableKeySet())) {
            if (this.monitorMap.get(storageId).stream().anyMatch(new MonitoredPredicate(minId, storageId)))
                return true;
        }
        return false;
    }

    /**
     * Verify the given object exists before proceeding with the given mutation via {@link #mutateAndNotify(Mutation}}.
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
                final TreeMap<Integer, ArrayList<FieldChangeNotifier>> pendingNotificationMap = this.pendingNotifications.get();
                while (!pendingNotificationMap.isEmpty()) {

                    // Get the next field with pending notifications
                    final Map.Entry<Integer, ArrayList<FieldChangeNotifier>> entry = pendingNotificationMap.pollFirstEntry();
                    final int storageId = entry.getKey();

                    // For all pending notifications, back-track references and notify all field monitors for the field
                    for (FieldChangeNotifier notifier : entry.getValue()) {
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
    private void notifyFieldMonitors(FieldChangeNotifier notifier,
      NavigableSet<ObjId> objects, ArrayList<FieldMonitor> monitorList, int step) {

        // Find the monitors for whom we have completed all the steps in their (inverse) path,
        // and group the remaining monitors by their next inverted reference path step.
        final HashMap<Integer, ArrayList<FieldMonitor>> remainingMonitorsMap = new HashMap<>();
        for (FieldMonitor monitor : monitorList) {

            // On the first step, apply the monitor's type filter, if any
            if (step == 0 && monitor.types != null && !monitor.types.contains(notifier.getId().getBytes()))
                continue;

            // Issue notification callback if we have back-tracked through the whole path
            if (monitor.path.length == step) {
                notifier.notify(this, monitor.listener, monitor.path, objects);
                continue;
            }

            // Sort the unfinished monitors by their next (i.e., previous) reference field
            final int storageId = monitor.path[monitor.path.length - step - 1];
            remainingMonitorsMap.computeIfAbsent(storageId, i -> new ArrayList<>()).add(monitor);
        }

        // Invert references for each group of remaining monitors and recurse
        for (Map.Entry<Integer, ArrayList<FieldMonitor>> entry : remainingMonitorsMap.entrySet()) {
            final int storageId = entry.getKey();

            // Gather all objects that refer to any object in our current "objects" set
            final ArrayList<NavigableSet<ObjId>> refsList = new ArrayList<>();
            for (ObjId object : objects) {
                final NavigableSet<ObjId> refs = this.queryReferences(storageId).get(object);
                if (refs != null)
                    refsList.add(refs);
            }

            // Recurse on the union of those objects
            if (!refsList.isEmpty())
                this.notifyFieldMonitors(notifier, NavigableSets.union(refsList), entry.getValue(), step + 1);
        }
    }

// Reference Path Queries

    /**
     * Find all objects that refer to any object in the given target set through the specified path of references.
     *
     * @param path path of one or more reference fields (represented by storage IDs) through which to reach the target objects
     * @param targetObjects target objects
     * @return set of objects that refer to the {@code targetObjects} via {@code path}
     * @throws UnknownFieldException if {@code path} contains a storage ID that does not correspond to a {@link ReferenceField}
     * @throws IllegalArgumentException if {@code targetObjects} or {@code path} is null
     * @throws IllegalArgumentException if {@code path} is empty
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public NavigableSet<ObjId> invertReferencePath(int[] path, Iterable<ObjId> targetObjects) {

        // Sanity check
        Preconditions.checkArgument(targetObjects != null, "null targetObjects");
        Preconditions.checkArgument(path != null && path.length > 0, "null/empty path");

        // Verify all fields in the path are reference fields
        for (int storageId : path)
            this.schemas.verifyStorageInfo(storageId, ReferenceFieldStorageInfo.class);

        // Invert references in reverse order
        NavigableSet<ObjId> result = null;
        for (int i = path.length - 1; i >= 0; i--) {
            final int storageId = path[i];

            // Gather all objects that refer to any object in our current target objects set
            final ArrayList<NavigableSet<ObjId>> refsList = new ArrayList<>();
            for (ObjId id : targetObjects) {
                final NavigableSet<ObjId> refs = this.queryReferences(storageId).get(id);
                if (refs != null)
                    refsList.add(refs);
            }
            if (refsList.isEmpty())
                return NavigableSets.empty(FieldTypeRegistry.OBJ_ID);

            // Recurse on the union of those objects
            targetObjects = result = NavigableSets.union(refsList);
        }

        // Done
        return result;
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
        final SimpleFieldStorageInfo<?> fieldInfo = this.schemas.verifyStorageInfo(storageId, SimpleFieldStorageInfo.class);
        if (fieldInfo.superFieldStorageId == 0)
            return fieldInfo.getSimpleFieldIndex(this);
        final ComplexFieldStorageInfo<?> superFieldInfo
          = this.schemas.verifyStorageInfo(fieldInfo.superFieldStorageId, ComplexFieldStorageInfo.class);
        return superFieldInfo.getSimpleSubFieldIndex(this, fieldInfo);
    }

    /**
     * Find all values stored as an element in the specified {@link ListField} and, for each such value,
     * the set of all objects having that value as an element in the list and the corresponding list index.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the list element field is indexed;
     * this method does not check whether any such schema versions exist.
     *
     * @param storageId {@link ListField}'s storage ID
     * @return read-only, real-time view of list element values, objects with the value in the list, and corresponding indicies
     * @throws UnknownFieldException if no {@link ListField} corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex2<?, ObjId, Integer> queryListElementIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final ListFieldStorageInfo<?> fieldInfo = this.schemas.verifyStorageInfo(storageId, ListFieldStorageInfo.class);
        return fieldInfo.getElementFieldIndex(this);
    }

    /**
     * Find all values stored as a value in the specified {@link MapField} and, for each such value,
     * the set of all objects having that value as a value in the map and the corresponding key.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the map value field is indexed;
     * this method does not check whether any such schema versions exist.
     *
     * @param storageId {@link MapField}'s storage ID
     * @return read-only, real-time view of map values, objects with the value in the map, and corresponding keys
     * @throws UnknownFieldException if no {@link MapField} corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex2<?, ObjId, ?> queryMapValueIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final MapFieldStorageInfo<?, ?> fieldInfo = this.schemas.verifyStorageInfo(storageId, MapFieldStorageInfo.class);
        return fieldInfo.getValueFieldIndex(this);
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
              + " is on " + indexInfo.fields.size() + " != 2 fields");
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
              + " is on " + indexInfo.fields.size() + " != 3 fields");
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
              + " is on " + indexInfo.fields.size() + " != 4 fields");
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

    // Query an index on a reference field for referring objects
    @SuppressWarnings("unchecked")
    private NavigableMap<ObjId, NavigableSet<ObjId>> queryReferences(int storageId) {
        assert this.schemas.verifyStorageInfo(storageId, ReferenceFieldStorageInfo.class) != null;
        return (NavigableMap<ObjId, NavigableSet<ObjId>>)this.queryIndex(storageId).asMap();
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
     * @param fieldStorageId reference field storage ID, or -1 to match any reference field
     */
    private NavigableSet<ObjId> findReferrers(ObjId target, DeleteAction onDelete, int fieldStorageId) {
        final ArrayList<NavigableSet<ObjId>> refSets = new ArrayList<>();
        for (Map.Entry<Integer, NavigableSet<ObjId>> entry : this.queryVersion().asMap().entrySet()) {

            // Get corresponding Schema object
            final Schema schemaVersion = this.schemas.versions.get(entry.getKey());
            if (schemaVersion == null)
                throw new InconsistentDatabaseException("encountered objects with unknown schema version " + entry.getKey());

            // Find all reference fields with storage ID matching fieldStorageId (if not -1) and check them. Do this separately
            // for each such field in each object type and version, because the fields may have different DeleteAction's.
            for (ObjType objType : schemaVersion.objTypeMap.values()) {
                for (ReferenceField field : objType.referenceFieldsAndSubFields.values()) {

                    // Check delete action and field
                    if (field.onDelete != onDelete || (fieldStorageId != -1 && field.storageId != fieldStorageId))
                        continue;

                    // Query index on this field, restricting to those only references coming from objType objects
                    final NavigableSet<ObjId> refs = this.queryIndex(field.storageId)
                      .filter(1, new KeyRanges(ObjId.getKeyRange(objType.storageId))).asMap().get(target);
                    if (refs == null)
                        continue;

                    // Restrict further to the specific schema version
                    refSets.add(NavigableSets.intersection(entry.getValue(), refs));
                }
            }
        }
        return !refSets.isEmpty() ? NavigableSets.union(refSets) : NavigableSets.empty(FieldTypeRegistry.OBJ_ID);
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

        // Verify listeners are compatible with this transaction
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

    private abstract static class SimpleFieldChangeNotifier implements FieldChangeNotifier {

        final int storageId;
        final ObjId id;

        SimpleFieldChangeNotifier(SimpleField<?> field, ObjId id) {
            this.storageId = field.storageId;
            this.id = id;
        }

        @Override
        public int getStorageId() {
            return this.storageId;
        }

        @Override
        public ObjId getId() {
            return this.id;
        }

        @Override
        public void notify(Transaction tx, Object listener, int[] path, NavigableSet<ObjId> referrers) {
            this.notify(tx, (SimpleFieldChangeListener)listener, path, referrers);
        }

        abstract void notify(Transaction tx, SimpleFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers);
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
            this.schema = tx.schema;
        }
    }

// MonitoredPredicate

    // Matches FieldMonitors who monitor the specified field in the specified object type
    private static final class MonitoredPredicate implements Predicate<FieldMonitor> {

        private final byte[] idBytes;
        private final int storageId;

        MonitoredPredicate(ObjId id, int storageId) {
            assert id != null;
            this.idBytes = id.getBytes();
            this.storageId = storageId;
        }

        @Override
        public boolean test(FieldMonitor monitor) {
            assert monitor != null;
            return monitor.storageId == this.storageId && (monitor.types == null || monitor.types.contains(this.idBytes));
        }
    }
}

