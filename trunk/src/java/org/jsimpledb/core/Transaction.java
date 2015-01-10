
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jsimpledb.kv.CountingKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.KeyRanges;
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
 * </p>
 *
 * <p>
 * Methods in this class can be divided into the following categories:
 * </p>
 *
 * <p>
 * <b>Transaction Meta-Data</b>
 * <ul>
 *  <li>{@link #getDatabase getDatabase()} - Get the associated {@link Database}</li>
 *  <li>{@link #getSchema getSchema()} - Get the database {@link Schema}, as seen by this transaction</li>
 *  <li>{@link #getSchemaVersion() getSchemaVersion()} - Get the {@link SchemaVersion} that will be used by this transaction</li>
 *  <li>{@link #deleteSchemaVersion deleteSchemaVersion()} - Delete a schema version that is no longer being used</li>
 * </ul>
 * </p>
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
 * </ul>
 * </p>
 *
 * <p>
 * <b>Object Lifecycle</b>
 * <ul>
 *  <li>{@link #create(int) create()} - Create a database object</li>
 *  <li>{@link #delete delete()} - Delete a database object</li>
 *  <li>{@link #exists exists()} - Test whether a database object exists</li>
 *  <li>{@link #copy copy()} - Copy an object's fields onto another object in a (possibly) different transaction</li>
 *  <li>{@link #addCreateListener addCreateListener()} - Register a {@link CreateListener} for notifications about new objects</li>
 *  <li>{@link #removeCreateListener removeCreateListener()} - Unregister a {@link CreateListener}</li>
 *  <li>{@link #addDeleteListener addDeleteListener()} - Register a {@link DeleteListener} for notifications
 *      about object deletions</li>
 *  <li>{@link #removeDeleteListener removeDeleteListener()} - Unregister a {@link DeleteListener}</li>
 * </ul>
 * </p>
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
 * </p>
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
 * </p>
 *
 * <p>
 * <b>Reference Inversion</b>
 * <ul>
 *  <li>{@link #invertReferencePath invertReferencePath()} - Find all objects that refer to any element in a given set
 *      of objects through a specified reference path</li>
 *  </li>
 * </ul>
 * </p>
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
 * </ul>
 * </p>
 *
 * <p>
 * All methods returning a set of values return a {@link NavigableSet}.
 * The {@link NavigableSets} utility class provides methods for the efficient {@link NavigableSets#intersection intersection},
 * {@link NavigableSets#union union}, {@link NavigableSets#difference difference}, and
 * {@link NavigableSets#symmetricDifference symmetric difference} of {@link NavigableSet}s containing the same element type,
 * thereby providing the equivalent of traditional database joins.
 * </p>
 */
public class Transaction {

    private static final int MAX_GENERATED_KEY_ATTEMPTS = 1000;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    final Database db;
    final KVTransaction kvt;
    final Schema schema;
    final SchemaVersion version;

    boolean stale;
    boolean readOnly;
    boolean rollbackOnly;

    private final ThreadLocal<TreeMap<Integer, ArrayList<FieldChangeNotifier>>> pendingNotifications = new ThreadLocal<>();
    private final HashSet<VersionChangeListener> versionChangeListeners = new HashSet<>();
    private final HashSet<CreateListener> createListeners = new HashSet<>();
    private final HashSet<DeleteListener> deleteListeners = new HashSet<>();
    private final TreeMap<Integer, HashSet<FieldMonitor>> monitorMap = new TreeMap<>();
    private final LinkedHashSet<Callback> callbacks = new LinkedHashSet<>();

    Transaction(Database db, KVTransaction kvt, Schema schema, int versionNumber) {
        this(db, kvt, schema, schema.getVersion(versionNumber));
    }

    Transaction(Database db, KVTransaction kvt, Schema schema, SchemaVersion version) {
        this.db = db;
        this.kvt = kvt;
        this.schema = schema;
        this.version = version;
    }

// Transaction Meta-Data

    /**
     * Get the database with which this transaction is associated.
     */
    public Database getDatabase() {
        return this.db;
    }

    /**
     * Get the database schema versions known to this transaction.
     * This reflects all schema versions currently recorded in the database.
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Get the database schema version associated with this transaction.
     * This is default schema version used and the target version when updating objects' schema versions.
     */
    public SchemaVersion getSchemaVersion() {
        return this.version;
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
        if (version <= 0)
            throw new IllegalArgumentException("invalid schema version " + version);
        if (version == this.version.getVersionNumber())
            throw new InvalidSchemaException("version " + version + " is this transaction's version");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.queryVersion().asMap().containsKey(version))
            throw new InvalidSchemaException("one or more version " + version + " objects still exist in database");

        // Delete schema version
        if (!this.schema.deleteVersion(version))
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
     * </p>
     */
    public KVTransaction getKVTransaction() {
        return this.kvt;
    }

    private CountingKVStore getCountingKVTransaction() {
        try {
            return (CountingKVStore)this.kvt;
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("the underlying key/value transaction does not support counters");
        }
    }

// Transaction Lifecycle

    /**
     * Commit this transaction.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws RetryTransactionException from {@link KVTransaction#commit KVTransaction.commit()}
     * @throws RollbackOnlyTransactionException if this instance has been {@linkplain #setRollbackOnly marked} rollback only;
     *  this instance will be automatically rolled back
     */
    public synchronized void commit() {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);

        // Rollback only?
        if (this.rollbackOnly) {
            this.log.info("commit() invoked on transaction " + this + " marked rollback-only, rolling back");
            this.rollback();
            throw new RollbackOnlyTransactionException(this);
        }
        this.stale = true;

        // Do before completion callbacks
        this.log.debug("commit() invoked on" + (this.readOnly ? " read-only" : "") + " transaction " + this);
        Callback failedCallback = null;
        try {
            for (Callback callback : this.callbacks) {
                failedCallback = callback;
                callback.beforeCommit(this.readOnly);
            }
            failedCallback = null;
        } finally {
            if (failedCallback != null) {
                this.log.warn("error invoking beforeCommit() method on transaction "
                  + this + " callback " + failedCallback + ", rolling back");
            }
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
        failedCallback = null;
        try {
            this.kvt.commit();
            for (Callback callback : this.callbacks) {
                failedCallback = callback;
                callback.afterCommit();
            }
            failedCallback = null;
        } finally {
            if (failedCallback != null)
                this.log.warn("error invoking afterCommit() method on transaction " + this + " callback " + failedCallback);
            this.triggerAfterCompletion(true);
        }
    }

    /**
     * Roll back this transaction.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void rollback() {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        this.log.debug("rollback() invoked on" + (this.readOnly ? " read-only" : "") + " transaction " + this);

        // Do before completion callbacks
        this.triggerBeforeCompletion();

        // Roll back KVTransaction and trigger after completion callbacks
        try {
            this.kvt.rollback();
        } finally {
            this.triggerAfterCompletion(false);
        }
    }

    private /*synchronized*/ void triggerBeforeCompletion() {
        for (Callback callback : this.callbacks) {
            try {
                callback.beforeCompletion();
            } catch (Throwable t) {
                this.log.error("error invoking beforeCompletion() method on transaction callback " + callback, t);
            }
        }
    }

    private /*synchronized*/ void triggerAfterCompletion(boolean committed) {
        for (Callback callback : this.callbacks) {
            try {
                callback.afterCompletion(committed);
            } catch (Throwable t) {
                this.log.error("error invoking afterCompletion() method on transaction callback " + callback, t);
            }
        }
    }

    /**
     * Determine whether this transaction is still valid. If not, all other methods in this class
     * will throw {@link StaleTransactionException}.
     */
    public synchronized boolean isValid() {
        return !this.stale;
    }

    /**
     * Determine whether this transaction is read-only.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean isReadOnly() {
        if (this.stale)
            throw new StaleTransactionException(this);
        return this.readOnly;
    }

    /**
     * Enable or disaable read-only mode. When in read-only mode, all mutating operations will fail with
     * a {@link ReadOnlyTransactionException}.
     *
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
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean isRollbackOnly() {
        if (this.stale)
            throw new StaleTransactionException(this);
        return this.rollbackOnly;
    }

    /**
     * Mark this transaction for rollback only. A subsequent attempt to {@link #commit} will throw an exception.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void setRollbackOnly() {
        if (this.stale)
            throw new StaleTransactionException(this);
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
    public void setTimeout(long timeout) {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.kvt.setTimeout(timeout);
    }

    /**
     * Register a transaction {@link Callback} to be invoked when this transaction completes.
     * Callbacks will be invoked in the order they are registered, but <i>duplicate registrations are ignored</i>.
     *
     * @throws IllegalArgumentException if {@code callback} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void addCallback(Callback callback) {
        if (callback == null)
            throw new IllegalArgumentException("nullcallback ");
        if (this.stale)
            throw new StaleTransactionException(this);
        this.callbacks.add(callback);
    }

    /**
     * Create an empty, in-memory transaction initialized with the same schema history as this transaction.
     * The result can be used as a destination for "snapshot" copies of objects made via {@link #copy copy()}.
     *
     * <p>
     * The returned {@link SnapshotTransaction} does not support {@link #commit}, {@link #rollback}, {@link #setRollbackOnly},
     * or {@link #addCallback addCallback()}. However, it can be used indefinitely after this transaction closes.
     * </p>
     *
     * @return uncloseable, empty, in-memory transaction with compatible schema information
     */
    public SnapshotTransaction createSnapshotTransaction() {
        return new SnapshotTransaction(this);
    }

// Object Lifecycle

    /**
     * Create a new object with the given object ID, if it doesn't already exist. If it does exist, nothing happens.
     *
     * <p>
     * If the object doesn't already exist, all fields are set to their default values and the object's
     * schema version is set to {@linkplain #getSchemaVersion the version associated with this transaction}.
     * </p>
     *
     * @param id object ID
     * @return true if the object did not exist and was created, false if the object already existed
     * @throws UnknownTypeException if {@code id} does not correspond to a known object type in this transaction's schema version
     * @throws IllegalArgumentException if {@code id} is null
     * @throws ReadOnlyTransactionException if this transaction has been {@linkplain #setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public boolean create(ObjId id) {
        return this.create(id, this.version.versionNumber);
    }

    /**
     * Create a new object with the given object ID, if it doesn't already exist. If it does exist, nothing happens.
     *
     * <p>
     * If the object doesn't already exist, all fields are set to their default values and the object's
     * schema version is set to the specified version.
     * </p>
     *
     * @param id object ID
     * @param versionNumber schema version number to use for newly created object
     * @return true if the object did not exist and was created, false if the object already existed
     * @throws UnknownTypeException if {@code id} does not correspond to a known object type in the specified schema version
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code versionNumber} is invalid or unknown
     * @throws ReadOnlyTransactionException if this transaction has been {@linkplain #setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean create(ObjId id, int versionNumber) {

        // Sanity check
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Does object already exist?
        if (this.exists(id))
            return false;

        // Initialize object
        this.initialize(id, this.schema.getVersion(versionNumber).getObjType(id.getStorageId()));

        // Done
        return true;
    }

    /**
     * Create a new object with a randomly assigned object ID and having the given type.
     *
     * <p>
     * All fields will be set to their default values.
     * The object's schema version will be set to {@linkplain #getSchemaVersion the version associated with this transaction}.
     * </p>
     *
     * @param storageId object type storage ID
     * @return object id of newly created object
     * @throws UnknownTypeException if {@code storageId} does not correspond to a known object type in this transaction's schema
     * @throws ReadOnlyTransactionException if this transaction has been {@linkplain #setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public ObjId create(int storageId) {
        return this.create(storageId, this.version.versionNumber);
    }

    /**
     * Create a new object with a randomly assigned object ID and having the given type and schema version.
     *
     * <p>
     * All fields will be set to their default values.
     * The object's schema version will be set to the specified version.
     * </p>
     *
     * @param storageId object type storage ID
     * @param versionNumber schema version number to use for newly created object
     * @return object id of newly created object
     * @throws UnknownTypeException if {@code storageId} does not correspond to a known object type in the specified schema version
     * @throws IllegalArgumentException if {@code versionNumber} is invalid or unknown
     * @throws ReadOnlyTransactionException if this transaction has been {@linkplain #setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized ObjId create(int storageId, int versionNumber) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        final ObjType objType = this.schema.getVersion(versionNumber).getObjType(storageId);

        // Generate object ID
        final ObjId id = this.generateIdValidated(objType.storageId);

        // Initialize object
        this.initialize(id, objType);

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
        final ObjTypeStorageInfo info = this.schema.verifyStorageInfo(storageId, ObjTypeStorageInfo.class);

        // Generate ID
        return this.generateIdValidated(info.storageId);
    }

    private /*synchronized*/ ObjId generateIdValidated(int storageId) {

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

    private synchronized void initialize(ObjId id, ObjType objType) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.readOnly)
            throw new ReadOnlyTransactionException(this);

        // Write object meta-data
        ObjInfo.write(this, id, objType.version.versionNumber, false);

        // Write object version index entry
        this.kvt.put(Database.buildVersionIndexKey(id, objType.version.versionNumber), ByteUtil.EMPTY);

        // Initialize counters to zero
        if (!objType.counterFields.isEmpty()) {
            final CountingKVStore ckv = this.getCountingKVTransaction();
            for (CounterField field : objType.counterFields.values())
                ckv.put(field.buildKey(id), ckv.encodeCounter(0));
        }

        // Write simple field index entries
        for (SimpleField<?> field : objType.simpleFields.values()) {
            if (field.indexed)
                this.kvt.put(Transaction.buildSimpleIndexEntry(field, id, null), ByteUtil.EMPTY);
        }

        // Write composite index entries
        for (CompositeIndex index : objType.compositeIndexes.values())
            this.kvt.put(Transaction.buildDefaultCompositeIndexEntry(id, index), ByteUtil.EMPTY);

        // Notify listeners
        for (CreateListener listener : this.createListeners.toArray(new CreateListener[this.createListeners.size()]))
            listener.onCreate(this, id);
    }

    /**
     * Delete an object. Does nothing if object does not exist (e.g., has already been deleted).
     *
     * <p>
     * This method does <i>not</i> change the object's schema version if it has a different version that this transaction.
     * </p>
     *
     * @param id object ID of the object to delete
     * @return true if object was found and deleted, false if object was not found,
     *  or if {@code id} specifies an unknown object type
     * @throws ReferencedObjectException if the object is referenced by some other object
     *  through a reference field configured for {@link DeleteAction#EXCEPTION}
     * @throws IllegalArgumentException if {@code id} is null
     * @throws ReadOnlyTransactionException if this transaction has been {@linkplain #setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized boolean delete(ObjId id) {

        // Sanity check
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.readOnly)
            throw new ReadOnlyTransactionException(this);

        // Get object info
        try {
            this.getObjectInfo(id, false);
        } catch (DeletedObjectException | UnknownTypeException e) {
            return false;
        }

        // Handle recurive DeleteAction.DELETE without hogging Java stack
        final ArrayList<ObjId> deletables = new ArrayList<>(1);
        deletables.add(id);
        boolean found = false;
        while (true) {
            final int size = deletables.size();
            if (size == 0)
                break;
            id = deletables.remove(size - 1);
            found |= this.doDelete(id, deletables);
        }

        // Done
        return found;
    }

    private synchronized boolean doDelete(final ObjId id, List<ObjId> deletables) {

        // Loop here to handle any mutations within delete notification listener callbacks
        ObjInfo info;
        while (true) {

            // Get object info
            try {
                info = this.getObjectInfo(id, false);
            } catch (DeletedObjectException e) {                    // possibly due to a cycle of DeleteAction.DELETE references
                return false;
            } catch (UnknownTypeException e) {
                throw new InconsistentDatabaseException("encountered reference with unknown type during delete cascade: " + id, e);
            }

            // Determine if any EXCEPTION reference fields refer to the object (from some other object); if so, throw exception
            for (ReferenceFieldStorageInfo fieldInfo :
              Iterables.filter(this.schema.storageInfos.values(), ReferenceFieldStorageInfo.class)) {
                for (ObjId referrer : this.findReferrers(id, DeleteAction.EXCEPTION, fieldInfo.storageId)) {
                    if (!referrer.equals(id))
                        throw new ReferencedObjectException(id, referrer, fieldInfo.storageId);
                }
            }

            // Do we need to issue delete notifications for the object being deleted?
            if (info.isDeleteNotified() || this.deleteListeners.isEmpty())
                break;

            // Issue delete notifications and retry
            ObjInfo.write(this, id, info.getVersionNumber(), true);
            for (DeleteListener listener : this.deleteListeners.toArray(new DeleteListener[this.deleteListeners.size()]))
                listener.onDelete(this, id);
        }

        // Actually delete the object
        this.deleteObjectData(info);

        // Find all UNREFERENCE references and unreference them
        for (ReferenceFieldStorageInfo fieldInfo :
          Iterables.filter(this.schema.storageInfos.values(), ReferenceFieldStorageInfo.class)) {
            final NavigableSet<ObjId> referrers = this.findReferrers(id, DeleteAction.UNREFERENCE, fieldInfo.storageId);
            if (fieldInfo.isSubField()) {
                final ComplexFieldStorageInfo<?> superFieldInfo = this.schema.verifyStorageInfo(
                  fieldInfo.superFieldStorageId, ComplexFieldStorageInfo.class);
                superFieldInfo.unreferenceAll(this, fieldInfo.storageId, id, referrers);
            } else {
                final int storageId = fieldInfo.storageId;
                for (ObjId referrer : referrers)
                    this.writeSimpleField(referrer, storageId, null, false);
            }
        }

        // Find all DELETE references and mark the containing object for deletion (caller will call us back to actually delete)
        for (ObjId referrer : this.findReferrers(id, DeleteAction.DELETE, -1))
            deletables.add(referrer);

        // Done
        return true;
    }

    // Delete all of an object's data
    private void deleteObjectData(ObjInfo info) {

        // Delete object's simple field index entries
        final ObjId id = info.getId();
        final ObjType type = info.getObjType();
        for (SimpleField<?> field : type.simpleFields.values()) {
            if (field.indexed)
                this.kvt.remove(Transaction.buildSimpleIndexEntry(field, id, this.kvt.get(field.buildKey(id))));
        }

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
        this.kvt.remove(Database.buildVersionIndexKey(id, info.getVersionNumber()));
    }

    /**
     * Determine if an object exists.
     *
     * <p>
     * This method does <i>not</i> change the object's schema version if it exists
     * and has a different version that this transaction.
     * </p>
     *
     * @param id object ID of the object to find
     * @return true if object was found, false if object was not found, or if {@code id} specifies an unknown object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    public synchronized boolean exists(ObjId id) {
        try {
            this.getObjectInfo(id, false);
        } catch (DeletedObjectException | UnknownTypeException e) {
            return false;
        }
        return true;
    }

    /**
     * Copy all of an object's fields onto a target object in a (possibly) different transaction, replacing any previous values.
     *
     * <p>
     * The schema version associated with the {@code source} object must be identical in this transaction and {@code dest}.
     * Only the object's fields are copied; any other objects they reference are not copied. If the {@code target} object
     * does not exist in {@code dest}, it will be created first (and {@link CreateListener}s notified); if {@code target}
     * does exist in {@code dest}, it's schema version will be upgraded if necessary to match {@code source} (and any registered
     * {@link VersionChangeListener}s notified).
     * (Meaningful) changes to {@code target}'s fields generate change listener notifications.
     * </p>
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     * </p>
     *
     * <p>
     * If {@code dest} is this instance, and {@code source} equals {@code target}, no changes are made and false is returned.
     * </p>
     *
     * @param source object ID of the source object in this transaction
     * @param target object ID of the target object in {@code dest}
     * @param dest destination transaction containing {@code target} (possibly same as this transaction)
     * @return false if object already existed in {@code dest}, true if {@code target} did not exist in {@code dest}
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found in this transaction
     * @throws UnknownTypeException if {@code source} or {@code target} specifies an unknown object type
     * @throws IllegalArgumentException if {@code source} and {@code target} specify different object types
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if any {@code source} and {@code target} have different object types
     * @throws ReadOnlyTransactionException if {@code dest} has been {@linkplain #setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws SchemaMismatchException if the schema version associated with {@code source} differs between
     *  this transaction and {@code dest}
     */
    public synchronized boolean copy(ObjId source, final ObjId target, final Transaction dest) {

        // Sanity check
        if (source == null)
            throw new IllegalArgumentException("null source");
        if (target == null)
            throw new IllegalArgumentException("null target");
        if (dest == null)
            throw new IllegalArgumentException("null dest");
        if (this.stale)
            throw new StaleTransactionException(this);

        // Do nothing if nothing to do
        if (this == dest && source.equals(target))
            return false;

        // Get object info
        final ObjInfo srcInfo = this.getObjectInfo(source, false);

        // Do the copy while both transactions are locked
        synchronized (dest) {

            // Sanity check
            if (dest.stale)
                throw new StaleTransactionException(dest);
            if (dest.readOnly)
                throw new ReadOnlyTransactionException(dest);

            // Copy fields
            return dest.mutateAndNotify(new Mutation<Boolean>() {
                @Override
                public Boolean mutate() {
                    return Transaction.doCopyFields(srcInfo, target, Transaction.this, dest);
                }
            });
        }
    }

    // This method assumes both transactions are locked
    private static boolean doCopyFields(ObjInfo srcInfo, ObjId dstId, Transaction srcTx, Transaction dstTx) {

        // Sanity check
        final ObjId srcId = srcInfo.getId();
        if (srcId.equals(dstId) && srcTx == dstTx)
            throw new RuntimeException("internal error");

        // Verify objects have the same type
        if (srcId.getStorageId() != dstId.getStorageId()) {
            throw new IllegalArgumentException("can't copy " + srcId + " to " + dstId
              + " due to non-equal object types (" + srcId.getStorageId() + " != " + dstId.getStorageId() + ")");
        }

        // Find and verify source object's schema version in destination transaction
        final int objectVersion = srcInfo.getVersionNumber();
        final SchemaVersion dstSchema;
        try {
            dstSchema = dstTx.schema.getVersion(objectVersion);
        } catch (IllegalArgumentException e) {
            throw new SchemaMismatchException("destination transaction has no schema version " + objectVersion);
        }
        if (!Arrays.equals(srcTx.version.encodedXML, dstTx.version.encodedXML))
            throw new SchemaMismatchException("destination transaction schema version " + objectVersion + " does not match");

        // Create destination object if it doesn't already exist
        final boolean existed = !dstTx.create(dstId, objectVersion);
        final ObjInfo dstInfo = dstTx.getObjectInfo(dstId, false);
        final boolean needUpgrade = dstInfo.getVersionNumber() != objectVersion;   // can only happen if object already existed

        // Do field-by-field copy if there are change or version listeners, otherwise do fast copy of key/value pairs
        final ObjType type = srcInfo.getObjType();
        if (dstTx.hasFieldMonitor(type) || (needUpgrade && !dstTx.versionChangeListeners.isEmpty())) {

            // Upgrade object first
            if (needUpgrade)
                dstTx.updateVersion(dstInfo, dstSchema);

            // Copy fields
            for (Field<?> field : type.fields.values())
                field.copy(srcId, dstId, srcTx, dstTx);
        } else {

            // Delete pre-existing object's field content, if any
            dstTx.deleteObjectData(dstInfo);

            // Add schema version index entry
            dstTx.kvt.put(Database.buildVersionIndexKey(dstId, objectVersion), ByteUtil.EMPTY);

            // Copy object meta-data and all field content in one key range sweep
            final byte[] srcMinKey = srcId.getBytes();
            final byte[] srcMaxKey = ByteUtil.getKeyAfterPrefix(srcMinKey);
            final ByteWriter dstWriter = new ByteWriter();
            dstWriter.write(dstId.getBytes());
            final int dstMark = dstWriter.mark();
            for (Iterator<KVPair> i = srcTx.kvt.getRange(srcMinKey, srcMaxKey, false); i.hasNext(); ) {
                final KVPair kv = i.next();
                final ByteReader srcReader = new ByteReader(kv.getKey());
                srcReader.skip(srcMinKey.length);
                dstWriter.reset(dstMark);
                dstWriter.write(srcReader);
                dstTx.kvt.put(dstWriter.getBytes(), kv.getValue());
            }

            // Create object's simple field index entries
            for (SimpleField<?> field : type.simpleFields.values()) {
                if (field.indexed) {
                    final byte[] fieldValue = dstTx.kvt.get(field.buildKey(dstId));     // can be null (if field has default value)
                    final byte[] indexKey = Transaction.buildSimpleIndexEntry(field, dstId, fieldValue);
                    dstTx.kvt.put(indexKey, ByteUtil.EMPTY);
                }
            }

            // Create object's composite index entries
            for (CompositeIndex index : type.compositeIndexes.values())
                dstTx.kvt.put(Transaction.buildCompositeIndexEntry(dstTx, dstId, index), ByteUtil.EMPTY);

            // Create object's complex field index entries
            for (ComplexField<?> field : type.complexFields.values()) {
                for (SimpleField<?> subField : field.getSubFields()) {
                    if (subField.indexed)
                        field.addIndexEntries(dstTx, dstId, subField);
                }
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
     */
    public synchronized void addCreateListener(CreateListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.createListeners.add(listener);
    }

    /**
     * Remove an {@link CreateListener} from this transaction.
     *
     * @param listener the listener to remove
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     */
    public synchronized void removeCreateListener(CreateListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.createListeners.remove(listener);
    }

    /**
     * Add a {@link DeleteListener} to this transaction.
     *
     * @param listener the listener to add
     * @throws IllegalArgumentException if {@code listener} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized void addDeleteListener(DeleteListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.deleteListeners.add(listener);
    }

    /**
     * Remove an {@link DeleteListener} from this transaction.
     *
     * @param listener the listener to remove
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     */
    public synchronized void removeDeleteListener(DeleteListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (listener == null)
            throw new IllegalArgumentException("null listener");
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
        if (id == null)
            throw new IllegalArgumentException("null id");

        // Get object version
        return this.getObjectInfo(id, false).getVersionNumber();
    }

    /**
     * Change the schema version of the specified object, if necessary, so that its version matches
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}.
     *
     * <p>
     * If a schema change occurs, any registered {@link VersionChangeListener}s will be notified prior
     * to this method returning.
     * </p>
     *
     * @param id object ID of the object to delete
     * @return true if the object schema version was changed, otherwise false
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws ReadOnlyTransactionException if this transaction has been {@linkplain #setReadOnly set read-only}
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws IllegalArgumentException if {@code id} is null
     * @throws TypeNotInSchemaVersionException if the object version could not be updated because the object's type
     *   does not exist in the schema version associated with this transaction
     * @see #getSchemaVersion
     */
    public synchronized boolean updateSchemaVersion(ObjId id) {

        // Sanity check
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.readOnly)
            throw new ReadOnlyTransactionException(this);

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, false);
        if (info.getVersionNumber() == this.version.versionNumber)
            return false;

        // Update schema version
        this.mutateAndNotify(new Mutation<Void>() {
            @Override
            public Void mutate() {
                Transaction.this.updateVersion(info, Transaction.this.version);
                return null;
            }
        });
        return true;
    }

    /**
     * Update object to the current schema version.
     *
     * @param info original object info
     * @param targetVersion version to change to
     */
    private synchronized void updateVersion(final ObjInfo info, final SchemaVersion targetVersion) {

        // Get version numbers
        final ObjId id = info.getId();
        final int oldVersion = info.getVersionNumber();
        final int newVersion = targetVersion.versionNumber;

        // Sanity check
        if (newVersion == oldVersion)
            throw new IllegalArgumentException("object already at version " + newVersion);
        if (this.readOnly)
            throw new ReadOnlyTransactionException(this);

        // Get old and new types
        final ObjType oldType = info.getObjType();
        final ObjType newType;
        try {
            newType = targetVersion.getObjType(id.getStorageId());
        } catch (UnknownTypeException e) {
            throw (TypeNotInSchemaVersionException)new TypeNotInSchemaVersionException(id, newVersion).initCause(e);
        }

        // Gather removed fields' values here for user migration if any VersionChangeListeners are registered
        final TreeMap<Integer, Object> oldValueMap = !this.versionChangeListeners.isEmpty() ?
          new TreeMap<Integer, Object>() : null;

    //////// Remove the index entries corresponding to removed composite indexes

        // Get composite index storage IDs (old or new)
        final TreeSet<Integer> compositeIndexStorageIds = new TreeSet<>();
        compositeIndexStorageIds.addAll(oldType.compositeIndexes.keySet());
        compositeIndexStorageIds.addAll(newType.compositeIndexes.keySet());

        // Remove index entries for composite indexes that are going away
        for (CompositeIndex index : oldType.compositeIndexes.values()) {
            if (!newType.compositeIndexes.containsKey(index.storageId))
                this.kvt.remove(this.buildCompositeIndexEntry(id, index));
        }

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
                final long value = oldValue != null ? this.getCountingKVTransaction().decodeCounter(oldValue) : 0;
                oldValueMap.put(storageId, value);
            }

            // Remove old value if field has disappeared, otherwise leave alone
            if (newField == null)
                this.kvt.remove(key);
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
                final byte[] bytes = oldValue != null ? oldValue : oldField.fieldType.getDefaultValue();
                final Object value = oldField.fieldType.read(new ByteReader(bytes));
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
        for (CompositeIndex index : newType.compositeIndexes.values()) {
            if (!oldType.compositeIndexes.containsKey(index.storageId))
                this.kvt.put(this.buildCompositeIndexEntry(id, index), ByteUtil.EMPTY);
        }

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
            if (oldField != null && oldValueMap != null)
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

        // Update object version
        ObjInfo.write(this, id, newVersion, info.isDeleteNotified());

        // Update object version index entry
        this.kvt.remove(Database.buildVersionIndexKey(id, oldVersion));
        this.kvt.put(Database.buildVersionIndexKey(id, newVersion), ByteUtil.EMPTY);

    //////// Notify listeners

        // Lock down old field values map
        final NavigableMap<Integer, Object> readOnlyOldValuesMap = oldValueMap != null ?
          Maps.unmodifiableNavigableMap(oldValueMap) : null;

        // Notify about version update
        for (VersionChangeListener listener : this.versionChangeListeners)
            listener.onVersionChange(this, id, oldVersion, newVersion, readOnlyOldValuesMap);
    }

    /**
     * Find storage ID's which are no longer allowed by a reference field when upgrading to the specified
     * schema version and therefore need to be scrubbed during the upgrade.
     */
    private TreeSet<Integer> findRemovedObjectTypes(ObjInfo info, SchemaVersion newVersion, int storageId) {

        // Get old schema version
        final SchemaVersion oldVersion = info.getSchemaVersion();

        // Get old and new object types
        final ObjType oldType = info.getObjType();
        final ObjType newType = newVersion.getObjType(info.getId().getStorageId());

        // Get old and new reference fields having specified storage ID
        final ReferenceField oldField = oldType.referenceFields.get(storageId);
        final ReferenceField newField = newType.referenceFields.get(storageId);
        if (oldField == null || newField == null)
            return null;

        // Check allowed storage IDs
        final SortedSet<Integer> newObjectTypes = newField.getObjectTypes();
        if (newObjectTypes == null)
            return null;                                                // new field can refer to any type in any schema version
        SortedSet<Integer> oldObjectTypes = oldField.getObjectTypes();
        if (oldObjectTypes == null)
            oldObjectTypes = this.schema.objTypeStorageIds;             // old field can refer to any type in any schema version

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
     */
    public synchronized void addVersionChangeListener(VersionChangeListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.versionChangeListeners.add(listener);
    }

    /**
     * Remove an {@link VersionChangeListener} from this transaction.
     *
     * @param listener the listener to remove
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code listener} is null
     */
    public synchronized void removeVersionChangeListener(VersionChangeListener listener) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.versionChangeListeners.remove(listener);
    }

// Object and Field Access

    /**
     * Get all objects in the database.
     *
     * <p>
     * The returned set includes objects from all schema versions. Use {@link #queryVersion} to
     * find objects with a specific schema version.
     * </p>
     *
     * <p>
     * The returned set is mutable, with the exception that {@link NavigableSet#add add()} is not supported.
     * Deleting an element results in {@linkplain #delete deleting} the corresponding object.
     * </p>
     *
     * @return set containing all objects
     * @throws StaleTransactionException if this transaction is no longer usable
     * @see #getAll(int)
     */
    public NavigableSet<ObjId> getAll() {

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
     * </p>
     *
     * @param storageId object type storage ID
     * @return set containing all objects having the specified storage ID
     * @throws UnknownTypeException if {@code storageId} does not correspond to any known object type
     * @throws StaleTransactionException if this transaction is no longer usable
     * @see #getAll()
     */
    public NavigableSet<ObjId> getAll(int storageId) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        this.schema.verifyStorageInfo(storageId, ObjTypeStorageInfo.class);

        // Return objects
        return new ObjTypeSet(this, storageId);
    }

    /**
     * Read the value of a {@link SimpleField} from an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary, prior to
     * reading the field.
     * </p>
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
        if (id == null)
            throw new IllegalArgumentException("null id");

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
        return field.fieldType.read(new ByteReader(value != null ? value : field.fieldType.getDefaultValue()));
    }

    /**
     * Change the value of a {@link SimpleField} in an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary, prior to
     * writing the field.
     * </p>
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
    public synchronized void writeSimpleField(final ObjId id,
      final int storageId, final Object value, final boolean updateVersion) {
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

        // Get new value
        final byte[] key = field.buildKey(id);
        final byte[] newValue = field.encode(newObj);

        // Before setting the new value, read the old value if one of the following is true:
        //  - The field is being monitored -> we need to filter out "changes" that don't actuallly change anything
        //  - The field is indexed -> we need the old value so we can remove the old index entry
        // If neither of the above is true, then there's no need to read the old value.
        byte[] oldValue = null;
        if (field.indexed || field.compositeIndexMap != null || this.hasFieldMonitor(id, field)) {

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
        final Object oldObj = field.fieldType.read(new ByteReader(oldValue != null ? oldValue : field.fieldType.getDefaultValue()));
        this.addFieldChangeNotification(new SimpleFieldChangeNotifier(field, id) {
            @Override
            @SuppressWarnings("unchecked")
            public void notify(Transaction tx, SimpleFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
                listener.onSimpleFieldChange(tx, this.id, (SimpleField<Object>)field, path, referrers, oldObj, newObj);
            }
        });
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
     * Read the value of a {@link CounterField} from an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary, prior to
     * reading the field.
     * </p>
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
        if (id == null)
            throw new IllegalArgumentException("null id");

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
        return value != null ? this.getCountingKVTransaction().decodeCounter(value) : 0;
    }

    /**
     * Set the value of a {@link CounterField} in an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary, prior to
     * writing the field.
     * </p>
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
        this.mutateAndNotify(id, new Mutation<Void>() {
            @Override
            public Void mutate() {
                Transaction.this.doWriteCounterField(id, storageId, value, updateVersion);
                return null;
            }
        });
    }

    private synchronized void doWriteCounterField(ObjId id, int storageId, long value, boolean updateVersion) {

        // Get object info
        final ObjInfo info = this.getObjectInfo(id, updateVersion);

        // Find field
        final CounterField field = info.getObjType().counterFields.get(storageId);
        if (field == null)
            throw new UnknownFieldException(info.getObjType(), storageId, "counter field");

        // Set value
        final byte[] key = field.buildKey(id);
        final CountingKVStore ckv = this.getCountingKVTransaction();
        ckv.put(key, ckv.encodeCounter(value));
    }

    /**
     * Adjust the value of a {@link CounterField} in an object by some amount, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary, prior to
     * writing the field.
     * </p>
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
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.readOnly)
            throw new ReadOnlyTransactionException(this);

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
        this.getCountingKVTransaction().adjustCounter(field.buildKey(id), offset);
    }

    /**
     * Access a {@link SetField} associated with an object, optionally updating the object's schema version.
     *
     * <p>
     * If {@code updateVersion} is true, the schema version of the object will be automatically changed to match
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary.
     * </p>
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link SetField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
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
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary.
     * </p>
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link ListField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
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
     * {@linkplain #getSchemaVersion the schema version associated with this transaction}, if necessary.
     * </p>
     *
     * @param id object ID of the object
     * @param storageId storage ID of the {@link MapField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
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
     * </p>
     *
     * @param id object ID
     * @return the {@link org.jsimpledb.kv.KVDatabase} key corresponding to {@code id}
     * @throws IllegalArgumentException if {@code id} is null
     */
    public byte[] getKey(ObjId id) {
        if (id == null)
            throw new IllegalArgumentException("null id");
        return id.getBytes();
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified field in the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>This method does not check whether {@code id} is valid, the object exists,
     *      or the field actually exists in the object.</li>
     *  <li>Complex fields utilize mutiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link org.jsimpledb.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     * </p>
     *
     * @param id object ID
     * @param storageId field storage ID
     * @return the {@link org.jsimpledb.kv.KVDatabase} key of the field in the specified object
     * @throws UnknownFieldException if no field corresponding to {@code storageId} exists
     * @throws IllegalArgumentException if {@code storageId} corresponds to a sub-field of a complex field
     * @throws IllegalArgumentException if {@code storageId} is less than or equal to zero
     * @throws IllegalArgumentException if {@code id} is null
     */
    public byte[] getKey(ObjId id, int storageId) {

        // Sanity check
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (storageId <= 0)
            throw new IllegalArgumentException("storageId <= 0");
        final FieldStorageInfo info = this.schema.verifyStorageInfo(storageId, FieldStorageInfo.class);
        if (info.isSubField())
            throw new IllegalArgumentException("field is a sub-field of a complex field");

        // Build key
        final ByteWriter writer = new ByteWriter();
        id.writeTo(writer);
        UnsignedIntEncoder.write(writer, storageId);
        return writer.getBytes();
    }

    boolean hasDefaultValue(ObjId id, SimpleField<?> field) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        if (id == null)
            throw new IllegalArgumentException("null id");

        // Get object info to verify object exists
        final ObjInfo info = this.getObjectInfo(id, false);

        // Check whether non-default value stored in field
        return this.kvt.get(field.buildKey(id)) == null;
    }

    private synchronized <F, V> V readComplexField(ObjId id,
      int storageId, boolean updateVersion, Class<F> fieldClass, Class<V> valueType) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        if (id == null)
            throw new IllegalArgumentException("null id");

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

        // Check object type
        this.schema.verifyStorageInfo(id.getStorageId(), ObjTypeStorageInfo.class);

        // Check schema version
        final ObjInfo info = new ObjInfo(this, id);
        if (!update || info.getSchemaVersion() == this.version)
            return info;

        // Update schema version
        this.mutateAndNotify(new Mutation<Void>() {
            @Override
            public Void mutate() {
                Transaction.this.updateVersion(info, Transaction.this.version);
                return null;
            }
        });

        // Get updated object info
        return new ObjInfo(this, id);
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
     * </p>
     *
     * <p>
     * If a non-null {@code types} is provided, then only objects whose types have one of the specified storage ID's
     * will trigger notifications to {@code listener}.
     * </p>
     *
     * <p>
     * A referring object may refer to the changed object through more than one actual path of references matching {@code path};
     * if so, it will still appear only once in the {@link NavigableSet} provided to the listener (this is of course required
     * by set semantics).
     * </p>
     *
     * <p>
     * Although the reference back-tracking algorithm does consolidate multiple paths between the same two objects,
     * be careful to avoid an explosion of notifications when objects in the {@code path} have a high degree of fan-in.
     * </p>
     *
     * <p>
     * When a {@link ReferenceField} in {@code path} also happens to be the field being changed, then there is ambiguity
     * about how the set of referring objects is calculated: does it use the field's value before or after the change?
     * This API guarantees that the answer is "after the change"; however, if another listener on the same field is
     * invoked before {@code listener} and mutates any reference field(s) in {@code path}, then whether that additional
     * change is also be included in the calculation is undefined.
     * </p>
     *
     * <p>
     * Therefore, for consistency, avoid changing any {@link ReferenceField} from within a listener callback when that
     * field is also in some other listener's reference path, and both listeners are watching the same field.
     * </p>
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
     * </p>
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
     * </p>
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
     * </p>
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
     */
    public synchronized void removeSimpleFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      SimpleFieldChangeListener listener) {
        this.validateChangeListener(SimpleField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, false).remove(new FieldMonitor(storageId, path, types, listener));
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
     */
    public synchronized void removeSetFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      SetFieldChangeListener listener) {
        this.validateChangeListener(SetField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, false).remove(new FieldMonitor(storageId, path, types, listener));
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
     */
    public synchronized void removeListFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      ListFieldChangeListener listener) {
        this.validateChangeListener(ListField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, false).remove(new FieldMonitor(storageId, path, types, listener));
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
     */
    public synchronized void removeMapFieldChangeListener(int storageId, int[] path, Iterable<Integer> types,
      MapFieldChangeListener listener) {
        this.validateChangeListener(MapField.class, storageId, path, listener);
        this.getMonitorsForField(storageId, false).remove(new FieldMonitor(storageId, path, types, listener));
    }

    private <T extends Field<?>> void validateChangeListener(Class<T> expectedFieldType,
      int storageId, int[] path, Object listener) {

        // Sanity check
        if (this.stale)
            throw new StaleTransactionException(this);
        if (path == null)
            throw new IllegalArgumentException("null path");
        if (listener == null)
            throw new IllegalArgumentException("null listener");

        // Get target field info
        final FieldStorageInfo fieldInfo = this.schema.verifyStorageInfo(storageId, SchemaItem.infoTypeFor(expectedFieldType));

        // Get object parent of target field, and make sure the field is not a sub-field
        if (fieldInfo.isSubField()) {
            throw new IllegalArgumentException("the field with storage ID "
              + storageId + " is a sub-field of a complex field; listeners can only be registered on regular object fields");
        }

        // Verify all fields in the path are reference fields
        for (int pathStorageId : path)
            this.schema.verifyStorageInfo(pathStorageId, ReferenceFieldStorageInfo.class);
    }

    private synchronized HashSet<FieldMonitor> getMonitorsForField(int storageId, boolean adding) {
        HashSet<FieldMonitor> monitors = this.monitorMap.get(storageId);
        if (monitors == null) {
            monitors = new HashSet<FieldMonitor>();
            if (adding)
                this.monitorMap.put(storageId, monitors);
        }
        return monitors;
    }

    /**
     * Add a pending notification for any {@link FieldMonitor}s watching the specified field in the specified object.
     * This method assumes only the appropriate type of monitor is registered as a listener on the field
     * and that the provided old and new values have the appropriate types.
     */
    void addFieldChangeNotification(FieldChangeNotifier notifier) {

        // Does anybody care?
        final int storageId = notifier.getStorageId();
        HashSet<FieldMonitor> monitors = this.monitorMap.get(storageId);
        if (monitors == null || !Iterables.any(monitors, new MonitoredPredicate(notifier.getId(), storageId)))
            return;

        // Add a pending field monitor notification for the specified field
        final TreeMap<Integer, ArrayList<FieldChangeNotifier>> pendingNotificationMap = this.pendingNotifications.get();
        ArrayList<FieldChangeNotifier> pendingNotificationList = pendingNotificationMap.get(storageId);
        if (pendingNotificationList == null) {
            pendingNotificationList = new ArrayList<FieldChangeNotifier>(2);
            pendingNotificationMap.put(storageId, pendingNotificationList);
        }
        pendingNotificationList.add(notifier);
    }

    /**
     * Determine if there are any monitors watching the specified field.
     */
    boolean hasFieldMonitor(ObjId id, Field<?> field) {
        final HashSet<FieldMonitor> monitors = this.monitorMap.get(field.storageId);
        return monitors != null && Iterables.any(monitors, new MonitoredPredicate(id, field.storageId));
    }

    /**
     * Determine if there are any monitors watching any field in the specified type.
     */
    boolean hasFieldMonitor(ObjType objType) {
        for (int storageId : NavigableSets.intersection(objType.fields.navigableKeySet(), this.monitorMap.navigableKeySet())) {
            if (Iterables.any(this.monitorMap.get(storageId), new MonitoredPredicate(ObjId.getMin(objType.storageId), storageId)))
                return true;
        }
        return false;
    }

    /**
     * Perform some action and, when entirely done (including re-entrant invocation), issue pending notifications to monitors.
     *
     * @param id object containing the mutated field; will be validated
     * @param mutation change to apply
     * @throws ReadOnlyTransactionException if this transaction has been {@linkplain #setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    synchronized <V> V mutateAndNotify(ObjId id, Mutation<V> mutation) {

        // Verify object exists
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.kvt.get(id.getBytes()) == null)
            throw new DeletedObjectException(id);

        // Perform mutation
        return this.mutateAndNotify(mutation);
    }

    private synchronized <V> V mutateAndNotify(Mutation<V> mutation) {

        // Validate transaction
        if (this.stale)
            throw new StaleTransactionException(this);
        if (this.readOnly)
            throw new ReadOnlyTransactionException(this);

        // If re-entrant invocation, we're already set up
        if (this.pendingNotifications.get() != null)
            return mutation.mutate();

        // Set up pending report list, perform mutation, and then issue reports
        this.pendingNotifications.set(new TreeMap<Integer, ArrayList<FieldChangeNotifier>>());
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
                        final ArrayList<FieldMonitor> monitorList = new ArrayList<>(this.getMonitorsForField(storageId, false));
                        if (!monitorList.isEmpty())
                            this.notifyFieldMonitors(notifier, NavigableSets.singleton(notifier.getId()), monitorList, 0);
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
            ArrayList<FieldMonitor> remainingMonitors = remainingMonitorsMap.get(storageId);
            if (remainingMonitors == null) {
                remainingMonitors = new ArrayList<FieldMonitor>();
                remainingMonitorsMap.put(storageId, remainingMonitors);
            }
            remainingMonitors.add(monitor);
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
        if (targetObjects == null)
            throw new IllegalArgumentException("null targetObjects");
        if (path == null)
            throw new IllegalArgumentException("null path");
        if (path.length == 0)
            throw new IllegalArgumentException("empty path");

        // Verify all fields in the path are reference fields
        for (int storageId : path)
            this.schema.verifyStorageInfo(storageId, ReferenceFieldStorageInfo.class);

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
                return NavigableSets.empty();

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
     * </p>
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the field is indexed;
     * this method does not check whether any such schema versions exist.
     * </p>
     *
     * <p>
     * Only objects having schema versions in which the field is indexed will be found;
     * this method does not check whether any such schema versions exist.
     * </p>
     *
     * @param storageId {@link SimpleField}'s storage ID
     * @return read-only, real-time view of field values mapped to sets of objects with the value in the field
     * @throws UnknownFieldException if no {@link SimpleField} corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex<?, ObjId> queryIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final SimpleFieldStorageInfo<?> fieldInfo = this.schema.verifyStorageInfo(storageId, SimpleFieldStorageInfo.class);
        if (fieldInfo.superFieldStorageId == 0)
            return fieldInfo.getSimpleFieldIndex(this);
        final ComplexFieldStorageInfo<?> superFieldInfo
          = this.schema.verifyStorageInfo(fieldInfo.superFieldStorageId, ComplexFieldStorageInfo.class);
        return superFieldInfo.getSimpleSubFieldIndex(this, fieldInfo);
    }

    /**
     * Find all values stored as an element in the specified {@link ListField} and, for each such value,
     * the set of all objects having that value as an element in the list and the corresponding list index.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the list element field is indexed;
     * this method does not check whether any such schema versions exist.
     * </p>
     *
     * @param storageId {@link ListField}'s storage ID
     * @return read-only, real-time view of list element values, objects with the value in the list, and indicies
     * @throws UnknownFieldException if no {@link ListField} corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex2<?, ObjId, Integer> queryListElementIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final ListFieldStorageInfo<?> fieldInfo = this.schema.verifyStorageInfo(storageId, ListFieldStorageInfo.class);
        return fieldInfo.getElementFieldIndex(this);
    }

    /**
     * Find all values stored as a value in the specified {@link MapField} and, for each such value,
     * the set of all objects having that value as a value in the map and the corresponding key.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions for which the map value field is indexed;
     * this method does not check whether any such schema versions exist.
     * </p>
     *
     * @param storageId {@link MapField}'s storage ID
     * @throws UnknownFieldException if no {@link MapField} corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public synchronized CoreIndex2<?, ObjId, ?> queryMapValueIndex(int storageId) {
        if (this.stale)
            throw new StaleTransactionException(this);
        final MapFieldStorageInfo<?, ?> fieldInfo = this.schema.verifyStorageInfo(storageId, MapFieldStorageInfo.class);
        return fieldInfo.getValueFieldIndex(this);
    }

    /**
     * Access a composite index on two fields.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions in which the composite index is defined.
     * </p>
     *
     * @param storageId composite index's storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws UnknownIndexException if {@code storageID} is unknown or does not correspond to a composite index on two fields
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public CoreIndex2<?, ?, ObjId> queryCompositeIndex2(int storageId) {
        final CompositeIndexStorageInfo indexInfo = this.schema.verifyStorageInfo(storageId, CompositeIndexStorageInfo.class);
        final Object index = indexInfo.getIndex(this);
        if (!(index instanceof CoreIndex2)) {
            throw new UnknownIndexException(storageId, "the composite index with storage ID " + storageId
              + " is on " + indexInfo.fields.size() + " != 2 fields");
        }
        return (CoreIndex2<?, ?, ObjId>)index;
    }

    /**
     * Access any composite index by storage ID, regardless of the number of fields indexed.
     *
     * <p>
     * The returned index contains objects from all recorded schema versions in which the composite index is defined.
     * </p>
     *
     * @param storageId composite index's storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws UnknownIndexException if {@code storageID} is unknown or does not correspond to a composite index
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public Object queryCompositeIndex(int storageId) {
        final CompositeIndexStorageInfo indexInfo = this.schema.verifyStorageInfo(storageId, CompositeIndexStorageInfo.class);
        return indexInfo.getIndex(this);
    }

    // Query an index on a reference field for referring objects
    @SuppressWarnings("unchecked")
    private NavigableMap<ObjId, NavigableSet<ObjId>> queryReferences(int storageId) {
        assert this.schema.verifyStorageInfo(storageId, ReferenceFieldStorageInfo.class) != null;
        return (NavigableMap<ObjId, NavigableSet<ObjId>>)this.queryIndex(storageId).asMap();
    }

    /**
     * Find all objects that refer to the given target object through the/any reference field with the specified
     * {@link DeleteAction}.
     *
     * <p>
     * Because different schema versions can have different {@link DeleteAction}'s configured for the
     * same field, we have to iterate through each schema version separately.
     * </p>
     *
     * @param target referred-to object
     * @param onDelete {@link DeleteAction} to match
     * @param fieldStorageId reference field storage ID, or -1 to match any reference field
     */
    private NavigableSet<ObjId> findReferrers(ObjId target, DeleteAction onDelete, int fieldStorageId) {
        final ArrayList<NavigableSet<ObjId>> refSets = new ArrayList<>();
        for (SchemaVersion schemaVersion : this.schema.versions.values()) {

            // Check whether any object of this version exist; if not, skip
            final NavigableSet<ObjId> versionObjects = this.queryVersion().asMap().get(schemaVersion.versionNumber);
            if (versionObjects == null)
                continue;

            // Find all reference fields with storage ID matching fieldStorageId (if not -1) and check them.
            // Do this separately for each such field in each object type because the fields may have different DeleteAction's.
            for (ObjType objType : schemaVersion.objTypeMap.values()) {
                for (ReferenceField field : Iterables.filter(objType.getFieldsAndSubFields(), ReferenceField.class)) {

                    // Check delete action and field
                    if (field.onDelete != onDelete || (fieldStorageId != -1 && field.storageId != fieldStorageId))
                        continue;

                    // Query index on this field, restricting to those only references coming from objType objects
                    final NavigableSet<ObjId> refs = this.queryIndex(field.storageId)
                      .filter(1, new KeyRanges(ObjId.getKeyRange(objType.storageId))).asMap().get(target);
                    if (refs == null)
                        continue;

                    // Restrict further to the specific schema version
                    refSets.add(NavigableSets.intersection(versionObjects, refs));
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

// Mutation

    interface Mutation<V> {
        V mutate();
    }

// SimpleFieldChangeNotifier

    private abstract class SimpleFieldChangeNotifier implements FieldChangeNotifier {

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
     * Callbacks are registered with a transaction via {@link Transaction#addCallback Transaction.addCallback()}.
     *
     * <p>
     * Modeled after Spring's {@link org.springframework.transaction.support.TransactionSynchronization} interface.
     * </p>
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
         * </p>
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
         * </p>
         */
        void beforeCompletion();

        /**
         * Invoked after successful transaction commit (and before {@link #afterCompletion afterCompletion()}).
         *
         * <p>
         * Any exceptions thrown will propagate to the caller.
         * </p>
         */
        void afterCommit();

        /**
         * Invoked after transaction completion (but after any {@link #afterCommit}).
         * This method is invoked in any case, whether the transaction was committed or rolled back.
         * Typically used to clean up resources after transaction completion.
         *
         * <p>
         * Any exceptions thrown will be logged but will <b>not</b> propagate to the caller.
         * </p>
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
     * </p>
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

// Predicates & Functions

    // Matches FieldMonitors who monitor the specified field in the specified object type
    private final class MonitoredPredicate implements Predicate<FieldMonitor> {

        private final ObjId id;
        private final int storageId;

        MonitoredPredicate(ObjId id, int storageId) {
            this.id = id;
            this.storageId = storageId;
        }

        @Override
        public boolean apply(FieldMonitor monitor) {
            return monitor.storageId == this.storageId && (monitor.types == null || monitor.types.contains(this.id.getBytes()));
        }
    }
}

