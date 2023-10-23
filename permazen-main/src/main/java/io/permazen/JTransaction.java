
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import io.permazen.core.CoreIndex;
import io.permazen.core.CoreIndex2;
import io.permazen.core.CoreIndex3;
import io.permazen.core.CoreIndex4;
import io.permazen.core.CounterField;
import io.permazen.core.CreateListener;
import io.permazen.core.DeleteListener;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitchAdapter;
import io.permazen.core.FieldType;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.ReferenceField;
import io.permazen.core.Schema;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.StaleTransactionException;
import io.permazen.core.Transaction;
import io.permazen.core.TypeNotInSchemaVersionException;
import io.permazen.core.UnknownFieldException;
import io.permazen.core.VersionChangeListener;
import io.permazen.core.type.ReferenceFieldType;
import io.permazen.core.util.ObjIdMap;
import io.permazen.core.util.ObjIdSet;
import io.permazen.index.Index;
import io.permazen.index.Index2;
import io.permazen.index.Index3;
import io.permazen.index.Index4;
import io.permazen.kv.KVDatabaseException;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.mvcc.ReadTracking;
import io.permazen.kv.util.AbstractKVNavigableSet;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.tuple.Tuple4;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ConvertedNavigableMap;
import io.permazen.util.ConvertedNavigableSet;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.groups.Default;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.dellroad.stuff.validation.ValidationContext;
import org.dellroad.stuff.validation.ValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transaction associated with a {@link Permazen} instance.
 *
 * <p>
 * Commonly used methods in this class can be divided into the following categories:
 *
 * <p>
 * <b>Transaction Meta-Data</b>
 * <ul>
 *  <li>{@link #getPermazen getPermazen()} - Get the associated {@link Permazen} instance</li>
 *  <li>{@link #getTransaction} - Get the core API {@link Transaction} underlying this instance</li>
 * </ul>
 *
 * <p>
 * <b>Transaction Lifecycle</b>
 * <ul>
 *  <li>{@link #commit commit()} - Commit transaction</li>
 *  <li>{@link #rollback rollback()} - Roll back transaction</li>
 *  <li>{@link #getCurrent getCurrent()} - Get the {@link JTransaction} instance associated with the current thread</li>
 *  <li>{@link #setCurrent setCurrent()} - Set the {@link JTransaction} instance associated with the current thread</li>
 *  <li>{@link #isValid isValid()} - Test transaction validity</li>
 *  <li>{@link #performAction performAction()} - Perform action with this instance as the current transaction</li>
 * </ul>
 *
 * <p>
 * <b>Object Access</b>
 * <ul>
 *  <li>{@link #get(ObjId, Class) get()} - Get the Java model object corresponding to a specific database object ID</li>
 *  <li>{@link #getAll getAll()} - Get all database objects that are instances of a given Java type</li>
 *  <li>{@link #create(Class) create()} - Create a new database object</li>
 * </ul>
 *
 * <p>
 * <b>Copying Objects</b>
 * <ul>
 *  <li>{@link #copyTo(JTransaction, CopyState, Stream) copyTo()}
 *  - Copy a {@link Stream} of objects into another transaction</li>
 *  <li>{@link #copyTo(JTransaction, JObject, CopyState, String[]) copyTo()}
 *  - Copy objects reachable through specified reference path(s) into another transaction</li>
 *  <li>{@link #cascadeFindAll(ObjId, String, int) cascadeFindAll()} - Find all objects reachable through a named cascade</li>
 *  <li>{@link ImportContext} - Import plain (POJO) model objects</li>
 *  <li>{@link ExportContext} - Export plain (POJO) model objects</li>
 * </ul>
 *
 * <p>
 * <b>Validation</b>
 * <ul>
 *  <li>{@link #validate validate()} - Validate objects in the validation queue</li>
 *  <li>{@link #resetValidationQueue} - Clear the validation queue</li>
 * </ul>
 *
 * <p>
 * <b>Index Queries</b>
 * <ul>
 *  <li>{@link #queryIndex(Class, String, Class) queryIndex()}
 *      - Access the index associated with a simple field</li>
 *  <li>{@link #queryListElementIndex queryListElementIndex()}
 *      - Access the composite index associated with a list field that includes corresponding list indices</li>
 *  <li>{@link #queryMapValueIndex queryMapValueIndex()}
 *      - Access the composite index associated with a map value field that includes corresponding map keys</li>
 *  <li>{@link #queryCompositeIndex(Class, String, Class, Class) queryCompositeIndex()}
 *      - Access a composite index defined on two fields</li>
 *  <li>{@link #queryCompositeIndex(Class, String, Class, Class, Class) queryCompositeIndex()}
 *      - Access a composite index defined on three fields</li>
 *  <li>{@link #queryCompositeIndex(Class, String, Class, Class, Class, Class) queryCompositeIndex()}
 *      - Access a composite index defined on four fields</li>
 *  <!-- COMPOSITE-INDEX -->
 *  <li>{@link #queryVersion queryVersion()} - Get database objects grouped according to their schema versions</li>
 * </ul>
 *
 * <p>
 * <b>Reference Paths</b>
 * <ul>
 *  <li>{@link #followReferencePath followReferencePath()} - Find all objects referred to by to any element in a given set
 *      of starting objects through a specified {@link ReferencePath}</li>
 *  <li>{@link #invertReferencePath invertReferencePath()} - Find all objects that refer to any element in a given set
 *      of objects through a specified {@link ReferencePath}</li>
 * </ul>
 *
 * <p>
 * <b>Snapshot Transactions</b>
 * <ul>
 *  <li>{@link #getSnapshotTransaction getSnapshotTransaction()} - Get the default in-memory snapshot transaction
 *      associated with this regular transaction</li>
 *  <li>{@link #createSnapshotTransaction createSnapshotTransaction()} - Create a new in-memory snapshot transaction</li>
 *  <li>{@link #isSnapshot} - Determine whether this transaction is a snapshot transaction</li>
 * </ul>
 *
 * <p>
 * <b>Lower Layer Access</b>
 * <ul>
 *  <li>{@link #getKey(JObject) getKey()} - Get the {@link io.permazen.kv.KVDatabase} key prefix for a specific object</li>
 *  <li>{@link #getKey(JObject, String) getKey()} - Get the {@link io.permazen.kv.KVDatabase}
 *      key for a specific field in a specific object</li>
 * </ul>
 *
 * <p>
 * The remaining methods in this class are normally only used by generated Java model object subclasses.
 * Instead of using these methods directly, using the appropriately annotated Java model object method
 * or {@link JObject} interface method is recommended.
 *
 * <p>
 * <b>Java Model Object Methods</b>
 * <ul>
 *  <li>{@link #readSimpleField readSimpleField()} - Read the value of a simple field</li>
 *  <li>{@link #writeSimpleField writeSimpleField()} - Write the value of a simple field</li>
 *  <li>{@link #readCounterField readCounterField()} - Access a {@link Counter} field</li>
 *  <li>{@link #readSetField readSetField()} - Access a set field</li>
 *  <li>{@link #readListField readListField()} - Access a list field</li>
 *  <li>{@link #readMapField readMapField()} - Access a map field</li>
 *  <li>{@link #registerJObject registerJObject()} - Ensure a {@link JObject} is registered in the object cache</li>
 * </ul>
 *
 * <p>
 * <b>{@link JObject} Methods</b>
 * <ul>
 *  <li>{@link #delete delete()} - Delete an object from this transaction</li>
 *  <li>{@link #exists exists()} - Test whether an object exists in this transaction</li>
 *  <li>{@link #recreate recreate()} - Recreate an object in this transaction</li>
 *  <li>{@link #revalidate revalidate()} - Manually add an object to the validation queue</li>
 *  <li>{@link #getSchemaVersion getSchemaVersion()} - Get this schema version of an object</li>
 *  <li>{@link #updateSchemaVersion updateSchemaVersion()} - Update an object's schema version</li>
 * </ul>
 */
@ThreadSafe
public class JTransaction {

    private static final ThreadLocal<JTransaction> CURRENT = new ThreadLocal<>();
    private static final Class<?>[] DEFAULT_CLASS_ARRAY = { Default.class };
    private static final Class<?>[] DEFAULT_AND_UNIQUENESS_CLASS_ARRAY = { Default.class, UniquenessConstraints.class };
    private static final int MAX_UNIQUE_CONFLICTORS = 5;

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final Permazen jdb;
    final Transaction tx;
    final ReferenceConverter<JObject> referenceConverter = new ReferenceConverter<>(this, JObject.class);

    private final ValidationMode validationMode;
    @GuardedBy("this")
    private final ObjIdMap<Class<?>[]> validationQueue = new ObjIdMap<>();  // maps object -> groups for pending validation
    private final JObjectCache jobjectCache = new JObjectCache(this);

    @GuardedBy("this")
    private SnapshotJTransaction snapshotTransaction;
    @GuardedBy("this")
    private boolean commitInvoked;

// Constructor

    /**
     * Constructor.
     *
     * @throws IllegalArgumentException if any parameter is null
     */
    JTransaction(Permazen jdb, Transaction tx, ValidationMode validationMode) {

        // Initialization
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        this.jdb = jdb;
        this.tx = tx;
        this.validationMode = validationMode;

        // Set back-reference
        tx.setUserObject(this);

        // Register listeners, or re-use our existing listener set
        final boolean automaticValidation = validationMode == ValidationMode.AUTOMATIC;
        final boolean isSnapshot = this.isSnapshot();
        final int listenerSetIndex = (automaticValidation ? 2 : 0) + (isSnapshot ? 0 : 1);
        final Transaction.ListenerSet listenerSet = jdb.listenerSets[listenerSetIndex];
        if (listenerSet == null) {
            JTransaction.registerListeners(jdb, tx, automaticValidation, isSnapshot);
            jdb.listenerSets[listenerSetIndex] = tx.snapshotListeners();
        } else
            tx.setListeners(listenerSet);
    }

    // Register listeners for the given situation
    private static void registerListeners(Permazen jdb, Transaction tx, boolean automaticValidation, boolean isSnapshot) {

        // Register listeners for @OnCreate and validation on creation
        if (jdb.hasOnCreateMethods || (automaticValidation && jdb.anyJClassRequiresDefaultValidation))
            tx.addCreateListener(new InternalCreateListener());

        // Register listeners for @OnDelete
        if (jdb.hasOnDeleteMethods)
            tx.addDeleteListener(new InternalDeleteListener());

        // Register listeners for @OnChange
        for (JClass<?> jclass : jdb.jclasses.values()) {
            for (OnChangeScanner<?>.MethodInfo info : jclass.onChangeMethods) {
                if (isSnapshot && !info.getAnnotation().snapshotTransactions())
                    continue;
                final OnChangeScanner<?>.ChangeMethodInfo changeInfo = (OnChangeScanner<?>.ChangeMethodInfo)info;
                changeInfo.registerChangeListener(tx);
            }
        }

        // Register field change listeners to trigger validation of corresponding JSR 303 and uniqueness constraints
        if (automaticValidation) {
            final DefaultValidationListener defaultValidationListener = new DefaultValidationListener();
            jdb.fieldsRequiringDefaultValidation
              .forEach(storageId -> tx.addFieldChangeListener(storageId, new int[0], null, defaultValidationListener));
        }

        // Register listeners for @OnVersionChange and validation on upgrade
        if (jdb.hasOnVersionChangeMethods
          || jdb.hasUpgradeConversions
          || (automaticValidation && jdb.anyJClassRequiresDefaultValidation))
            tx.addVersionChangeListener(new InternalVersionChangeListener());
    }

// Thread-local Access

    /**
     * Get the {@link JTransaction} associated with the current thread, if any, otherwise throw an exception.
     *
     * @return instance previously associated with the current thread via {@link #setCurrent setCurrent()}
     * @throws IllegalStateException if there is no such instance
     */
    public static JTransaction getCurrent() {
        final JTransaction jtx = CURRENT.get();
        if (jtx == null) {
            throw new IllegalStateException("there is no " + JTransaction.class.getSimpleName()
              + " associated with the current thread");
        }
        return jtx;
    }

    /**
     * Set the {@link JTransaction} associated with the current thread.
     *
     * @param jtx transaction to associate with the current thread
     */
    public static void setCurrent(JTransaction jtx) {
        CURRENT.set(jtx);
    }

// Accessors

    /**
     * Get the {@link Permazen} associated with this instance.
     *
     * @return the associated database
     */
    public Permazen getPermazen() {
        return this.jdb;
    }

    /**
     * Get the {@link Transaction} associated with this instance.
     *
     * @return the associated core API transaction
     */
    public Transaction getTransaction() {
        return this.tx;
    }

    /**
     * Get the {@link ValidationMode} configured for this instance.
     *
     * @return the configured validation mode
     */
    public ValidationMode getValidationMode() {
        return this.validationMode;
    }

    /**
     * Get all instances of the given type.
     *
     * <p>
     * The returned set includes objects from all schema versions. Use {@link #queryVersion queryVersion()} to
     * find objects with a specific schema version.
     *
     * <p>
     * The returned set is mutable, with the exception that {@link NavigableSet#add add()} is not supported.
     * Deleting an element results in {@linkplain JObject#delete deleting} the corresponding object.
     *
     * @param type any Java type; use {@link Object Object.class} to return all database objects
     * @param <T> containing Java type
     * @return a live view of all instances of {@code type}
     * @throws IllegalArgumentException if {@code type} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> getAll(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        NavigableSet<ObjId> ids = this.tx.getAll();
        final KeyRanges keyRanges = this.jdb.keyRangesFor(type);
        if (!keyRanges.isFull())
            ids = ((AbstractKVNavigableSet<ObjId>)ids).filterKeys(keyRanges);
        return new ConvertedNavigableSet<T, ObjId>(ids, new ReferenceConverter<T>(this, type));
    }

    /**
     * Get all instances of the given type, grouped according to schema version.
     *
     * @param type any Java type; use {@link Object Object.class} to return all database objects
     * @param <T> containing Java type
     * @return mapping from schema version to objects having that version
     * @throws IllegalArgumentException if {@code type} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public <T> NavigableMap<Integer, NavigableSet<T>> queryVersion(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        CoreIndex<Integer, ObjId> index = this.tx.queryVersion();
        final KeyRanges keyRanges = this.jdb.keyRangesFor(type);
        if (!keyRanges.isFull())
            index = index.filter(1, keyRanges);
        return new ConvertedNavigableMap<Integer, NavigableSet<T>, Integer, NavigableSet<ObjId>>(index.asMap(),
          Converter.<Integer>identity(), new NavigableSetConverter<T, ObjId>(new ReferenceConverter<T>(this, type)));
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>Objects utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link io.permazen.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param jobj Java model object
     * @return the {@link io.permazen.kv.KVDatabase} key corresponding to {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     * @see io.permazen.kv.KVTransaction#watchKey KVTransaction.watchKey()
     * @see io.permazen.core.Transaction#getKey(ObjId) Transaction.getKey()
     */
    public byte[] getKey(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return this.tx.getKey(jobj.getObjId());
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified field in the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>Complex fields utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link io.permazen.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param jobj Java model object
     * @param fieldName the name of a field in {@code jobj}'s type
     * @return the {@link io.permazen.kv.KVDatabase} key of the field in the specified object
     * @throws TypeNotInSchemaVersionException if the current schema version does not contain the object's type
     * @throws IllegalArgumentException if {@code jobj} does not contain the specified field
     * @throws IllegalArgumentException if {@code fieldName} is otherwise invalid
     * @throws IllegalArgumentException if either parameter is null
     * @see io.permazen.kv.KVTransaction#watchKey KVTransaction.watchKey()
     * @see io.permazen.core.Transaction#getKey(ObjId, int) Transaction.getKey()
     */
    public byte[] getKey(JObject jobj, String fieldName) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        final Class<?> type = this.jdb.getJClass(jobj.getObjId()).type;
        final ReferencePath refPath = this.jdb.parseReferencePath(type, fieldName, true, false);
        if (refPath.getReferenceFields().length > 0)
            throw new IllegalArgumentException("invalid field name `" + fieldName + "'");
        assert refPath.getTargetTypes().iterator().next().isInstance(jobj);
        return this.tx.getKey(jobj.getObjId(), refPath.targetFieldStorageId);
    }

// Snapshots

    /**
     * Determine whether this instance is a {@link SnapshotJTransaction}.
     *
     * @return true if this instance is a {@link SnapshotJTransaction}, otherwise false
     */
    public boolean isSnapshot() {
        return false;
    }

    /**
     * Get the default {@link SnapshotJTransaction} associated with this instance.
     *
     * <p>
     * The default {@link SnapshotJTransaction} uses {@link ValidationMode#MANUAL}.
     *
     * <p>
     * This instance must not itself be a {@link SnapshotJTransaction}; use
     * {@link #createSnapshotTransaction createSnapshotTransaction()} to create additional snapshot transactions.
     *
     * @return the associated snapshot transaction
     * @see JObject#copyOut JObject.copyOut()
     * @throws IllegalArgumentException if this instance is itself a {@link SnapshotJTransaction}
     */
    public synchronized SnapshotJTransaction getSnapshotTransaction() {
        Preconditions.checkArgument(!this.isSnapshot(),
          "getSnapshotTransaction() invoked on a snapshot transaction; use createSnapshotTransaction() instead");
        if (this.snapshotTransaction == null)
            this.snapshotTransaction = this.createSnapshotTransaction(ValidationMode.MANUAL);
        return this.snapshotTransaction;
    }

    /**
     * Create an empty snapshot transaction based on this instance.
     *
     * <p>
     * This new instance will have the same schema meta-data as this instance.
     *
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return newly created snapshot transaction
     * @throws IllegalArgumentException if {@code validationMode} is null
     * @throws io.permazen.core.StaleTransactionException if this instance is no longer usable
     */
    public SnapshotJTransaction createSnapshotTransaction(ValidationMode validationMode) {
        return new SnapshotJTransaction(this.jdb, this.tx.createSnapshotTransaction(), validationMode);
    }

    /**
     * Copy the specified object, and any other objects referenced through the specified reference paths,
     * into the specified destination transaction.
     *
     * <p>
     * If the target object does not exist, it will be created, otherwise its schema version will be updated to match the source
     * object if necessary (with resulting {@link io.permazen.annotation.OnVersionChange &#64;OnVersionChange} notifications).
     * If {@link CopyState#isSuppressNotifications()} returns false, {@link io.permazen.annotation.OnCreate &#64;OnCreate}
     * and {@link io.permazen.annotation.OnChange &#64;OnChange} notifications will also be delivered; however,
     * these annotations must also have {@code snapshotTransactions = true} if {@code dest} is a {@link SnapshotJTransaction}).
     *
     * <p>
     * Circular references are handled properly: if an object is encountered more than once, it is not copied again.
     * The {@code copyState} tracks which objects have already been copied and/or traversed along some reference path.
     * For a "fresh" copy operation, pass a newly created {@link CopyState}; for a copy operation that is a continuation
     * of a previous copy, reuse the previous {@code copyState}. The {@link CopyState} may also be configured to remap object ID's.
     *
     * <p>
     * This instance and {@code dest} must be compatible in that for any schema versions encountered, those schema versions
     * must be identical in both transactions.
     *
     * <p>
     * If any copied objects contain reference fields configured with
     * {@link io.permazen.annotation.JField#allowDeleted} equal to {@code false},
     * then any objects referenced by those fields must also be copied, or else must already exist in {@code dest}
     * (if {@code dest} is a {@link SnapshotJTransaction}, then {@link io.permazen.annotation.JField#allowDeletedSnapshot}
     * applies instead). Otherwise, a {@link DeletedObjectException} is thrown and it is indeterminate which objects were copied.
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     *
     * @param dest destination transaction
     * @param jobj object to copy
     * @param copyState tracks which objects have already been copied and traversed and whether to remap object ID's
     * @param refPaths zero or more reference paths that refer to additional objects to be copied (including intermediate objects)
     * @return the copied object, i.e., the object having ID {@code dstId} in {@code dest}
     * @throws DeletedObjectException if any object to be copied does not actually exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object that does not exist
     *  in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws io.permazen.core.SchemaMismatchException
     *  if the schema corresponding to any copied object is not identical in both this instance and {@code dest}
     * @throws TypeNotInSchemaVersionException if the current schema version does not contain the source object's type
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @throws IllegalArgumentException if any parameter other than {@code dstId} is null
     * @see JObject#copyTo JObject.copyTo()
     * @see JObject#copyOut JObject.copyOut()
     * @see JObject#copyIn JObject.copyIn()
     * @see #copyTo(JTransaction, CopyState, Stream)
     * @see ReferencePath
     */
    public JObject copyTo(JTransaction dest, JObject jobj, CopyState copyState, String... refPaths) {

        // Sanity check
        Preconditions.checkArgument(dest != null, "null dest");
        Preconditions.checkArgument(jobj != null, "null jobj");
        Preconditions.checkArgument(copyState != null, "null copyState");
        Preconditions.checkArgument(refPaths != null, "null refPaths");

        // Handle possible re-entrant object cache load
        JTransaction.registerJObject(jobj);

        // Get object ID
        final ObjId id = jobj.getObjId();

        // Parse paths and convert each into an array of reference fields (including the target reference field at the end)
        final Class<?> startType = this.jdb.getJClass(id).type;
        final ArrayList<int[]> pathReferencesList = new ArrayList<>(refPaths.length);
        for (String refPath : refPaths) {

            // Parse reference path
            Preconditions.checkArgument(refPath != null, "null refPath");
            final ReferencePath path = this.jdb.parseReferencePath(startType, refPath, false, true);

            // Append array of reference fields to our list
            pathReferencesList.add(path.referenceFieldStorageIds);
        }

        // Reset deleted assignments
        copyState.deletedAssignments.clear();

        // Ensure object is copied even when there are zero reference paths
        if (pathReferencesList.isEmpty())
            this.copyTo(copyState, dest, id, true, 0, new int[0]);

        // Recurse over each reference path
        for (int[] pathReferences : pathReferencesList)
            this.copyTo(copyState, dest, id, false/*doesn't matter*/, 0, pathReferences);

        // Check for any remaining deleted assignments
        copyState.checkDeletedAssignments(this);

        // Done
        return dest.get(copyState.getDestinationId(id));
    }

    /**
     * Copy the specified objects into the specified destination transaction.
     *
     * <p>
     * This is a convenience method; equivalent to {@link #copyTo(JTransaction, CopyState, Stream)}}
     * but with the objects specified by object ID.
     *
     * @param dest destination transaction
     * @param copyState tracks which objects have already been copied and whether to remap object ID's
     * @param ids the object ID's of the objects to copy
     * @throws DeletedObjectException if any object to be copied does not actually exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object that does not exist
     *  in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws io.permazen.core.SchemaMismatchException
     *  if the schema corresponding to any copied object is not identical in both this instance and {@code dest}
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws IllegalArgumentException if any parameter is null
     */
    public void copyTo(JTransaction dest, CopyState copyState, ObjIdSet ids) {
        Preconditions.checkArgument(ids != null, "null ids");
        this.copyIdStreamTo(dest, copyState, ids.stream());
    }

    /**
     * Copy the specified objects into the specified destination transaction.
     *
     * <p>
     * If a target object does not exist, it will be created, otherwise its schema version will be updated to match the source
     * object if necessary (with resulting {@link io.permazen.annotation.OnVersionChange &#64;OnVersionChange} notifications).
     * If {@link CopyState#isSuppressNotifications()} returns false, {@link io.permazen.annotation.OnCreate &#64;OnCreate}
     * and {@link io.permazen.annotation.OnChange &#64;OnChange} notifications will also be delivered; however,
     * these annotations must also have {@code snapshotTransactions = true} if {@code dest} is a {@link SnapshotJTransaction}).
     *
     * <p>
     * The {@code copyState} tracks which objects have already been copied. For a "fresh" copy operation,
     * pass a newly created {@link CopyState}; for a copy operation that is a continuation of a previous copy,
     * reuse the previous {@link CopyState}. The {@link CopyState} may also be configured to remap object ID's.
     *
     * <p>
     * This instance and {@code dest} must be compatible in that for any schema versions encountered, those schema versions
     * must be identical in both transactions.
     *
     * <p>
     * If any copied objects contain reference fields configured with
     * {@link io.permazen.annotation.JField#allowDeleted}{@code = false},
     * then any objects referenced by those fields must also be copied, or else must already exist in {@code dest}
     * (if {@code dest} is a {@link SnapshotJTransaction}, then {@link io.permazen.annotation.JField#allowDeletedSnapshot}
     * applies instead). Otherwise, a {@link DeletedObjectException} is thrown and it is indeterminate which objects were copied.
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     *
     * @param dest destination transaction
     * @param jobjs the objects to copy; null values are ignored
     * @param copyState tracks which objects have already been copied and whether to remap object ID's
     * @throws DeletedObjectException if any object to be copied does not actually exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object that does not exist
     *  in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws io.permazen.core.SchemaMismatchException
     *  if the schema corresponding to any copied object is not identical in both this instance and {@code dest}
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws IllegalArgumentException if {@code dest}, {@code copyState}, or {@code jobjs} is null
     */
    public void copyTo(JTransaction dest, CopyState copyState, Stream<? extends JObject> jobjs) {
        this.copyIdStreamTo(dest, copyState, jobjs
          .filter(Objects::nonNull)
          .peek(JTransaction::registerJObject)                              // handle possible re-entrant object cache load
          .map(JObject::getObjId));
    }

    void copyIdStreamTo(JTransaction dest, CopyState copyState, Stream<ObjId> ids) {

        // Sanity check
        Preconditions.checkArgument(dest != null, "null dest");
        Preconditions.checkArgument(copyState != null, "null copyState");
        Preconditions.checkArgument(ids != null, "null ids");

        // Reset deleted assignments
        copyState.deletedAssignments.clear();

        // Copy objects
        ids.forEachOrdered(id -> this.copyTo(copyState, dest, id, true, 0, new int[0]));

        // Check for any remaining deleted assignments
        copyState.checkDeletedAssignments(this);
    }

    void copyTo(CopyState copyState, JTransaction dest, ObjId srcId, boolean required, int fieldIndex, int[] fields) {

        // Copy current instance unless already copied, upgrading it in the process
        if (copyState.markCopied(srcId)) {

            // Get destination ID
            final ObjId dstId = copyState.getDestinationId(srcId);

            // See if we can disable listener notifications
            boolean disableListenerNotifications = copyState.isSuppressNotifications();
            final JClass<?> jclass = dest.jdb.jclasses.get(dstId.getStorageId());
            if (!disableListenerNotifications && dest.isSnapshot() && jclass != null)
                disableListenerNotifications = !jclass.hasSnapshotCreateOrChangeMethods;

            // Reset any cached fields in the destination object
            final JObject dstObject = dest.jobjectCache.getIfExists(dstId);
            if (dstObject != null)
                dstObject.resetCachedFieldValues();

            // Copy object at the core API level
            final ObjIdMap<ReferenceField> coreDeletedAssignments = new ObjIdMap<>();
            boolean exists = true;
            try {
                this.tx.copy(srcId, dest.tx, true,
                  !disableListenerNotifications, coreDeletedAssignments, copyState.getObjectIdMap());
            } catch (DeletedObjectException e) {
                if (required)
                    throw e;
                exists = false;
            }

            // Revalidate destination object if needed
            if (dest.validationMode.equals(ValidationMode.AUTOMATIC) && jclass != null && jclass.requiresDefaultValidation)
                dest.revalidate(Collections.singleton(dstId));

            // Add any deleted assignments from the core API copy to our copy state
            for (Map.Entry<ObjId, ReferenceField> entry : coreDeletedAssignments.entrySet()) {
                assert !copyState.isCopied(entry.getKey());
                copyState.deletedAssignments.put(entry.getKey(), new DeletedAssignment(dstId, entry.getValue()));
            }

            // If the copy was successful, remove the copied object from the deleted assignments set in our copy state.
            // This fixes up "forward reference" deleted assignments that get satisfied later in the overall copy operation.
            if (exists)
                copyState.deletedAssignments.remove(dstId);
        }

        // Any more fields to traverse?
        if (fieldIndex == fields.length)
            return;

        // Have we already traversed the path?
        final int[] pathSuffix = fieldIndex == 0 ? fields : Arrays.copyOfRange(fields, fieldIndex, fields.length);
        if (!copyState.markTraversed(srcId, pathSuffix))
            return;

        // Recurse through the next reference field in the path
        final int storageId = fields[fieldIndex++];
        final SimpleFieldIndexInfo info = (SimpleFieldIndexInfo)this.jdb.indexInfoMap.get(storageId);
        assert info == null || info.getFieldType() instanceof ReferenceFieldType;
        if (info instanceof ComplexSubFieldIndexInfo) {
            final ComplexSubFieldIndexInfo subFieldInfo = (ComplexSubFieldIndexInfo)info;
            subFieldInfo.copyRecurse(copyState, this, dest, srcId, fieldIndex, fields);
        } else {
            final ObjId referrent = (ObjId)this.tx.readSimpleField(srcId, storageId, false);
            if (referrent != null)
                this.copyTo(copyState, dest, referrent, false, fieldIndex, fields);
        }
    }

    /**
     * Create a new object in this transaction of the same type for each object ID in the given set.
     *
     * <p>
     * The newly created objects will be in their initial states.
     *
     * @param ids object ID's
     * @return mapping from object ID in {@code ids} to newly created object
     * @throws IllegalArgumentException if {@code ids} is null
     */
    public ObjIdMap<ObjId> createClones(ObjIdSet ids) {
        Preconditions.checkArgument(ids != null, "null ids");
        final ObjIdMap<ObjId> map = new ObjIdMap<>(ids.size());
        for (ObjId id : ids)
            map.put(id, this.tx.create(id.getStorageId()));
        return map;
    }

    /**
     * Recursively traverse cascade references starting from the given object to find all objects reachable
     * through the specified cascade.
     *
     * <p>
     * This method finds all objects reachable from the starting object based on
     * {@link io.permazen.annotation.JField#cascades &#64;JField.cascades()} and
     * {@link io.permazen.annotation.JField#inverseCascades &#64;JField.inverseCascades()} annotation properties on
     * reference fields: a reference field is traversed in the forward or inverse direction if {@code cascadeName} is
     * specified in the corresponding annotation property. See {@link io.permazen.annotation.JField &#64;JField} for details.
     *
     * <p>
     * All objects found will be {@linkplain #updateSchemaVersion upgraded} if necessary.
     *
     * <p>
     * The {@code recursionLimit} parameter can be used to limit the maximum distance of any reachable object,
     * measured in the number of reference field "hops" from the given object.
     *
     * @param id starting object ID
     * @param cascadeName cascade name, or null for no cascade (returns just the {@code id} object)
     * @param recursionLimit the maximum number of references to hop through, or -1 for infinity
     * @return the object ID's of all objects reachable through the specified cascade (including the {@code id} object)
     * @throws DeletedObjectException if any object containing a traversed reference field does not actually exist
     * @throws IllegalArgumentException if {@code recursionLimit} is less that -1
     * @throws IllegalArgumentException if {@code id} is null
     * @see JObject#cascadeCopyTo(JTransaction, String, int, boolean)
     */
    public ObjIdSet cascadeFindAll(ObjId id, String cascadeName, int recursionLimit) {
        final ObjIdSet ids = new ObjIdSet();
        this.cascadeFindAll(id, cascadeName, recursionLimit, ids);
        return ids;
    }

    /**
     * Recursively traverse cascade references starting from the given object to find all objects reachable
     * through the specified cascade.
     *
     * <p>
     * Upon invocation {@code visitedIds} contains the ID's of objects already visited; these objects will not be traversed.
     * In particular, if {@code id} is in {@code visitedIds}, then this method does nothing.
     * Upon return, {@code visitedIds} will also contain the ID's of all new objects found.
     *
     * <p>
     * All objects found will be {@linkplain #updateSchemaVersion upgraded} if necessary.
     *
     * <p>
     * The {@code recursionLimit} parameter can be used to limit the maximum distance of any reachable object,
     * measured in the number of reference field "hops" from the given object.
     *
     * @param id starting object ID
     * @param cascadeName cascade name, or null for no cascade
     * @param recursionLimit the maximum number of references to hop through, or -1 for infinity
     * @param visitedIds on entry objects already visited, on return all objects reachable
     * @throws DeletedObjectException if any object containing a traversed reference field does not actually exist
     * @throws IllegalArgumentException if {@code recursionLimit} is less that -1
     * @throws IllegalArgumentException if {@code id} or {@code visitedIds} is null
     * @see JObject#cascadeCopyTo(JTransaction, String, int, boolean)
     */
    public void cascadeFindAll(ObjId id, String cascadeName, int recursionLimit, ObjIdSet visitedIds) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(recursionLimit >= -1, "recursionLimit < -1");
        Preconditions.checkArgument(visitedIds != null, "null visitedIds");

        // Initialize search
        ObjIdSet toVisitIds = new ObjIdSet();
        if (visitedIds.add(id))
            toVisitIds.add(id);

        // Do we need to cascade?
        if (cascadeName == null)
            return;

        // While there are objects remaining to scan, cascade through forward and inverse reference field cascades
        while (!toVisitIds.isEmpty()) {
            assert visitedIds.containsAll(toVisitIds);

            // Stop if we reach the recursion limit
            if (recursionLimit != -1 && recursionLimit-- <= 0)
                break;

            // Find all new objects reachable in one hop from any object in 'toVisitIds'
            final ObjIdSet newIds = new ObjIdSet();
            for (ObjId toVisitId : toVisitIds) {

                // Upgrade object if needed
                this.tx.updateSchemaVersion(toVisitId);

                // Gather references
                this.gatherForwardCascadeRefs(toVisitId, cascadeName, visitedIds, newIds);
                this.gatherInverseCascadeRefs(toVisitId, cascadeName, visitedIds, newIds);
            }

            // New objects will be expanded on next time
            toVisitIds = newIds;
        }
    }

    private void gatherForwardCascadeRefs(ObjId id, String cascadeName, ObjIdSet visitedIds, ObjIdSet toVisitIds) {
        final JClass<?> jclass = this.jdb.jclasses.get(id.getStorageId());
        if (jclass == null)
            return;
        final List<JReferenceField> fieldList = jclass.forwardCascadeMap.get(cascadeName);
        if (fieldList == null)
            return;
        for (JReferenceField field : fieldList) {
            final SimpleFieldIndexInfo info = (SimpleFieldIndexInfo)this.jdb.indexInfoMap.get(field.storageId);
            assert info.getFieldType() instanceof ReferenceFieldType;
            if (info instanceof ComplexSubFieldIndexInfo) {
                final ComplexSubFieldIndexInfo subFieldInfo = (ComplexSubFieldIndexInfo)info;
                this.gatherRefs(subFieldInfo.iterateReferences(this.tx, id).iterator(), visitedIds, toVisitIds);
            } else {
                final ObjId referrent = (ObjId)this.tx.readSimpleField(id, field.storageId, false);
                if (referrent != null && visitedIds.add(referrent))
                    toVisitIds.add(referrent);
            }
        }
    }

    private void gatherInverseCascadeRefs(ObjId id, String cascadeName, ObjIdSet visitedIds, ObjIdSet toVisitIds) {
        final JClass<?> jclass = this.jdb.jclasses.get(id.getStorageId());
        if (jclass == null)
            return;
        final Map<Integer, KeyRanges> incomingCascades = jclass.inverseCascadeMap.get(cascadeName);
        if (incomingCascades == null)
            return;
        for (Map.Entry<Integer, KeyRanges> entry : incomingCascades.entrySet()) {
            final int referenceFieldStorageId = entry.getKey();
            final KeyRanges cascadeReferringTypeRanges = entry.getValue();

            // Access the index associated with the reference field
            CoreIndex<?, ?> index = this.tx.queryIndex(referenceFieldStorageId);

            // Restrict index to references from objects containing the field with the cascade (their ranges already precomputed)
            index = index.filter(1, cascadeReferringTypeRanges);

            // Find objects referring to "id" through the field and add them to our cascade
            final NavigableSet<?> refs = index.asMap().get(id);
            if (refs != null)
                this.gatherRefs(refs.iterator(), visitedIds, toVisitIds);
        }
    }

    private void gatherRefs(Iterator<?> i0, ObjIdSet visitedIds, ObjIdSet toVisitIds) {
        try (CloseableIterator<?> i = CloseableIterator.wrap(i0)) {
            while (i.hasNext()) {
                final ObjId ref = (ObjId)i.next();
                if (ref != null && visitedIds.add(ref))
                    toVisitIds.add(ref);
            }
        }
    }

// Object/Field Access

    /**
     * Get the Java model object that is associated with this transaction and has the given ID.
     *
     * <p>
     * This method guarantees that for any particular {@code id}, the same Java instance will always be returned.
     *
     * <p>
     * <b>A non-null object is always returned, but the corresponding object may not actually exist in this transaction.</b>
     * In that case, attempts to access its fields will throw {@link io.permazen.core.DeletedObjectException}.
     * Use {@link JObject#exists JObject.exists()} to check.
     *
     * <p>
     * Also, it's possible that {@code id} corresponds to an object type that no longer exists in the schema
     * version associated with this transaction. In that case, an {@link UntypedJObject} is returned.
     *
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     * @see #get(ObjId, Class)
     * @see #get(JObject)
     */
    public JObject get(ObjId id) {
        return this.jobjectCache.get(id);
    }

    /**
     * Get the Java model object that is associated with this transaction and has the given ID, cast to the given type.
     *
     * <p>
     * This method guarantees that for any particular {@code id}, the same Java instance will always be returned.
     *
     * <p>
     * <b>A non-null object is always returned, but the corresponding object may not actually exist in this transaction.</b>
     * In that case, attempts to access its fields will throw {@link io.permazen.core.DeletedObjectException}.
     * Use {@link JObject#exists JObject.exists()} to check.
     *
     * <p>
     * This method just invoke {@link #get(ObjId)} and then casts the result.
     *
     * @param id object ID
     * @param type expected type
     * @param <T> expected Java model type
     * @return Java model object
     * @throws ClassCastException if the Java model object does not have type {@code type}
     * @throws IllegalArgumentException if {@code type} is null
     * @see #get(ObjId)
     * @see #get(JObject)
     */
    public <T> T get(ObjId id, Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        return type.cast(this.get(id));
    }

    /**
     * Get the Java model object with the same object ID as the given {@link JObject} and whose state derives from this transaction.
     *
     * <p>
     * This method can be thought of as a "refresh" operation for objects being imported from other transactions into this one.
     *
     * <p>
     * <b>A non-null object is always returned, but the corresponding object may not actually exist in this transaction.</b>
     * In that case, attempts to access its fields will throw {@link io.permazen.core.DeletedObjectException}.
     * Use {@link JObject#exists JObject.exists()} to check.
     *
     * <p>
     * This method is equivalent to {@code get(jobj.getObjId())} followed by an appropriate cast to type {@code T}.
     *
     * @param jobj Java model object
     * @param <T> expected Java type
     * @return Java model object in this transaction with the same object ID (possibly {@code jobj} itself)
     * @throws IllegalArgumentException if {@code jobj} is null, or not a {@link Permazen} database object
     * @throws ClassCastException if the Java model object in this transaction somehow does not have the same type as {@code jobj}
     * @see #get(ObjId)
     * @see #get(ObjId, Class)
     */
    @SuppressWarnings("unchecked")
    public <T extends JObject> T get(T jobj) {
        return (T)jobj.getModelClass().cast(this.get(jobj.getObjId()));
    }

    /**
     * Create a new instance of the given model class in this transaction.
     *
     * @param type Java object model type
     * @param <T> Java model type
     * @return newly created instance
     * @throws IllegalArgumentException if {@code type} is not a known Java object model type
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public <T> T create(Class<T> type) {
        return this.create(this.jdb.getJClass(type));
    }

    /**
     * Create a new instance of the given type in this transaction.
     *
     * @param jclass object type
     * @param <T> Java model type
     * @return newly created instance
     * @throws IllegalArgumentException if {@code jclass} is not valid for this instance
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public <T> T create(JClass<T> jclass) {
        final ObjId id = this.tx.create(jclass.storageId);
        return jclass.getType().cast(this.get(id));
    }

    /**
     * Delete the object with the given object ID in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#delete} would be used instead.
     *
     * @param jobj the object to delete
     * @return true if object was found and deleted, false if object was not found
     * @throws io.permazen.core.ReferencedObjectException if the object is referenced by some other object
     *  through a reference field configured for {@link io.permazen.core.DeleteAction#EXCEPTION}
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean delete(JObject jobj) {

        // Handle possible re-entrant object cache load
        JTransaction.registerJObject(jobj);

        // Delete object
        final ObjId id = jobj.getObjId();
        final boolean deleted = this.tx.delete(id);

        // Remove object from validation queue if enqueued
        if (deleted) {
            synchronized (this) {
                this.validationQueue.remove(id);
            }
        }

        // Reset cached field values
        if (deleted)
            jobj.resetCachedFieldValues();

        // Done
        return deleted;
    }

    /**
     * Determine whether the object with the given object ID exists in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#exists} would be used instead.
     *
     * @param id ID of the object to test for existence
     * @return true if object was found, false if object was not found
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws NullPointerException if {@code id} is null
     */
    public boolean exists(ObjId id) {
        return this.tx.exists(id);
    }

    /**
     * Recreate the given instance in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#recreate} would be used instead.
     *
     * @param jobj the object to recreate
     * @return true if the object was recreated, false if the object already existed
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean recreate(JObject jobj) {
        JTransaction.registerJObject(jobj);                                     // handle possible re-entrant object cache load
        return this.tx.create(jobj.getObjId());
    }

    /**
     * Add the given instance to the validation queue for validation, which will occur either at {@link #commit} time
     * or at the next invocation of {@link #validate}, whichever occurs first.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#revalidate} would be used instead.
     *
     * @param id ID of the object to revalidate
     * @param groups validation group(s) to use for validation; if empty, {@link jakarta.validation.groups.Default} is assumed
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws IllegalArgumentException if either parameter is null
     * @throws IllegalArgumentException if any group in {@code groups} is null
     */
    public void revalidate(ObjId id, Class<?>... groups) {
        if (!this.tx.exists(id))
            throw new DeletedObjectException(this.tx, id);
        this.revalidate(Collections.singleton(id), groups);
    }

    /**
     * Clear the validation queue associated with this transaction. Any previously enqueued objects that have not
     * yet been validated will no longer receive validation.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if transaction commit is already in progress
     */
    public synchronized void resetValidationQueue() {
        if (!this.tx.isValid())
            throw new StaleTransactionException(this.tx);
        this.validationQueue.clear();
    }

    private synchronized void revalidate(Collection<? extends ObjId> ids, Class<?>... groups) {

        // Sanity checks
        if (!this.tx.isValid())
            throw new StaleTransactionException(this.tx);
        Preconditions.checkArgument(groups != null, "null groups");
        for (Class<?> group : groups)
            Preconditions.checkArgument(group != null, "null group");
        if (this.validationMode == ValidationMode.DISABLED)
            return;

        // "Intern" default group array
        if (groups.length == 0 || Arrays.equals(groups, DEFAULT_CLASS_ARRAY))
            groups = DEFAULT_CLASS_ARRAY;

        // Add to queue
        for (ObjId id : ids) {
            final Class<?>[] existingGroups = this.validationQueue.get(id);
            if (existingGroups == null) {
                this.validationQueue.put(id, groups);
                continue;
            }
            if (existingGroups == groups)                                       // i.e., both are DEFAULT_CLASS_ARRAY
                continue;
            final HashSet<Class<?>> newGroups = new HashSet<>(Arrays.asList(existingGroups));
            newGroups.addAll(Arrays.asList(groups));
            this.validationQueue.put(id, newGroups.toArray(new Class<?>[newGroups.size()]));
        }
    }

    /**
     * Get this schema version of the specified object. Does not change the object's schema version.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#getSchemaVersion} would be used instead.
     *
     * @param id ID of the object containing the field
     * @return object's schema version
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws NullPointerException if {@code id} is null
     */
    public int getSchemaVersion(ObjId id) {
        return this.tx.getSchemaVersion(id);
    }

    /**
     * Update the schema version of the specified object, if necessary, so that its version matches
     * the schema version associated with this instance's {@link Permazen}.
     *
     * <p>
     * If a version change occurs, matching {@link io.permazen.annotation.OnVersionChange &#64;OnVersionChange}
     * methods will be invoked prior to this method returning.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#upgrade} would be used instead.
     *
     * @param jobj object to update
     * @return true if the object's schema version was changed, false if it was already updated
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws TypeNotInSchemaVersionException if the current schema version does not contain the object's type
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean updateSchemaVersion(JObject jobj) {
        JTransaction.registerJObject(jobj);                                     // handle possible re-entrant object cache load
        return this.tx.updateSchemaVersion(jobj.getObjId());
    }

    /**
     * Ensure the given {@link JObject} is registered in its associated transaction's object cache.
     *
     * <p>
     * This method is used internally, to handle mutations in model class superclass constructors, which will occur
     * before the newly created {@link JObject} is fully constructed and associated with its {@link JTransaction}.
     *
     * @param jobj object to register
     * @throws NullPointerException if {@code jobj} is null
     */
    public static void registerJObject(JObject jobj) {
        jobj.getTransaction().jobjectCache.register(jobj);
    }

    /**
     * Read a simple field. This returns the value returned by {@link Transaction#readSimpleField Transaction.readSimpleField()}
     * with {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.JField &#64;JField} getter override methods
     * and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param storageId storage ID of the {@link JSimpleField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link JSimpleField} corresponding to {@code storageId} exists
     * @throws TypeNotInSchemaVersionException if {@code updateVersion} is true but the object has a type
     *  that does not exist in this instance's schema version
     * @throws NullPointerException if {@code id} is null
     */
    public Object readSimpleField(ObjId id, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(id, storageId, JSimpleField.class).getConverter(this),
          this.tx.readSimpleField(id, storageId, updateVersion));
    }

    /**
     * Write a simple field. This writes the value via {@link Transaction#writeSimpleField Transaction.writeSimpleField()}
     * after converting {@link JObject}s into {@link ObjId}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.JField &#64;JField} setter override methods
     * and not normally invoked directly by user code.
     *
     * @param jobj object containing the field
     * @param storageId storage ID of the {@link JSimpleField}
     * @param value new value for the field
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link JSimpleField} corresponding to {@code storageId} exists
     * @throws TypeNotInSchemaVersionException if {@code updateVersion} is true but {@code jobj} has a type
     *  that does not exist in this instance's schema version
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for the field
     * @throws NullPointerException if {@code jobj} is null
     */
    public void writeSimpleField(JObject jobj, int storageId, Object value, boolean updateVersion) {
        JTransaction.registerJObject(jobj);                                    // handle possible re-entrant object cache load
        final ObjId id = jobj.getObjId();
        final Converter<?, ?> converter = this.jdb.getJField(id, storageId, JSimpleField.class).getConverter(this);
        if (converter != null)
            value = this.convert(converter.reverse(), value);
        this.tx.writeSimpleField(id, storageId, value, updateVersion);
    }

    /**
     * Read a counter field.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.JField &#64;JField} getter override methods
     * and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param storageId storage ID of the {@link JCounterField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return value of the counter in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link JCounterField} corresponding to {@code storageId} exists
     * @throws TypeNotInSchemaVersionException if {@code updateVersion} is true but the object has a type
     *  that does not exist in this instance's schema version
     * @throws NullPointerException if {@code id} is null
     */
    public Counter readCounterField(ObjId id, int storageId, boolean updateVersion) {
        this.jdb.getJField(id, storageId, JCounterField.class);                 // validate field type
        if (updateVersion)
            this.tx.updateSchemaVersion(id);
        return new Counter(this.tx, id, storageId, updateVersion);
    }

    /**
     * Read a set field. This returns the set returned by {@link Transaction#readSetField Transaction.readSetField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.JSetField &#64;JSetField}
     * getter override methods and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param storageId storage ID of the {@link JSetField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return the set field in the object with storage ID {@code storageId}
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link JSetField} corresponding to {@code storageId} exists
     * @throws TypeNotInSchemaVersionException if {@code updateVersion} is true but the object has a type
     *  that does not exist in this instance's schema version
     * @throws NullPointerException if {@code id} is null
     */
    public NavigableSet<?> readSetField(ObjId id, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(id, storageId, JSetField.class).getConverter(this),
          this.tx.readSetField(id, storageId, updateVersion));
    }

    /**
     * Read a list field. This returns the list returned by {@link Transaction#readListField Transaction.readListField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.JListField &#64;JListField}
     * getter override methods and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param storageId storage ID of the {@link JListField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return the list field in the object with storage ID {@code storageId}
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link JListField} corresponding to {@code storageId} exists
     * @throws TypeNotInSchemaVersionException if {@code updateVersion} is true but the object has a type
     *  that does not exist in this instance's schema version
     * @throws NullPointerException if {@code id} is null
     */
    public List<?> readListField(ObjId id, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(id, storageId, JListField.class).getConverter(this),
          this.tx.readListField(id, storageId, updateVersion));
    }

    /**
     * Read a map field. This returns the map returned by {@link Transaction#readMapField Transaction.readMapField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.JMapField &#64;JMapField}
     * getter override methods and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param storageId storage ID of the {@link JMapField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return the map field in the object with storage ID {@code storageId}
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link JMapField} corresponding to {@code storageId} exists
     * @throws TypeNotInSchemaVersionException if {@code updateVersion} is true but the object has a type
     *  that does not exist in this instance's schema version
     * @throws NullPointerException if {@code id} is null
     */
    public NavigableMap<?, ?> readMapField(ObjId id, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(id, storageId, JMapField.class).getConverter(this),
          this.tx.readMapField(id, storageId, updateVersion));
    }

// Reference Path Access

    /**
     * Find all objects that are reachable from the given starting object set through the specified {@link ReferencePath}.
     *
     * @param path reference path
     * @param startObjects starting objects
     * @return read-only set of objects reachable from {@code startObjects} via {@code path}
     * @throws UnknownFieldException if {@code path} contains an unknown field
     * @throws IllegalArgumentException if either parameter is null
     * @see ReferencePath
     */
    public NavigableSet<JObject> followReferencePath(ReferencePath path, Stream<? extends JObject> startObjects) {
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(startObjects != null, "null startObjects");
        final NavigableSet<ObjId> ids = this.tx.followReferencePath(startObjects.map(this.referenceConverter),
          path.getReferenceFields(), path.getPathKeyRanges());
        return new ConvertedNavigableSet<JObject, ObjId>(ids, this.referenceConverter);
    }

    /**
     * Find all objects that refer to any object in the given target set through the specified {@link ReferencePath}.
     *
     * @param path reference path
     * @param targetObjects target objects
     * @return read-only set of objects that refer to any of the {@code targetObjects} via {@code path}
     * @throws UnknownFieldException if {@code path} contains an unknown field
     * @throws IllegalArgumentException if either parameter is null
     * @see ReferencePath
     */
    public NavigableSet<JObject> invertReferencePath(ReferencePath path, Stream<? extends JObject> targetObjects) {
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(targetObjects != null, "null targetObjects");
        final NavigableSet<ObjId> ids = this.tx.invertReferencePath(path.getReferenceFields(), path.getPathKeyRanges(),
          targetObjects.map(this.referenceConverter));
        return new ConvertedNavigableSet<JObject, ObjId>(ids, this.referenceConverter);
    }

// Index Access

    /**
     * Get the index on a simple field. The simple field may be a sub-field of a complex field.
     *
     * @param targetType Java type containing the indexed field; may also be any super-type (e.g., an interface type),
     *  as long as {@code fieldName} is not ambiguous among all sub-types
     * @param fieldName name of the indexed field; for complex fields,
     *  must include the sub-field name (e.g., {@code "mylist.element"}, {@code "mymap.key"})
     * @param valueType the Java type corresponding to the field value
     * @param <V> Java type corresponding to the indexed field
     * @param <T> Java type containing the field
     * @return read-only, real-time view of field values mapped to sets of objects having that value in the field
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V, T> Index<V, T> queryIndex(Class<T> targetType, String fieldName, Class<V> valueType) {
        final IndexQueryInfo info = this.jdb.getIndexQueryInfo(new IndexQueryInfoKey(fieldName, false, targetType, valueType));
        assert info.indexInfo instanceof SimpleFieldIndexInfo;          // otherwise getIndexQueryInfo() would have failed...
        final SimpleFieldIndexInfo indexInfo = (SimpleFieldIndexInfo)info.indexInfo;
        final CoreIndex<?, ObjId> index = info.applyFilters(this.tx.queryIndex(indexInfo.storageId));
        final Converter<?, ?> valueConverter = indexInfo.getConverter(this).reverse();
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex(index, valueConverter, targetConverter);
    }

    /**
     * Get the composite index on a list field that includes list indices.
     *
     * @param targetType type containing the indexed field; may also be any super-type (e.g., an interface type),
     *  as long as {@code fieldName} is not ambiguous among all sub-types
     * @param fieldName name of the indexed field; must include {@code "element"} sub-field name (e.g., {@code "mylist.element"})
     * @param valueType the Java type corresponding to list elements
     * @param <V> Java type corresponding to the indexed list's element field
     * @param <T> Java type containing the field
     * @return read-only, real-time view of field values, objects having that value in the field, and corresponding list indices
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V, T> Index2<V, T, Integer> queryListElementIndex(Class<T> targetType, String fieldName, Class<V> valueType) {
        final IndexQueryInfo info = this.jdb.getIndexQueryInfo(new IndexQueryInfoKey(fieldName, false, targetType, valueType));
        if (!(info.indexInfo instanceof ListElementIndexInfo))
            throw new IllegalArgumentException("field `" + fieldName + "' is not a list element sub-field");
        final ListElementIndexInfo indexInfo = (ListElementIndexInfo)info.indexInfo;
        final CoreIndex2<?, ObjId, Integer> index = info.applyFilters(this.tx.queryListElementIndex(indexInfo.storageId));
        final Converter<?, ?> valueConverter = indexInfo.getConverter(this).reverse();
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex2(index, valueConverter, targetConverter, Converter.<Integer>identity());
    }

    /**
     * Get the composite index on a map value field that includes map keys.
     *
     * @param targetType type containing the indexed field; may also be any super-type (e.g., an interface type),
     *  as long as {@code fieldName} is not ambiguous among all sub-types
     * @param fieldName name of the indexed field; must include {@code "value"} sub-field name (e.g., {@code "mymap.value"})
     * @param valueType the Java type corresponding to map values
     * @param keyType the Java type corresponding to map keys
     * @param <V> Java type corresponding to the indexed map's value field
     * @param <T> Java type containing the field
     * @param <K> Java type corresponding to the indexed map's key field
     * @return read-only, real-time view of map values, objects having that value in the map field, and corresponding map keys
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V, T, K> Index2<V, T, K> queryMapValueIndex(Class<T> targetType,
      String fieldName, Class<V> valueType, Class<K> keyType) {
        final IndexQueryInfo info = this.jdb.getIndexQueryInfo(
          new IndexQueryInfoKey(fieldName, false, targetType, valueType, keyType));
        if (!(info.indexInfo instanceof MapValueIndexInfo))
            throw new IllegalArgumentException("field `" + fieldName + "' is not a map value sub-field");
        final MapValueIndexInfo indexInfo = (MapValueIndexInfo)info.indexInfo;
        final CoreIndex2<?, ObjId, ?> index = info.applyFilters(this.tx.queryMapValueIndex(indexInfo.storageId));
        final Converter<?, ?> valueConverter = indexInfo.getConverter(this).reverse();
        final Converter<?, ?> keyConverter = indexInfo.getKeyConverter(this).reverse();
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex2(index, valueConverter, targetConverter, keyConverter);
    }

    /**
     * Access a composite index on two fields.
     *
     * @param targetType type containing the indexed fields; may also be any super-type (e.g., an interface type)
     * @param indexName the name of the composite index
     * @param value1Type the Java type corresponding to the first field value
     * @param value2Type the Java type corresponding to the second field value
     * @param <V1> Java type corresponding to the first indexed field
     * @param <V2> Java type corresponding to the second indexed field
     * @param <T> Java type containing the field
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V1, V2, T> Index2<V1, V2, T> queryCompositeIndex(Class<T> targetType,
      String indexName, Class<V1> value1Type, Class<V2> value2Type) {
        final IndexQueryInfo info = this.jdb.getIndexQueryInfo(
          new IndexQueryInfoKey(indexName, true, targetType, value1Type, value2Type));
        final CompositeIndexInfo indexInfo = (CompositeIndexInfo)info.indexInfo;
        final CoreIndex2<?, ?, ObjId> index = info.applyFilters(this.tx.queryCompositeIndex2(indexInfo.storageId));
        final Converter<?, ?> value1Converter = indexInfo.getConverter(this, 0).reverse();
        final Converter<?, ?> value2Converter = indexInfo.getConverter(this, 1).reverse();
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex2(index, value1Converter, value2Converter, targetConverter);
    }

    /**
     * Access a composite index on three fields.
     *
     * @param targetType type containing the indexed fields; may also be any super-type (e.g., an interface type)
     * @param indexName the name of the composite index
     * @param value1Type the Java type corresponding to the first field value
     * @param value2Type the Java type corresponding to the second field value
     * @param value3Type the Java type corresponding to the third field value
     * @param <V1> Java type corresponding to the first indexed field
     * @param <V2> Java type corresponding to the second indexed field
     * @param <V3> Java type corresponding to the third indexed field
     * @param <T> Java type containing the field
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V1, V2, V3, T> Index3<V1, V2, V3, T> queryCompositeIndex(Class<T> targetType,
      String indexName, Class<V1> value1Type, Class<V2> value2Type, Class<V3> value3Type) {
        final IndexQueryInfo info = this.jdb.getIndexQueryInfo(
          new IndexQueryInfoKey(indexName, true, targetType, value1Type, value2Type, value3Type));
        final CompositeIndexInfo indexInfo = (CompositeIndexInfo)info.indexInfo;
        final CoreIndex3<?, ?, ?, ObjId> index = info.applyFilters(this.tx.queryCompositeIndex3(indexInfo.storageId));
        final Converter<?, ?> value1Converter = indexInfo.getConverter(this, 0).reverse();
        final Converter<?, ?> value2Converter = indexInfo.getConverter(this, 1).reverse();
        final Converter<?, ?> value3Converter = indexInfo.getConverter(this, 2).reverse();
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex3(index, value1Converter, value2Converter, value3Converter, targetConverter);
    }

    /**
     * Access a composite index on four fields.
     *
     * @param targetType type containing the indexed fields; may also be any super-type (e.g., an interface type)
     * @param indexName the name of the composite index
     * @param value1Type the Java type corresponding to the first field value
     * @param value2Type the Java type corresponding to the second field value
     * @param value3Type the Java type corresponding to the third field value
     * @param value4Type the Java type corresponding to the fourth field value
     * @param <V1> Java type corresponding to the first indexed field
     * @param <V2> Java type corresponding to the second indexed field
     * @param <V3> Java type corresponding to the third indexed field
     * @param <V4> Java type corresponding to the fourth indexed field
     * @param <T> Java type containing the field
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V1, V2, V3, V4, T> Index4<V1, V2, V3, V4, T> queryCompositeIndex(Class<T> targetType,
      String indexName, Class<V1> value1Type, Class<V2> value2Type, Class<V3> value3Type, Class<V4> value4Type) {
        final IndexQueryInfo info = this.jdb.getIndexQueryInfo(
          new IndexQueryInfoKey(indexName, true, targetType, value1Type, value2Type, value3Type, value4Type));
        final CompositeIndexInfo indexInfo = (CompositeIndexInfo)info.indexInfo;
        final CoreIndex4<?, ?, ?, ?, ObjId> index = info.applyFilters(this.tx.queryCompositeIndex4(indexInfo.storageId));
        final Converter<?, ?> value1Converter = indexInfo.getConverter(this, 0).reverse();
        final Converter<?, ?> value2Converter = indexInfo.getConverter(this, 1).reverse();
        final Converter<?, ?> value3Converter = indexInfo.getConverter(this, 2).reverse();
        final Converter<?, ?> value4Converter = indexInfo.getConverter(this, 3).reverse();
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex4(index, value1Converter, value2Converter, value3Converter, value4Converter, targetConverter);
    }

    // COMPOSITE-INDEX

    /**
     * Query an index by storage ID. For storage ID's corresponding to simple fields, this method returns an
     * {@link Index}, except for list element and map value fields, for which an {@link Index2} is returned.
     * For storage ID's corresponding to composite indexes, this method returns an {@link Index2}, {@link Index3},
     * etc. as appropriate.
     *
     * <p>
     * This method exists mainly for the convenience of programmatic tools, etc.
     *
     * @param storageId indexed {@link JSimpleField}'s storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if {@code storageId} does not correspond to an indexed field or composite index
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object queryIndex(int storageId) {

        // Find index
        final IndexInfo indexInfo = this.jdb.indexInfoMap.get(storageId);
        if (indexInfo == null)
            throw new IllegalArgumentException("no composite index or simple indexed field exists with storage ID " + storageId);

        // Handle a composite index
        if (indexInfo instanceof CompositeIndexInfo) {
            final CompositeIndexInfo compositeInfo = (CompositeIndexInfo)indexInfo;
            switch (compositeInfo.getStorageIds().size()) {
            case 2:
            {
                final Converter<?, ?> value1Converter = compositeInfo.getConverter(this, 0).reverse();
                final Converter<?, ?> value2Converter = compositeInfo.getConverter(this, 1).reverse();
                return new ConvertedIndex2(this.tx.queryCompositeIndex2(indexInfo.storageId),
                  value1Converter, value2Converter, this.referenceConverter);
            }
            case 3:
            {
                final Converter<?, ?> value1Converter = compositeInfo.getConverter(this, 0).reverse();
                final Converter<?, ?> value2Converter = compositeInfo.getConverter(this, 1).reverse();
                final Converter<?, ?> value3Converter = compositeInfo.getConverter(this, 2).reverse();
                return new ConvertedIndex3(this.tx.queryCompositeIndex3(indexInfo.storageId),
                  value1Converter, value2Converter, value3Converter, this.referenceConverter);
            }
            case 4:
            {
                final Converter<?, ?> value1Converter = compositeInfo.getConverter(this, 0).reverse();
                final Converter<?, ?> value2Converter = compositeInfo.getConverter(this, 1).reverse();
                final Converter<?, ?> value3Converter = compositeInfo.getConverter(this, 2).reverse();
                final Converter<?, ?> value4Converter = compositeInfo.getConverter(this, 3).reverse();
                return new ConvertedIndex4(this.tx.queryCompositeIndex4(indexInfo.storageId),
                  value1Converter, value2Converter, value3Converter, value4Converter, this.referenceConverter);
            }
            // COMPOSITE-INDEX
            default:
                throw new RuntimeException("internal error");
            }
        }

        // Must be a simple field index
        return ((SimpleFieldIndexInfo)indexInfo).toIndex(this);
    }

// Transaction Lifecycle

    /**
     * Commit this transaction.
     *
     * <p>
     * Prior to actual commit, if this transaction was created with a validation mode other than {@link ValidationMode#DISABLED},
     * {@linkplain #validate validation} of outstanding objects in the validation queue is performed.
     *
     * <p>
     * If a {@link ValidationException} is thrown, the transaction is no longer usable. To perform validation and leave
     * the transaction open, invoke {@link #validate} prior to commit.
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws io.permazen.kv.RetryTransactionException from {@link io.permazen.kv.KVTransaction#commit KVTransaction.commit()}
     * @throws ValidationException if a validation error is detected
     * @throws IllegalStateException if this method is invoked re-entrantly from within a validation check
     */
    public synchronized void commit() {

        // Sanity check
        if (!this.tx.isValid())
            throw new StaleTransactionException(this.tx);
        synchronized (this) {
            if (this.commitInvoked)
                throw new IllegalStateException("commit() invoked re-entrantly");
            this.commitInvoked = true;
        }

        // Do validation
        try {
            this.validate();
        } catch (ValidationException e) {
            this.tx.rollback();
            throw e;
        }

        // Commit
        this.tx.commit();
    }

    /**
     * Roll back this transaction.
     *
     * <p>
     * This method may be invoked at any time, even after a previous invocation of
     * {@link #commit} or {@link #rollback}, in which case the invocation will be ignored.
     */
    public void rollback() {
        this.tx.rollback();
    }

    /**
     * Determine whether this transaction is still usable.
     *
     * @return true if this transaction is still valid
     * @see Transaction#isValid
     */
    public boolean isValid() {
        return this.tx.isValid();
    }

    /**
     * Perform validation checks on all objects currently in the validation queue.
     * This method may be called at any time prior to {@link #commit} to
     * process and clear the queue of validatable objects.
     *
     * <p>
     * If validation fails, validation stops, all remaining unvalidated objects are left on the validation queue,
     * and a {@link ValidationException} is thrown. The transaction will remain usable.
     *
     * <p>
     * <b>Note:</b> if the this transaction was created with {@link ValidationMode#DISABLED}, then this method does nothing.
     *
     * @throws io.permazen.kv.RetryTransactionException from {@link io.permazen.kv.KVTransaction#commit KVTransaction.commit()}
     * @throws ValidationException if a validation error is detected
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws StaleTransactionException if this transaction is no longer usable
     *
     * @see JObject#revalidate
     */
    public void validate() {

        // Sanity check
        if (!this.tx.isValid())
            throw new StaleTransactionException(this.tx);

        // Check validation mode
        if (this.validationMode == ValidationMode.DISABLED)
            return;

        // Do validation
        this.performAction(this::doValidate);
    }

    /**
     * Invoke the given {@link Runnable} with this instance as the {@linkplain #getCurrent current transaction}.
     *
     * <p>
     * If another instance is currently associated with the current thread, it is set aside for the duration of
     * {@code action}'s execution, and then restored when {@code action} is finished (regardless of outcome).
     *
     * @param action action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void performAction(Runnable action) {
        Preconditions.checkArgument(action != null, "null action");
        final JTransaction previous = CURRENT.get();
        CURRENT.set(this);
        try {
            action.run();
        } finally {
            CURRENT.set(previous);
        }
    }

    /**
     * Apply weaker transaction consistency while invoking the given {@link Runnable}, if applicable.
     *
     * <p>
     * If the the underlying {@link KVTransaction} does not implement {@link ReadTracking}, then this method
     * simply invokes {@code action}. Otherwise, it disables tracking of reads while performing {@code action},
     * which means some reads could return stale data.
     *
     * <p>
     * <b>This method is for experts only</b>; inappropriate use can result in a corrupted database. In particular,
     * you should not perform any writes based on information read with weak consistency. Note that this includes
     * the writes associated with implicit schema migration; therefore, you must ensure any objects accessed
     * do not need to be upgraded.
     *
     * <p>
     * There must be a {@linkplain #getCurrent current transaction} associated with the current thread.
     *
     * @param action action to perform
     * @throws IllegalStateException if there is no {@linkplain #getCurrent current transaction}
     * @throws IllegalArgumentException if {@code action} is null
     * @see ReadTracking
     */
    public static void weakConsistency(Runnable action) {
        Preconditions.checkArgument(action != null, "null action");
        final KVTransaction kvt = JTransaction.getCurrent().getTransaction().getKVTransaction();
        if (kvt instanceof ReadTracking) {
            final AtomicBoolean readTrackingControl = ((ReadTracking)kvt).getReadTrackingControl();
            final boolean previous = readTrackingControl.getAndSet(false);
            try {
                action.run();
            } finally {
                readTrackingControl.set(previous);
            }
        } else
            action.run();
    }

// Internal methods

    @SuppressWarnings("unchecked")
    private void doValidate() {
        final ValidatorFactory validatorFactory = this.jdb.getValidatorFactory();
        final Validator validator = validatorFactory != null ? validatorFactory.getValidator() : null;
        while (true) {

            // Pop next object to validate off the queue
            final ObjId id;
            final Class<?>[] validationGroups;
            synchronized (this) {
                final Map.Entry<ObjId, Class<?>[]> entry = this.validationQueue.removeOne();
                if (entry == null)
                    return;
                id = entry.getKey();
                validationGroups = entry.getValue();
                assert id != null;
                assert validationGroups != null;
            }

            // Does it still exist?
            if (!this.tx.exists(id))
                continue;

            // Get object and verify type exists in current schema (if not, the remaining validation is unneccessary)
            final JObject jobj = this.get(id);
            final JClass<?> jclass = this.jdb.jclasses.get(id.getStorageId());
            if (jclass == null)
                return;

            // Do JSR 303 validation if needed
            if (validator != null) {
                final Set<ConstraintViolation<JObject>> violations;
                try {
                    violations = new ValidationContext<JObject>(jobj, validationGroups).validate(validator);
                } catch (RuntimeException e) {
                    final Throwable rootCause = Throwables.getRootCause(e);
                    if (rootCause instanceof KVDatabaseException)
                        throw (KVDatabaseException)rootCause;
                    throw e;
                }
                if (!violations.isEmpty()) {
                    throw new ValidationException(jobj, violations, "validation error for object " + id + " of type `"
                      + this.jdb.jclasses.get(id.getStorageId()).name + "':\n" + ValidationUtil.describe(violations));
                }
            }

            // Do @OnValidate method validation
            for (OnValidateScanner<?>.MethodInfo info : jclass.onValidateMethods) {
                Class<?>[] methodGroups = info.getAnnotation().groups();
                if (methodGroups.length == 0)
                    methodGroups = DEFAULT_CLASS_ARRAY;
                if (Util.isAnyGroupBeingValidated(methodGroups, validationGroups))
                    Util.invoke(info.getMethod(), jobj);
            }

            // Do uniqueness validation
            if ((!jclass.uniqueConstraintFields.isEmpty() || !jclass.uniqueConstraintCompositeIndexes.isEmpty())
              && Util.isAnyGroupBeingValidated(DEFAULT_AND_UNIQUENESS_CLASS_ARRAY, validationGroups)) {

                // Check simple index uniqueness constraints
                for (JSimpleField jfield : jclass.uniqueConstraintFields) {
                    assert jfield.indexed;
                    assert jfield.unique;

                    // Get field's (core API) value
                    final Object value = this.tx.readSimpleField(id, jfield.storageId, false);

                    // Compare to excluded value list
                    if (jfield.uniqueExcludes != null
                      && Collections.binarySearch(jfield.uniqueExcludes, value, (Comparator<Object>)jfield.fieldType) >= 0)
                        continue;

                    // Query core API index to find other objects with the same value in the field, but restrict the search to
                    // only include those types having the annotated method, not some other method with the same name/storage ID.
                    final IndexQueryInfo info = this.jdb.getIndexQueryInfo(new IndexQueryInfoKey(jfield.name,
                      false, jfield.getter.getDeclaringClass(), jfield.typeToken.wrap().getRawType()));
                    final CoreIndex<?, ObjId> index = info.applyFilters(this.tx.queryIndex(jfield.storageId));

                    // Seach for other objects with the same value in the field and report violation if any are found
                    final List<ObjId> conflictors = this.findUniqueConflictors(id, index.asMap().get(value));
                    if (!conflictors.isEmpty()) {
                        throw new ValidationException(jobj, "uniqueness constraint on " + jfield + " failed for object "
                          + id + ": field value " + value + " is also shared by object(s) " + conflictors);
                    }
                }

                // Check composite index uniqueness constraints
                for (JCompositeIndex index : jclass.uniqueConstraintCompositeIndexes) {
                    assert index.unique;

                    // Get field (core API) values
                    final int numFields = index.jfields.size();
                    final List<Object> values = new ArrayList<>(numFields);
                    for (JSimpleField jfield : index.jfields)
                        values.add(this.tx.readSimpleField(id, jfield.storageId, false));

                    // Compare to excluded value combinations list
                    if (index.uniqueExcludes != null
                      && Collections.binarySearch(index.uniqueExcludes, values, index.uniqueComparator) >= 0)
                        continue;

                    // Query core API index to find all objects with the same values in the fields
                    final IndexQueryInfo info = this.jdb.getIndexQueryInfo(
                      new IndexQueryInfoKey(index.name, true, index.declaringType, index.getQueryInfoValueTypes()));
                    final CompositeIndexInfo indexInfo = (CompositeIndexInfo)info.indexInfo;
                    final NavigableSet<ObjId> ids;
                    switch (numFields) {
                    case 2:
                        final CoreIndex2<Object, Object, ObjId> coreIndex2
                          = (CoreIndex2<Object, Object, ObjId>)this.tx.queryCompositeIndex2(indexInfo.storageId);
                        ids = info.applyFilters(coreIndex2).asMap().get(new Tuple2<Object, Object>(values.get(0), values.get(1)));
                        break;
                    case 3:
                        final CoreIndex3<Object, Object, Object, ObjId> coreIndex3
                          = (CoreIndex3<Object, Object, Object, ObjId>)this.tx.queryCompositeIndex3(indexInfo.storageId);
                        ids = info.applyFilters(coreIndex3).asMap().get(
                          new Tuple3<Object, Object, Object>(values.get(0), values.get(1), values.get(2)));
                        break;
                    case 4:
                        final CoreIndex4<Object, Object, Object, Object, ObjId> coreIndex4
                          = (CoreIndex4<Object, Object, Object, Object, ObjId>)this.tx.queryCompositeIndex4(indexInfo.storageId);
                        ids = info.applyFilters(coreIndex4).asMap().get(
                          new Tuple4<Object, Object, Object, Object>(values.get(0), values.get(1), values.get(2), values.get(3)));
                        break;
                    // COMPOSITE-INDEX
                    default:
                        throw new RuntimeException("internal error");
                    }

                    // Seach for other objects with the same values in the same fields and report violation if any are found
                    final List<ObjId> conflictors = this.findUniqueConflictors(id, ids);
                    if (!conflictors.isEmpty()) {
                        throw new ValidationException(jobj, "uniqueness constraint on composite index `" + index.name
                          + "' failed for object " + id + ": field value combination " + values + " is also shared by object(s) "
                          + conflictors);
                    }
                }
            }
        }
    }

    private ArrayList<ObjId> findUniqueConflictors(ObjId id, NavigableSet<ObjId> ids) {
        final ArrayList<ObjId> conflictors = new ArrayList<>(MAX_UNIQUE_CONFLICTORS);
        for (ObjId conflictor : ids) {
            if (conflictor.equals(id))                          // ignore object's own index entry
                continue;
            conflictors.add(conflictor);
            if (conflictors.size() >= MAX_UNIQUE_CONFLICTORS)
                break;
        }
        return conflictors;
    }

// InternalCreateListener

    private static class InternalCreateListener implements CreateListener {

        @Override
        public void onCreate(Transaction tx, ObjId id) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            jtx.doOnCreate(id);
        }
    }

    private void doOnCreate(ObjId id) {

        // Get JClass, if known
        final JClass<?> jclass;
        try {
            jclass = this.jdb.getJClass(id);
        } catch (TypeNotInSchemaVersionException e) {
            return;                                             // object type does not exist in our schema
        }

        // Enqueue for revalidation
        if (this.validationMode == ValidationMode.AUTOMATIC && jclass.requiresDefaultValidation)
            this.revalidate(Collections.singleton(id));

        // Notify @OnCreate methods
        Object jobj = null;
        for (OnCreateScanner<?>.MethodInfo info : jclass.onCreateMethods) {
            if (this.isSnapshot() && !info.getAnnotation().snapshotTransactions())
                continue;
            if (jobj == null)
                jobj = this.get(id);
            Util.invoke(info.getMethod(), jobj);
        }
    }

// InternalDeleteListener

    private static class InternalDeleteListener implements DeleteListener {

        @Override
        public void onDelete(Transaction tx, ObjId id) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            jtx.doOnDelete(id);
        }
    }

    private void doOnDelete(ObjId id) {

        // Get JClass, if known
        final JClass<?> jclass;
        try {
            jclass = this.jdb.getJClass(id);
        } catch (TypeNotInSchemaVersionException e) {
            return;                                             // object type does not exist in our schema
        }

        // Notify @OnDelete methods
        Object jobj = null;
        for (OnDeleteScanner<?>.MethodInfo info : jclass.onDeleteMethods) {
            if (this.isSnapshot() && !info.getAnnotation().snapshotTransactions())
                continue;
            if (jobj == null)
                jobj = this.get(id);
            Util.invoke(info.getMethod(), jobj);
        }
    }

// InternalVersionChangeListener

    private static class InternalVersionChangeListener implements VersionChangeListener {

        @Override
        public void onVersionChange(Transaction tx, ObjId id, int oldVersion, int newVersion, Map<Integer, Object> oldValues) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            jtx.doOnVersionChange(id, oldVersion, newVersion, oldValues);
        }
    }

    private void doOnVersionChange(ObjId id, int oldVersion, int newVersion, Map<Integer, Object> oldValues) {

        // Get JClass, if known
        final JClass<?> jclass;
        try {
            jclass = this.jdb.getJClass(id);
        } catch (TypeNotInSchemaVersionException e) {
            return;                                             // object type does not exist in our schema
        }

        // Enqueue for revalidation
        if (this.validationMode == ValidationMode.AUTOMATIC && jclass.requiresDefaultValidation)
            this.revalidate(Collections.singleton(id));

        // Skip the rest if there are no fields to convert and no @OnChange methods
        if (jclass.upgradeConversionFields.isEmpty() && jclass.onVersionChangeMethods.isEmpty())
            return;

        // Get old and new object type info
        final Schema oldSchema = this.tx.getSchemas().getVersion(oldVersion);
        final Schema newSchema = this.tx.getSchema();
        final ObjType oldObjType = oldSchema.getObjType(id.getStorageId());
        final ObjType newObjType = newSchema.getObjType(id.getStorageId());

        // Auto-convert any upgrade-convertable fields
        for (JField jfield0 : jclass.upgradeConversionFields) {
            final int storageId = jfield0.getStorageId();

            // Find the old version of the field, if any
            final Field<?> oldField0;
            try {
                oldField0 = oldObjType.getField(storageId);
            } catch (UnknownFieldException e) {
                continue;
            }

            // Get old field value
            final Object oldValue = oldValues.get(storageId);

            // Handle conversion to counter field from counter field or simple numeric field
            if (jfield0 instanceof JCounterField) {

                // Get new counter field
                final JCounterField jfield = (JCounterField)jfield0;
                assert jfield.upgradeConversion.isConvertsValues();

                // Handle trivial conversion from counter -> counter
                if (oldField0 instanceof CounterField)
                    continue;

                // Handle conversion from numeric simple -> counter
                if (oldField0 instanceof SimpleField) {
                    final SimpleField<?> oldField = (SimpleField)oldField0;
                    final FieldType<?> oldFieldType = oldField.getFieldType();
                    if (Number.class.isAssignableFrom(oldFieldType.getTypeToken().wrap().getRawType())) {
                        final Number value = (Number)oldValue;
                        if (value != null)
                            this.tx.writeCounterField(id, storageId, value.longValue(), false);
                        continue;
                    }
                }

                // Conversion is not possible
                if (jfield.upgradeConversion.isRequireConversion()) {
                    throw new UpgradeConversionException(id, storageId, "conversion from the previous schema version's "
                      + (oldValue != null ? "non-numeric " : "null ") + oldField0 + " to " + jfield
                      + " is not supported, but the upgrade conversion policy is configured as " + jfield.upgradeConversion);
                }
                continue;
            }

            // Get new field, which must be simple
            final JSimpleField jfield = (JSimpleField)jfield0;
            assert jfield.upgradeConversion.isConvertsValues();
            final SimpleField<?> newField = (SimpleField)newObjType.getField(storageId);

            // Handle conversion from counter field to numeric simple field
            if (oldField0 instanceof CounterField) {
                this.doConvertAndSetField(id, oldField0,
                  this.tx.getDatabase().getFieldTypeRegistry().getFieldType(TypeToken.of(long.class)),
                  newField, oldValue, jfield.upgradeConversion);
                continue;
            }

            // If the old field is not a simple field, we can't convert
            if (!(oldField0 instanceof SimpleField))
                continue;
            final SimpleField<?> oldField = (SimpleField<?>)oldField0;

            // Convert the old field value and update the field
            this.convertAndSetField(id, oldField, newField, oldValue, jfield.upgradeConversion);
        }

        // Skip the rest if there are no @OnChange methods
        if (jclass.onVersionChangeMethods.isEmpty())
            return;

        // The object that was upgraded
        JObject jobj = null;

        // Convert old field values from core API objects to JDB layer objects, but do not convert EnumValue objects
        final Map<Integer, Object> oldValuesByStorageId = Maps.transformEntries(oldValues,
          (storageId, oldValue) -> this.convertOldVersionValue(id, oldObjType.getField(storageId), oldValue));

        // Build alternate version of old values map that is keyed by field name instead of storage ID
        final Map<String, Object> oldValuesByName = Maps.transformValues(oldObjType.getFieldsByName(),
          field -> oldValuesByStorageId.get(field.getStorageId()));

        // Invoke listener methods
        for (OnVersionChangeScanner<?>.MethodInfo info0 : jclass.onVersionChangeMethods) {
            final OnVersionChangeScanner<?>.VersionChangeMethodInfo info = (OnVersionChangeScanner<?>.VersionChangeMethodInfo)info0;

            // Get Java model object
            if (jobj == null)
                jobj = this.get(id);

            // Invoke method
            info.invoke(jobj, oldVersion, newVersion, oldValuesByStorageId, oldValuesByName);
        }
    }

    private <OT, NT> void convertAndSetField(ObjId id, SimpleField<OT> oldField,
      SimpleField<NT> newField, Object oldValue, UpgradeConversionPolicy policy) {
        assert policy.isConvertsValues();

        // If the old and new field types are equal, there's nothing to do
        final FieldType<OT> oldFieldType = oldField.getFieldType();
        final FieldType<NT> newFieldType = newField.getFieldType();
        if (newFieldType.equals(oldFieldType))
            return;

        // Perform conversion
        this.doConvertAndSetField(id, oldField, oldFieldType, newField, oldValue, policy);
    }

    private <OT, NT> void doConvertAndSetField(ObjId id, Field<?> oldField,
      FieldType<OT> oldFieldType, SimpleField<NT> newField, Object oldValue0, UpgradeConversionPolicy policy) {

        // Validate old value
        final OT oldValue = oldFieldType.validate(oldValue0);

        // Get the new field type
        final FieldType<NT> newFieldType = newField.getFieldType();

        // Attempt conversion
        final NT newValue;
        try {
            newValue = newFieldType.convert(oldFieldType, oldValue);
        } catch (IllegalArgumentException e) {
            if (policy.isRequireConversion()) {
                throw new UpgradeConversionException(id, newField.getStorageId(), "the value " + oldFieldType.toString(oldValue)
                  + " in the previous schema version's " + oldField + " could not be converted type to the new field type "
                  + newFieldType + ", but the upgrade conversion policy is configured as " + policy, e);
            }
            return;
        }

        // Update field
        newField.setValue(this.tx, id, newValue);
    }

// Convert methods

    @SuppressWarnings("unchecked")
    private <X, Y> Y convert(Converter<X, Y> converter, Object value) {
        return converter != null ? converter.convert((X)value) : (Y)value;
    }

    /**
     * Convert a core API field value from a different schema version.
     * We need to do this to convert the old field values after a schema upgrade.
     *
     * @param id originating object
     * @param field originating core API field
     * @param value field value
     * @return converted value
     */
    private Object convertOldVersionValue(ObjId id, Field<?> field, Object value) {

        // Null always converts to null
        if (value == null)
            return null;

        // Convert the value
        return this.convert(field.visit(new OldVersionValueConverterBuilder()), value);
    }

// OldVersionValueConverterBuilder

    /**
     * Builds a {@link Converter} for core API {@link Field} that converts, in the forward direction, core API values
     * into {@link Permazen} values, based only on the core API {@link Field}. That means we don't convert
     * {@link io.permazen.core.EnumValue}s. In the case of reference fields, the original Java type may no
     * longer be available; such values are converted to {@link UntypedJObject}.
     *
     * <p>
     * Returns null if no conversion is necessary.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private class OldVersionValueConverterBuilder extends FieldSwitchAdapter<Converter<?, ?>> {

        @Override
        public Converter<?, ?> caseReferenceField(ReferenceField field) {
            return JTransaction.this.referenceConverter.reverse();
        }

        @Override
        public Converter<?, ?> caseSimpleField(SimpleField field) {
            return null;
        }

        @Override
        public <E> Converter<?, ?> caseSetField(SetField<E> field) {
            final Converter elementConverter = field.getElementField().visit(this);
            return elementConverter != null ? new NavigableSetConverter(elementConverter) : null;
        }

        @Override
        public <E> Converter<?, ?> caseListField(ListField<E> field) {
            final Converter elementConverter = field.getElementField().visit(this);
            return elementConverter != null ? new ListConverter(elementConverter) : null;
        }

        @Override
        public <K, V> Converter<?, ?> caseMapField(MapField<K, V> field) {
            Converter keyConverter = field.getKeyField().visit(this);
            Converter valueConverter = field.getValueField().visit(this);
            if (keyConverter != null || valueConverter != null) {
                if (keyConverter == null)
                    keyConverter = Converter.identity();
                if (valueConverter == null)
                    valueConverter = Converter.identity();
                return new NavigableMapConverter(keyConverter, valueConverter);
            }
            return null;
        }

        @Override
        public Converter<?, ?> caseCounterField(CounterField field) {
            return null;
        }
    }

// DefaultValidationListener

    private static class DefaultValidationListener implements AllChangesListener {

    // SimpleFieldChangeListener

        @Override
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          SimpleField<T> field, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

    // SetFieldChangeListener

        @Override
        public <E> void onSetFieldAdd(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public <E> void onSetFieldRemove(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

    // ListFieldChangeListener

        @Override
        public <E> void onListFieldAdd(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public <E> void onListFieldRemove(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public <E> void onListFieldReplace(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

    // MapFieldChangeListener

        @Override
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            this.revalidateIfNeeded(tx, id, field, referrers);
        }

    // Internal methods

        private void revalidateIfNeeded(Transaction tx, ObjId id, Field<?> field, NavigableSet<ObjId> referrers) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JField jfield = jtx.jdb.getJField(id, field.getStorageId(), JField.class);
            if (jfield.requiresDefaultValidation)
                jtx.revalidate(referrers);
        }
    }
}

