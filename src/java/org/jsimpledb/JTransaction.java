
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.validation.ConstraintViolation;

import org.dellroad.stuff.validation.ValidationUtil;
import org.jsimpledb.annotation.OnVersionChange;
import org.jsimpledb.core.CreateListener;
import org.jsimpledb.core.DeleteListener;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldSwitchAdapter;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferenceField;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.StaleTransactionException;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.VersionChangeListener;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.jsimpledb.util.ConvertedNavigableSet;
import org.jsimpledb.util.NavigableSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transaction associated with a {@link JSimpleDB} instance.
 *
 * <p>
 * Commonly used methods in this class can be divided into the following categories:
 * </p>
 *
 * <p>
 * <b>Transaction Meta-Data</b>
 * <ul>
 *  <li>{@link #getJSimpleDB getJSimpleDB()} - Get the associated {@link JSimpleDB} instance</li>
 *  <li>{@link #getTransaction} - Get the core API {@link Transaction} underlying this instance</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>Transaction Lifecycle</b>
 * <ul>
 *  <li>{@link #commit commit()} - Commit transaction</li>
 *  <li>{@link #rollback rollback()} - Roll back transaction</li>
 *  <li>{@link #getCurrent getCurrent()} - Get the {@link JTransaction} instance associated with the current thread</li>
 *  <li>{@link #setCurrent setCurrent()} - Set the {@link JTransaction} instance associated with the current thread</li>
 *  <li>{@link #isValid isValid()} - Test transaction validity</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>Object Access</b>
 * <ul>
 *  <li>{@link #getJObject(ObjId) getJObject()} - Get the Java model object corresponding to a specific database object ID</li>
 *  <li>{@link #create(Class) create()} - Create a new database object</li>
 *  <li>{@link #getAll getAll()} - Get all database objects that are instances of a given Java type</li>
 *  <li>{@link #queryVersion queryVersion()} - Get database objects grouped according to their schema versions</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>Validation</b>
 * <ul>
 *  <li>{@link #validate validate()} - Validate all objects in the validation queue</li>
 *  <li>{@link #resetValidationQueue} - Clear the validation queue</li>
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
 * <b>Snapshot Transactions</b>
 * <ul>
 *  <li>{@link #getSnapshotTransaction getSnapshotTransaction()} - Get the default {@link SnapshotJTransaction}</li>
 *  <li>{@link #copyTo(JTransaction, ObjId, ObjId, String[]) copyTo()} - Copy an object
 *      and its related objects into another transaction</li>
 *  <li>{@link #copyTo(JTransaction, Iterable) copyTo()} - Copy explicitly specified objects into another transaction</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>Lower Layer Access</b>
 * <ul>
 *  <li>{@link #getKey(JObject) getKey()} - Get the {@link org.jsimpledb.kv.KVDatabase} key prefix for a specific object</li>
 *  <li>{@link #getKey(JObject, String) getKey()} - Get the {@link org.jsimpledb.kv.KVDatabase}
 *      key for a specific field in a specific object</li>
 * </ul>
 * </p>
 *
 * <p>
 * The remaining methods in this class are normally only used by generated Java model object subclasses.
 * Instead of using these methods directly, using the appropriately annotated Java model object method
 * or {@link JObject} interface method is recommended.
 * </p>
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
 *  <li>{@link #querySimpleField querySimpleField()} - Query a simple field index</li>
 *  <li>{@link #queryListFieldEntries queryListFieldEntries()} - Query a list field entry index</li>
 *  <li>{@link #queryMapFieldKeyEntries queryMapFieldKeyEntries()} - Query a map field key entry index</li>
 *  <li>{@link #queryMapFieldKeyEntries queryMapFieldKeyEntries()} - Query a map field value entry index</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>{@link JObject} Methods</b>
 * <ul>
 *  <li>{@link #delete delete()} - Delete an object from this transaction</li>
 *  <li>{@link #exists exists()} - Test whether an object exists in this transaction</li>
 *  <li>{@link #recreate recreate()} - Recreate an object in this transaction</li>
 *  <li>{@link #revalidate revalidate()} - Add an object to the validation queue</li>
 *  <li>{@link #getSchemaVersion getSchemaVersion()} - Get this schema version of an object</li>
 *  <li>{@link #updateSchemaVersion updateSchemaVersion()} - Update an object's schema version</li>
 * </ul>
 * </p>
 */
public class JTransaction {

    private static final ThreadLocal<JTransaction> CURRENT = new ThreadLocal<>();

    final Logger log = LoggerFactory.getLogger(this.getClass());
    final ReferenceConverter referenceConverter = new ReferenceConverter(this);

    final JSimpleDB jdb;
    final Transaction tx;

    private final ValidationMode validationMode;
    private final ValidationListener validationListener = new ValidationListener();
    private final InternalCreateListener internalCreateListener = new InternalCreateListener();
    private final InternalDeleteListener internalDeleteListener = new InternalDeleteListener();
    private final InternalVersionChangeListener internalVersionChangeListener = new InternalVersionChangeListener();
    private final HashSet<ObjId> validationQueue = new HashSet<ObjId>(); // TODO: use a more efficient data structure for longs

    private SnapshotJTransaction snapshotTransaction;
    private boolean committing;

// Constructor

    /**
     * Constructor.
     *
     * @throws IllegalArgumentException if any parameter is null
     */
    JTransaction(JSimpleDB jdb, Transaction tx, ValidationMode validationMode) {

        // Initialization
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        if (validationMode == null)
            throw new IllegalArgumentException("null validationMode");
        this.jdb = jdb;
        this.tx = tx;
        this.validationMode = validationMode;

        // Register listeners
        this.tx.addCreateListener(this.internalCreateListener);
        this.tx.addDeleteListener(this.internalDeleteListener);
        this.tx.addVersionChangeListener(this.internalVersionChangeListener);
        for (JClass<?> jclass : this.jdb.jclasses.values()) {
            for (OnChangeScanner<?>.MethodInfo info : jclass.onChangeMethods) {
                final OnChangeScanner<?>.ChangeMethodInfo changeInfo = (OnChangeScanner<?>.ChangeMethodInfo)info;
                changeInfo.registerChangeListener(this);
            }
        }
        if (validationMode == ValidationMode.AUTOMATIC) {
            for (JClass<?> jclass : this.jdb.jclasses.values()) {
                for (JField jfield : jclass.jfields.values()) {
                    if (jfield.requiresValidation)
                        jfield.registerChangeListener(this.tx, new int[0], this.validationListener);
                }
            }
        }
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
     */
    public static void setCurrent(JTransaction jtransaction) {
        CURRENT.set(jtransaction);
    }

// Accessors

    /**
     * Get the {@link JSimpleDB} associated with this instance.
     */
    public JSimpleDB getJSimpleDB() {
        return this.jdb;
    }

    /**
     * Get the {@link Transaction} associated with this instance.
     */
    public Transaction getTransaction() {
        return this.tx;
    }

    /**
     * Get all instances of the given {@link JClass} (or any sub-type). The ordering of the returned set is based on the object IDs.
     *
     * @param jclass Java model type
     * @return read-only view of all instances of {@code type}
     * @throws IllegalArgumentException if {@code jclass} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> getAll(JClass<T> jclass) {
        if (jclass == null)
            throw new IllegalArgumentException("null jclass");
        return this.getAll(jclass.getTypeToken());
    }

    /**
     * Get all instances of the given type (or any sub-type). The ordering of the returned set is based on the object IDs.
     *
     * @param type any Java type, or null to get all objects
     * @return read-only view of all instances of {@code type}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> getAll(Class<T> type) {
        return this.getAll(type != null ? TypeToken.of(type) : (TypeToken<T>)null);
    }

    /**
     * Get all instances of the given type (or any sub-type). The ordering of the returned set is based on the object IDs.
     *
     * @param type any Java type, or null to get all objects
     * @return read-only view of all instances of {@code type}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> getAll(TypeToken<T> type) {
        final List<NavigableSet<ObjId>> sets = Lists.transform(this.jdb.getJClasses(type),
          new Function<JClass<? extends T>, NavigableSet<ObjId>>() {
            @Override
            public NavigableSet<ObjId> apply(JClass<? extends T> jclass) {
                return JTransaction.this.tx.getAll(jclass.storageId);
            }
        });
        return sets.isEmpty() ? NavigableSets.<T>empty() :
          (NavigableSet<T>)new ConvertedNavigableSet<JObject, ObjId>(NavigableSets.union(sets), this.referenceConverter);
    }

    /**
     * Get all instances having exactly the given type. The ordering of the returned set is based on the object IDs.
     *
     * @param jclass object {@link JClass}
     * @return read-only view of all instances having exactly type {@code jclass}
     * @throws IllegalArgumentException if {@code jclass} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> getAllOfType(JClass<T> jclass) {
        if (jclass == null)
            throw new IllegalArgumentException("null jclass");
        return (NavigableSet<T>)new ConvertedNavigableSet<JObject, ObjId>(
          JTransaction.this.tx.getAll(jclass.storageId), this.referenceConverter);
    }

    /**
     * Get all database objects grouped according to their schema versions.
     *
     * <p>
     * This returns the map returned by {@link Transaction#queryVersion} with {@link ObjId}s converted into {@link JObject}s.
     * </p>
     *
     * @param type type restriction for the returned objects, or null to not restrict indexed object type
     */
    public NavigableMap<Integer, NavigableSet<JObject>> queryVersion(Class<?> type) {
        final NavigableMap<Integer, NavigableSet<ObjId>> map = this.tx.queryVersion();
        return new ConvertedNavigableMap<Integer, NavigableSet<JObject>, Integer, NavigableSet<ObjId>>(
          this.tx.queryVersion(this.getTypeStorageIds(type)), Converter.<Integer>identity(),
          new NavigableSetConverter<JObject, ObjId>(this.referenceConverter));
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>Objects utilize mutiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link org.jsimpledb.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     * </p>
     *
     * @param jobj Java model object
     * @return the {@link org.jsimpledb.kv.KVDatabase} key corresponding to {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public byte[] getKey(JObject jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        return this.tx.getKey(jobj.getObjId());
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified field in the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>Complex fields utilize mutiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link org.jsimpledb.kv.KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     * </p>
     *
     * @param jobj Java model object
     * @param fieldName the name of a field in {@code jobj}'s type
     * @return the {@link org.jsimpledb.kv.KVDatabase} key of the field in the specified object
     * @throws IllegalArgumentException if {@code jobj} does not contain the specified field
     * @throws IllegalArgumentException if {@code fieldName} is otherwise invalid
     * @throws IllegalArgumentException if either parameter is null
     */
    public byte[] getKey(JObject jobj, String fieldName) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        final TypeToken<?> startType = this.jdb.getJClass(jobj.getObjId().getStorageId()).typeToken;
        final ReferencePath refPath = this.jdb.parseReferencePath(startType, fieldName, false);
        if (refPath.getReferenceFields().length > 0)
            throw new IllegalArgumentException("invalid field `" + fieldName + "'");
        if (!refPath.targetType.getRawType().isInstance(jobj))
            throw new IllegalArgumentException("jobj is not an instance of " + refPath.targetType); // should never happen
        return this.tx.getKey(jobj.getObjId(), refPath.targetField.storageId);
    }

// Snapshots

    /**
     * Get the default {@link SnapshotJTransaction} associated with this instance.
     *
     * @see JObject#copyOut JObject.copyOut()
     */
    public synchronized SnapshotJTransaction getSnapshotTransaction() {
        if (this.snapshotTransaction == null)
            this.snapshotTransaction = new SnapshotJTransaction(this, ValidationMode.AUTOMATIC);
        return this.snapshotTransaction;
    }

    /**
     * Copy the specified object into the specified destination transaction. The destination object will
     * be created if necessary. {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnChange} notifications will be delivered accordingly.
     *
     * <p>
     * Circular references are handled properly: if an object is encountered more than once, it is not copied again.
     * Does nothing if this instance and {@code dest} are the same instance and {@code srcId} and {@code dstId} are the same.
     * </p>
     *
     * <p>
     * This instance and {@code dest} must be compatible in that for any schema versions encountered, those schema versions
     * must be identical in both transactions.
     * </p>
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     * </p>
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#copyIn}, {@link JObject#copyOut},
     * or {@link JObject#copyTo} would be used instead.
     * </p>
     *
     * @param dest destination transaction
     * @param srcId source object ID
     * @param dstId target object ID, or null for same as {@code srcId}
     * @param refPaths zero or more reference paths that refer to additional objects to be copied
     * @return the copied object, i.e., the object having ID {@code dstId} in {@code dest}
     * @throws DeletedObjectException if {@code srcId} does not exist in this transaction
     * @throws org.jsimpledb.core.SchemaMismatchException if the schema corresponding to {@code srcId}'s object's version
     *  is not identical in this instance and {@code dest} (as well for any referenced objects)
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws ReadOnlyTransactionException if {@code dest}'s underlying transaction
     *  is {@linkplain Transaction#setReadOnly set read-only}
     * @throws IllegalArgumentException if any path in {@code refPaths} is invalid
     * @throws IllegalArgumentException if any parameter is null
     * @see JObject#copyTo JObject.copyTo()
     * @see JObject#copyOut JObject.copyOut()
     * @see JObject#copyIn JObject.copyIn()
     * @see #copyTo(JTransaction, Iterable)
     */
    public JObject copyTo(JTransaction dest, ObjId srcId, ObjId dstId, String... refPaths) {

        // Sanity check
        if (dest == null)
            throw new IllegalArgumentException("null destination transaction");
        if (srcId == null)
            throw new IllegalArgumentException("null srcId");
        if (dstId == null)
            dstId = srcId;

        // Check trivial case
        if (this.tx == dest.tx && srcId.equals(dstId))
            return dest.getJObject(dstId);

        // Copy "related objects" if a null reference path is given
        final Iterable<? extends JObject> relatedObjects = refPaths == null ? this.getJObject(srcId).getRelatedObjects() : null;
        if (refPaths == null)
            refPaths = new String[0];

        // Parse paths
        final TypeToken<?> startType = this.jdb.getJClass(srcId.getStorageId()).typeToken;
        final HashSet<ReferencePath> paths = new HashSet<>(refPaths.length);
        for (String refPath : refPaths) {

            // Parse refernce path
            if (refPath == null)
                throw new IllegalArgumentException("null refPath");
            final ReferencePath path = this.jdb.parseReferencePath(startType, refPath, null);

            // Verify target field is a reference field; convert a complex target field into its reference sub-field(s)
            final String lastFieldName = refPath.substring(refPath.lastIndexOf('.') + 1);
            final JField targetField = this.jdb.jfields.get(path.getTargetField());
            if (targetField instanceof JComplexField) {
                final JComplexField superField = (JComplexField)targetField;
                boolean foundReferenceSubField = false;
                for (JSimpleField subField : superField.getSubFields()) {
                    if (subField instanceof JReferenceField) {
                        paths.add(this.jdb.parseReferencePath(startType,
                          refPath + "." + superField.getSubFieldName(subField), true));
                        foundReferenceSubField = true;
                    }
                }
                if (!foundReferenceSubField) {
                    throw new IllegalArgumentException("the last field `" + lastFieldName
                      + "' of path `" + refPath + "' does not contain any reference sub-fields");
                }
            } else {
                if (!(targetField instanceof JReferenceField)) {
                    throw new IllegalArgumentException("the last field `" + lastFieldName
                      + "' of path `" + path + "' is not a reference field");
                }
                paths.add(path);
            }
        }

        // Ensure object is copied even when there are zero reference paths
        final HashSet<ObjId> seen = new HashSet<>();
        this.copyTo(seen, dest, srcId, dstId, true, new ArrayDeque<JReferenceField>());

        // Copy related objects (if any) if refPaths was null
        if (relatedObjects != null) {
            for (JObject jobj : Iterables.filter(relatedObjects, JObject.class)) {
                final ObjId id = jobj.getObjId();
                this.copyTo(seen, dest, id, id, false, new ArrayDeque<JReferenceField>());
            }
        }

        // Recurse over each reference path
        for (ReferencePath path : paths) {
            final int[] storageIds = path.getReferenceFields();

            // Convert reference path, including final target field, into a list of JReferenceFields
            final ArrayDeque<JReferenceField> fields = new ArrayDeque<>(storageIds.length + 1);
            for (int storageId : storageIds)
                fields.add((JReferenceField)this.jdb.jfields.get(storageId));
            fields.add((JReferenceField)this.jdb.jfields.get(path.getTargetField()));

            // Recurse over this path
            this.copyTo(seen, dest, srcId, dstId, false/*doesn't matter*/, fields);
        }

        // Done
        return dest.getJObject(dstId);
    }

    /**
     * Copy the objects in the specified {@link Iterable} into the specified destination transaction.
     * Destination objects will be created if necessary, and {@link org.jsimpledb.annotation.OnCreate &#64;OnCreate} and
     * {@link org.jsimpledb.annotation.OnCreate &#64;OnChange} notifications will be delivered accordingly.
     *
     * <p>
     * If an object is encountered more than once, it is not copied again.
     * Does nothing if this instance and {@code dest} are the same instance.
     * </p>
     *
     * <p>
     * This instance and {@code dest} must be compatible in that for any schema versions encountered, those schema versions
     * must be identical in both transactions.
     * </p>
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     * </p>
     *
     * @param dest destination transaction
     * @param objIds {@link Iterable} returning the object ID's of the objects to copy; null values are ignored
     * @throws DeletedObjectException if an object ID in {@code objIds} does not exist in this transaction
     * @throws org.jsimpledb.core.SchemaMismatchException if the schema corresponding to an object ID in
     *  {@code objId}'s object's version is not identical in this instance and {@code dest}
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws ReadOnlyTransactionException if {@code dest}'s underlying transaction
     *  is {@linkplain Transaction#setReadOnly set read-only}
     * @throws IllegalArgumentException if {@code dest} or {@code objIds} is null
     * @see #copyTo(JTransaction, ObjId, ObjId, String[])
     */
    public void copyTo(JTransaction dest, Iterable<? extends ObjId> objIds) {

        // Sanity check
        if (dest == null)
            throw new IllegalArgumentException("null dest");
        if (objIds == null)
            throw new IllegalArgumentException("null objIds");

        // Check trivial case
        if (this.tx == dest.tx)
            return;

        // Copy objects
        final HashSet<ObjId> seen = new HashSet<>();
        final ArrayDeque<JReferenceField> emptyFields = new ArrayDeque<>();
        for (ObjId id : objIds) {
            if (id != null)
                this.copyTo(seen, dest, id, id, true, emptyFields);
        }
    }

    void copyTo(Set<ObjId> seen, JTransaction dest, ObjId srcId, ObjId dstId, boolean required, Deque<JReferenceField> fields) {

        // Already copied this object?
        if (!seen.add(dstId))
            return;

        // Copy current instance
        try {
            this.tx.copy(srcId, dstId, dest.tx);
        } catch (DeletedObjectException e) {
            if (required)
                throw e;
        }

        // Recurse through the next reference field in the path
        if (fields.isEmpty())
            return;
        final JReferenceField jfield = fields.removeFirst();
        if (jfield.parent instanceof JComplexField) {
            final JComplexField superField = (JComplexField)jfield.parent;
            superField.copyRecurse(seen, this, dest, srcId, jfield, fields);
        } else {
            assert jfield instanceof JReferenceField;
            final ObjId referrent = (ObjId)this.tx.readSimpleField(srcId, jfield.storageId, false);
            if (referrent != null)
                this.copyTo(seen, dest, referrent, referrent, false, fields);
        }
    }

// Object/Field Access

    /**
     * Get the Java model object with the given object ID and whose state derives from this transaction.
     *
     * <p>
     * Note that while a non-null object is always returned, the corresponding object may not exist in this transaction.
     * If not, attempts to access its fields will throw {@link DeletedObjectException}.
     * </p>
     *
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     * @throws UnknownTypeException if no Java model class corresponding to {@code id} exists in the schema
     *  associated with this instance's {@link JSimpleDB}
     * @see #getJObject(ObjId, Class)
     * @see #getJObject(JObject)
     * @see JSimpleDB#getJObject JSimpleDB.getJObject()
     */
    public JObject getJObject(ObjId id) {
        return this.jdb.getJObject(id);
    }

    /**
     * Get the Java model object with the given object ID and whose state derives from this transaction, cast to the given type.
     *
     * @param id object ID
     * @return Java model object
     * @throws UnknownTypeException if no Java model class corresponding to {@code id} exists in the schema
     *  associated with this instance's {@link JSimpleDB}
     * @throws ClassCastException if the Java model object does not have type {@code type}
     * @throws IllegalArgumentException if {@code id} or {@code type} is null
     * @see #getJObject(ObjId)
     * @see #getJObject(JObject)
     */
    public <T> T getJObject(ObjId id, Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        return type.cast(this.getJObject(id));
    }

    /**
     * Get the Java model object with the same object ID as the given object and whose state derives from this transaction.
     * This method is equivalent to {@code getJObject(jobj.getObjId())} followed by an appropriate cast to type {@code T}.
     *
     * @param jobj Java model object
     * @return Java model object in this transaction with the same object ID (possibly {@code jobj} itself)
     * @throws UnknownTypeException if no Java model class corresponding to {@code jobj}'s object ID exists in the schema
     *  associated with this instance's {@link JSimpleDB}
     * @throws IllegalArgumentException if {@code jobj} is null
     * @throws ClassCastException if the Java model object in this transaction somehow does not have the same type as {@code jobj}
     * @see #getJObject(ObjId)
     * @see #getJObject(ObjId, Class)
     */
    @SuppressWarnings("unchecked")
    public <T extends JObject> T getJObject(T jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        return (T)jobj.getClass().cast(this.getJObject(jobj.getObjId()));
    }

    /**
     * Create a new instance of the given model class in this transaction.
     *
     * @param type an annotated Java object model type
     * @return newly created instance
     * @throws IllegalArgumentException if {@code type} is not a known Java object model type
     * @throws ReadOnlyTransactionException if the underlying transaction is {@linkplain Transaction#setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public <T> T create(Class<T> type) {
        return this.create(this.jdb.getJClass(TypeToken.of(type)));
    }

    /**
     * Create a new instance of the given type in this transaction.
     *
     * @param jclass object type
     * @return newly created instance
     * @throws IllegalArgumentException if {@code jclass} is not valid for this instance
     * @throws ReadOnlyTransactionException if the underlying transaction is {@linkplain Transaction#setReadOnly set read-only}
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings("unchecked")
    public <T> T create(JClass<T> jclass) {
        final ObjId id = this.tx.create(jclass.storageId);
        return (T)jclass.getTypeToken().getRawType().cast(this.getJObject(id));
    }

    /**
     * Delete the object with the given object ID in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#delete} would be used instead.
     * </p>
     *
     * @param jobj the object to delete
     * @return true if object was found and deleted, false if object was not found
     * @throws ReferencedObjectException if the object is referenced by some other object
     *  through a reference field configured for {@link org.jsimpledb.core.DeleteAction#EXCEPTION}
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws ReadOnlyTransactionException if the underlying transaction is {@linkplain Transaction#setReadOnly set read-only}
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean delete(JObject jobj) {
        return this.tx.delete(jobj.getObjId());
    }

    /**
     * Determine whether the object with the given object ID exists in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#exists} would be used instead.
     * </p>
     *
     * @param jobj the object to test for existence
     * @return true if object was found, false if object was not found
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean exists(JObject jobj) {
        return this.tx.exists(jobj.getObjId());
    }

    /**
     * Recreate the given instance in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#recreate} would be used instead.
     * </p>
     *
     * @param jobj the object to recreate
     * @return true if the object was recreated, false if the object already existed
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws ReadOnlyTransactionException if the underlying transaction is {@linkplain Transaction#setReadOnly set read-only}
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean recreate(JObject jobj) {
        return this.tx.create(jobj.getObjId());
    }

    /**
     * Add the given instance to the validation queue for validation, which will occur either at {@link #commit} time
     * or at the next invocation of {@link #validate}, whichever occurs first.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#revalidate} would be used instead.
     * </p>
     *
     * @param jobj the object to revalidate
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws NullPointerException if {@code jobj} is null
     */
    public void revalidate(JObject jobj) {
        final ObjId id = jobj.getObjId();
        if (!this.tx.exists(id))
            throw new DeletedObjectException(id);
        this.revalidate(Collections.singleton(id));
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
        if (this.committing)
            throw new IllegalStateException("commit() has already been invoked");
        this.validationQueue.clear();
    }

    void revalidate(Collection<? extends ObjId> ids) {
        if (!this.tx.isValid())
            throw new StaleTransactionException(this.tx);
        if (this.validationMode == ValidationMode.DISABLED)
            return;
        synchronized (this) {
            if (this.committing)
                throw new IllegalStateException("commit() has already been invoked");
            this.validationQueue.addAll(ids);
        }
    }

    /**
     * Get this schema version of the specified object. Does not change the object's schema version.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#getSchemaVersion} would be used instead.
     * </p>
     *
     * @param jobj object whose version to query
     * @return object's schema version
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws NullPointerException if {@code jobj} is null
     */
    public int getSchemaVersion(JObject jobj) {
        return this.tx.getSchemaVersion(jobj.getObjId());
    }

    /**
     * Update the schema version of the specified object, if necessary, so that its version matches
     * the schema version associated with this {@link JSimpleDB}.
     *
     * <p>
     * If a version change occurs, matching {@link OnVersionChange &#64;OnVersionChange} methods will be invoked prior
     * to this method returning.
     * </p>
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#upgrade} would be used instead.
     * </p>
     *
     * @param jobj object to update
     * @return true if the object's schema version was changed, false if it was already updated
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws ReadOnlyTransactionException if the underlying transaction is {@linkplain Transaction#setReadOnly set read-only}
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean updateSchemaVersion(JObject jobj) {
        return this.tx.updateSchemaVersion(jobj.getObjId());
    }

    /**
     * Read a simple field. This returns the value returned by {@link Transaction#readSimpleField Transaction.readSimpleField()}
     * with {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JField &#64;JField} getter override methods
     * and not normally invoked directly by user code.
     * </p>
     *
     * @param jobj object containing the field
     * @param storageId storage ID of the {@link JSimpleField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link JSimpleField} corresponding to {@code storageId} exists in the object
     * @throws NullPointerException if {@code jobj} is null
     */
    public Object readSimpleField(JObject jobj, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(storageId, JSimpleField.class).getConverter(this),
          this.tx.readSimpleField(jobj.getObjId(), storageId, updateVersion));
    }

    /**
     * Write a simple field. This writes the value via {@link Transaction#writeSimpleField Transaction.writeSimpleField()}
     * after converting {@link JObject}s into {@link ObjId}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JField &#64;JField} setter override methods
     * and not normally invoked directly by user code.
     * </p>
     *
     * @param jobj object containing the field
     * @param storageId storage ID of the {@link JSimpleField}
     * @param value new value for the field
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws ReadOnlyTransactionException if the underlying transaction is {@linkplain Transaction#setReadOnly set read-only}
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link JSimpleField} corresponding to {@code storageId} exists in the object
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for the field
     * @throws NullPointerException if {@code jobj} is null
     */
    public void writeSimpleField(JObject jobj, int storageId, Object value, boolean updateVersion) {
        final Converter<?, ?> converter = this.jdb.getJField(storageId, JSimpleField.class).getConverter(this);
        if (converter != null)
            value = this.convert(converter.reverse(), value);
        this.tx.writeSimpleField(jobj.getObjId(), storageId, value, updateVersion);
    }

    /**
     * Read a counter field.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JField &#64;JField} getter override methods
     * and not normally invoked directly by user code.
     * </p>
     *
     * @param jobj object containing the field
     * @param storageId storage ID of the {@link JCounterField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @return value of the counter in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link JCounterField} corresponding to {@code storageId} exists in the object
     * @throws NullPointerException if {@code jobj} is null
     */
    public Counter readCounterField(JObject jobj, int storageId, boolean updateVersion) {
        this.jdb.getJField(storageId, JCounterField.class);
        if (updateVersion)
            this.tx.updateSchemaVersion(jobj.getObjId());
        return new Counter(this.tx, jobj.getObjId(), storageId, updateVersion);
    }

    /**
     * Read a set field. This returns the set returned by {@link Transaction#readSetField Transaction.readSetField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JSetField &#64;JSetField}
     * getter override methods and not normally invoked directly by user code.
     * </p>
     *
     * @param jobj object containing the field
     * @param storageId storage ID of the {@link JSetField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link JSetField} corresponding to {@code storageId} exists in the object
     * @throws NullPointerException if {@code jobj} is null
     */
    public NavigableSet<?> readSetField(JObject jobj, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(storageId, JSetField.class).getConverter(this),
          this.tx.readSetField(jobj.getObjId(), storageId, updateVersion));
    }

    /**
     * Read a list field. This returns the list returned by {@link Transaction#readListField Transaction.readListField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JListField &#64;JListField}
     * getter override methods and not normally invoked directly by user code.
     * </p>
     *
     * @param jobj object containing the field
     * @param storageId storage ID of the {@link JListField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link JListField} corresponding to {@code storageId} exists in the object
     * @throws NullPointerException if {@code jobj} is null
     */
    public List<?> readListField(JObject jobj, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(storageId, JListField.class).getConverter(this),
          this.tx.readListField(jobj.getObjId(), storageId, updateVersion));
    }

    /**
     * Read a map field. This returns the map returned by {@link Transaction#readMapField Transaction.readMapField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JMapField &#64;JMapField}
     * getter override methods and not normally invoked directly by user code.
     * </p>
     *
     * @param jobj object containing the field
     * @param storageId storage ID of the {@link JMapField}
     * @param updateVersion true to first automatically update the object's schema version, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code jobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link JMapField} corresponding to {@code storageId} exists in the object
     * @throws NullPointerException if {@code jobj} is null
     */
    public NavigableMap<?, ?> readMapField(JObject jobj, int storageId, boolean updateVersion) {
        return this.convert(this.jdb.getJField(storageId, JMapField.class).getConverter(this),
          this.tx.readMapField(jobj.getObjId(), storageId, updateVersion));
    }

// Reference Path Access

    /**
     * Find all objects that refer to any object in the given target set through the specified path of references.
     *
     * @param startType starting Java type for the path
     * @param path dot-separated path of one or more reference fields
     * @param targetObjects target objects
     * @return set of objects that refer to any of the {@code targetObjects} via the {@code path} from {@code startType}
     * @throws org.jsimpledb.core.UnknownFieldException if {@code path} is invalid
     * @throws IllegalArgumentException if any parameter is null
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> invertReferencePath(Class<T> startType, String path, Iterable<? extends JObject> targetObjects) {
        if (targetObjects == null)
            throw new IllegalArgumentException("null targetObjects");
        final ReferencePath refPath = this.jdb.parseReferencePath(TypeToken.of(startType), path, true);
        final int targetField = refPath.getTargetField();
        if (!(this.jdb.jfields.get(targetField) instanceof JReferenceField)) {
            final String fieldName = path.substring(path.lastIndexOf('.') + 1);
            throw new IllegalArgumentException("last field `" + fieldName + "' of path `" + path + "' is not a reference field");
        }
        final int[] refs = Ints.concat(refPath.getReferenceFields(), new int[] { targetField });
        final NavigableSet<ObjId> ids = this.tx.invertReferencePath(refs,
          Iterables.transform(targetObjects, this.referenceConverter));
        return (NavigableSet<T>)new ConvertedNavigableSet<JObject, ObjId>(ids, this.referenceConverter);
    }

// Index Access

    /**
     * Query a simple field index for {@link JObject}s.
     *
     * <p>
     * This returns the map returned by {@link Transaction#querySimpleField} with {@link ObjId}s converted into {@link JObject}s.
     * </p>
     *
     * <p>
     * Used by generated {@link org.jsimpledb.annotation.IndexQuery &#64;IndexQuery} override methods
     * that return index maps whose values are sets of Java model objects (i.e., not other index entry types).
     * </p>
     *
     * @param storageId {@link JSimpleField}'s storage ID
     * @param type type restriction for the returned objects, or null to not restrict indexed object type
     * @return read-only, real-time view of field values mapped to sets of {@link JObject}s with the value in the field
     * @throws UnknownFieldException if no {@link JSimpleField} corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, ? extends NavigableSet<JObject>> querySimpleField(int storageId, Class<?> type) {
        Converter<?, ?> keyConverter = this.jdb.getJField(storageId, JSimpleField.class).getConverter(this);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        final NavigableSetConverter<JObject, ObjId> valueConverter = new NavigableSetConverter(this.referenceConverter);
        final NavigableMap<?, NavigableSet<ObjId>> map = this.tx.querySimpleField(storageId, this.getTypeStorageIds(type));
        return new ConvertedNavigableMap(map, keyConverter, valueConverter);
    }

    /**
     * Query a list field index for {@link ListIndexEntry}s.
     *
     * <p>
     * This returns the map returned by {@link Transaction#queryListFieldEntries} with
     * {@link org.jsimpledb.ListIndexEntry}s converted into {@link ListIndexEntry}s.
     * </p>
     *
     * <p>
     * Used by generated {@link org.jsimpledb.annotation.IndexQuery &#64;IndexQuery} override methods
     * that return index maps whose values are sets of {@link ListIndexEntry}s.
     * </p>
     *
     * @param storageId {@link JListField}'s storage ID
     * @param type type restriction for the returned objects, or null to not restrict indexed object type
     * @return read-only, real-time view of list element values mapped to sets of {@link ListIndexEntry}s
     * @throws UnknownFieldException if no {@link JListField} field corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, NavigableSet<ListIndexEntry<?>>> queryListFieldEntries(int storageId, Class<?> type) {
        final JListField setField = this.jdb.getJField(storageId, JListField.class);
        Converter<?, ?> keyConverter = setField.elementField.getConverter(this);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        final NavigableSetConverter valueConverter = new NavigableSetConverter(
          new ListIndexEntryConverter(this.referenceConverter));
        final NavigableMap<?, NavigableSet<org.jsimpledb.core.ListIndexEntry>> map
          = this.tx.queryListFieldEntries(storageId, this.getTypeStorageIds(type));
        return new ConvertedNavigableMap(map, keyConverter, valueConverter);
    }

    /**
     * Query a map field key index for {@link MapKeyIndexEntry}s.
     *
     * <p>
     * This returns the map returned by {@link Transaction#queryMapFieldKeyEntries}
     * with {@link org.jsimpledb.MapKeyIndexEntry}s converted into {@link MapKeyIndexEntry}s.
     * </p>
     *
     * <p>
     * Used by generated {@link org.jsimpledb.annotation.IndexQuery &#64;IndexQuery} override methods
     * that return index maps whose values are sets of {@link MapKeyIndexEntry}s.
     * </p>
     *
     * @param storageId {@link JMapField}'s storage ID
     * @param type type restriction for the returned objects, or null to not restrict indexed object type
     * @return read-only, real-time view of map key values mapped to sets of {@link MapKeyIndexEntry}s
     * @throws UnknownFieldException if no {@link JMapField} field corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, NavigableSet<MapKeyIndexEntry<?, ?>>> queryMapFieldKeyEntries(int storageId, Class<?> type) {
        final JMapField mapField = this.jdb.getJField(storageId, JMapField.class);
        Converter<?, ?> keyConverter = mapField.keyField.getConverter(this);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        Converter<?, ?> valueConverter = mapField.valueField.getConverter(this);
        valueConverter = valueConverter != null ? valueConverter.reverse() : Converter.identity();
        final NavigableMap<?, NavigableSet<org.jsimpledb.core.MapKeyIndexEntry<?>>> map
          = this.tx.queryMapFieldKeyEntries(storageId, this.getTypeStorageIds(type));
        return new ConvertedNavigableMap(map, keyConverter,
          new NavigableSetConverter(new MapKeyIndexEntryConverter(this.referenceConverter, valueConverter)));
    }

    /**
     * Query a map field value index for {@link MapValueIndexEntry}s.
     *
     * <p>
     * This returns the map returned by {@link Transaction#queryMapFieldValueEntries}
     * with {@link org.jsimpledb.MapValueIndexEntry}s converted into {@link MapValueIndexEntry}s.
     * </p>
     *
     * <p>
     * Used by generated {@link org.jsimpledb.annotation.IndexQuery &#64;IndexQuery} override methods
     * that return index maps whose values are sets of {@link MapValueIndexEntry}s.
     * </p>
     *
     * @param storageId {@link JMapField}'s storage ID
     * @param type type restriction for the returned objects, or null to not restrict indexed object type
     * @return read-only, real-time view of map values mapped to sets of {@link MapValueIndexEntry}s
     * @throws UnknownFieldException if no {@link JMapField} field corresponding to {@code storageId} exists
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, NavigableSet<MapValueIndexEntry<?, ?>>> queryMapFieldValueEntries(int storageId, Class<?> type) {
        final JMapField mapField = this.jdb.getJField(storageId, JMapField.class);
        Converter<?, ?> keyConverter = mapField.keyField.getConverter(this);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        Converter<?, ?> valueConverter = mapField.valueField.getConverter(this);
        valueConverter = valueConverter != null ? valueConverter.reverse() : Converter.identity();
        final NavigableMap<?, NavigableSet<org.jsimpledb.core.MapValueIndexEntry<?>>> map
          = this.tx.queryMapFieldValueEntries(storageId, this.getTypeStorageIds(type));
        return new ConvertedNavigableMap(map, valueConverter,
          new NavigableSetConverter(new MapValueIndexEntryConverter(this.referenceConverter, keyConverter)));
    }

    private int[] getTypeStorageIds(Class<?> type) {
        if (type == null)
            return new int[0];
        final TreeSet<Integer> storageIds = new TreeSet<>();
        for (JClass<?> jclass : this.jdb.getJClasses(TypeToken.of(type)))
            storageIds.add(jclass.storageId);
        if (storageIds.isEmpty())
            return new int[] { -1 };        // won't match anything
        return Ints.toArray(storageIds);
    }

// Transaction Lifecycle

    /**
     * Commit this transaction.
     *
     * <p>
     * Prior to actual commit, if this transaction was created with a validation mode other than {@link ValidationMode#DISABLED},
     * {@linkplain #validate validation} of outstanding objects in the validation queue is performed.
     * </p>
     *
     * <p>
     * If a {@link ValidationException} is thrown, the transaction is no longer usable. To perform validation and leave
     * the transaction open, invoke {@link #validate} prior to commit.
     * </p>
     *
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws org.jsimpledb.kv.RetryTransactionException from {@link org.jsimpledb.kv.KVTransaction#commit KVTransaction.commit()}
     * @throws ValidationException if a validation error is detected
     * @throws IllegalStateException if this method is invoked re-entrantly from within a validation check
     */
    public synchronized void commit() {

        // Sanity check
        if (!this.tx.isValid())
            throw new StaleTransactionException(this.tx);
        synchronized (this) {
            if (this.committing)
                throw new IllegalStateException("commit() invoked re-entrantly");
            this.committing = true;
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
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public void rollback() {
        this.tx.rollback();
    }

    /**
     * Determine whether this transaction is still usable.
     *
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
     * </p>
     *
     * <p>
     * <b>Note:</b> if the this transaction was created with {@link ValidationMode#DISABLED}, then this method does nothing.
     * </p>
     *
     * @throws org.jsimpledb.kv.RetryTransactionException from {@link org.jsimpledb.kv.KVTransaction#commit KVTransaction.commit()}
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
        this.performAction(new Runnable() {
            @Override
            public void run() {
                JTransaction.this.doValidate();
            }
        });
    }

    /**
     * Invoke the given {@link Runnable} with this instance as the {@linkplain #getCurrent current transaction}.
     *
     * <p>
     * If another instance is currently associated with the current thread, it is set aside for the duration of
     * {@code action}'s execution, and then restored when {@code action} is complete.
     * </p>
     *
     * @param action action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void performAction(Runnable action) {
        if (action == null)
            throw new IllegalArgumentException("null action");
        final JTransaction previous = CURRENT.get();
        CURRENT.set(this);
        try {
            action.run();
        } finally {
            CURRENT.set(previous);
        }
    }

// Internal methods

    private void doValidate() {
        while (true) {

            // Get next object
            final ObjId id;
            synchronized (this) {
                final Iterator<ObjId> i = this.validationQueue.iterator();
                try {
                    id = i.next();
                } catch (NoSuchElementException e) {
                    return;
                }
                i.remove();
            }

            // Does it still exist?
            if (!this.tx.exists(id))
                continue;

            // Do JSR 303 validation
            final JObject jobj = this.getJObject(id);
            final Set<ConstraintViolation<JObject>> violations = ValidationUtil.validate(jobj);
            if (!violations.isEmpty()) {
                throw new ValidationException(jobj, violations, "validation error for object " + id + " of type `"
                  + this.jdb.jclasses.get(id.getStorageId()).name + "':\n" + ValidationUtil.describe(violations));
            }

            // Do @Validate validation
            final JClass<?> jclass = this.jdb.getJClass(id.getStorageId());
            for (ValidateScanner<?>.MethodInfo info : jclass.validateMethods)
                Util.invoke(info.getMethod(), jobj);
        }
    }

// InternalCreateListener

    private class InternalCreateListener implements CreateListener {

        @Override
        public void onCreate(Transaction tx, ObjId id) {
            final JClass<?> jclass = JTransaction.this.jdb.getJClass(id.getStorageId());
            if (jclass == null)             // object type does not exist in our schema
                return;
            this.doOnCreate(jclass, id);
        }

        // This method exists solely to bind the generic type parameters
        private <T> void doOnCreate(JClass<T> jclass, ObjId id) {
            Object jobj = null;
            for (OnCreateScanner<T>.MethodInfo info : jclass.onCreateMethods) {
                if (jobj == null)
                    jobj = JTransaction.this.getJObject(id);
                Util.invoke(info.getMethod(), jobj);
            }
            if (validationMode == ValidationMode.AUTOMATIC)
                JTransaction.this.revalidate(Collections.singleton(id));
        }
    }

// InternalDeleteListener

    private class InternalDeleteListener implements DeleteListener {

        @Override
        public void onDelete(Transaction tx, ObjId id) {
            final JClass<?> jclass = JTransaction.this.jdb.getJClass(id.getStorageId());
            if (jclass == null)             // object type does not exist in our schema
                return;
            this.doOnDelete(jclass, id);
        }

        // This method exists solely to bind the generic type parameters
        private <T> void doOnDelete(JClass<T> jclass, ObjId id) {
            Object jobj = null;
            for (OnDeleteScanner<T>.MethodInfo info : jclass.onDeleteMethods) {
                if (jobj == null)
                    jobj = JTransaction.this.getJObject(id);
                Util.invoke(info.getMethod(), jobj);
            }
        }
    }

// InternalVersionChangeListener

    private class InternalVersionChangeListener implements VersionChangeListener {

        @Override
        public void onVersionChange(Transaction tx, ObjId id, int oldVersion, int newVersion, Map<Integer, Object> oldFieldValues) {
            final JClass<?> jclass = JTransaction.this.jdb.getJClass(id.getStorageId());
            if (jclass == null)             // object type does not exist in our schema
                return;
            this.doOnVersionChange(jclass, id, oldVersion, newVersion, oldFieldValues);
        }

        // This method exists solely to bind the generic type parameters
        private <T> void doOnVersionChange(JClass<T> jclass, ObjId id,
          int oldVersion, int newVersion, Map<Integer, Object> oldFieldValues) {
            Object jobj = null;
            final SchemaVersion oldSchema = JTransaction.this.tx.getSchema().getVersion(oldVersion);
            for (OnVersionChangeScanner<T>.MethodInfo info : jclass.onVersionChangeMethods) {
                final OnVersionChange annotation = info.getAnnotation();
                final Method method = info.getMethod();

                // Check old & new version numbers
                if ((annotation.oldVersion() != 0 && annotation.oldVersion() != oldVersion)
                  || (annotation.newVersion() != 0 && annotation.newVersion() != newVersion))
                    continue;

                // Get Java model object
                if (jobj == null)
                    jobj = JTransaction.this.getJObject(id);

                // Convert old field values so ObjId's become JObjects
                final TreeMap<Integer, Object> convertedValues = new TreeMap<>();
                for (Map.Entry<Integer, Object> entry : oldFieldValues.entrySet()) {
                    final int storageId = entry.getKey();
                    Object oldValue = entry.getValue();

                    // Convert old field value as needed; we only convert ObjId -> JObject
                    final Field<?> field = oldSchema.getSchemaItem(storageId, Field.class);
                    oldValue = JTransaction.this.convertOldValue(field, oldValue);

                    // Update value
                    convertedValues.put(storageId, oldValue);
                }

                // Invoke method
                switch ((annotation.oldVersion() != 0 ? 2 : 0) + (annotation.newVersion() != 0 ? 1 : 0)) {
                case 0:
                    Util.invoke(method, jobj, oldVersion, newVersion, convertedValues);
                    break;
                case 1:
                    Util.invoke(method, jobj, oldVersion, convertedValues);
                    break;
                case 2:
                    Util.invoke(method, jobj, newVersion, convertedValues);
                    break;
                case 3:
                default:
                    Util.invoke(method, jobj, convertedValues);
                    break;
                }
            }
        }
    }

// Convert methods

    @SuppressWarnings("unchecked")
    private <X, Y> Y convert(Converter<X, Y> converter, Object value) {
        return converter != null ? converter.convert((X)value) : (Y)value;
    }

    // Convert an old value from core database in prior schema version
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object convertOldValue(Field<?> field, Object value) {
        if (value == null)
            return null;
        final Converter converter = field.visit(new FieldSwitchAdapter<Converter>() {

            @Override
            public Converter caseReferenceField(ReferenceField field) {
                return JTransaction.this.referenceConverter;
            }

            @Override
            public <E> Converter caseSetField(SetField<E> field) {
                return field.getElementField() instanceof ReferenceField ?
                  new NavigableSetConverter(JTransaction.this.referenceConverter) : null;
            }

            @Override
            public <E> Converter caseListField(ListField<E> field) {
                return field.getElementField() instanceof ReferenceField ?
                  new ListConverter(JTransaction.this.referenceConverter) : null;
            }

            @Override
            public <K, V> Converter caseMapField(MapField<K, V> field) {
                if (field.getKeyField() instanceof ReferenceField || field.getValueField() instanceof ReferenceField) {
                    final Converter keyConverter = field.getKeyField() instanceof ReferenceField ?
                      JTransaction.this.referenceConverter : Converter.identity();
                    final Converter valueConverter = field.getValueField() instanceof ReferenceField ?
                      JTransaction.this.referenceConverter : Converter.identity();
                    return new NavigableMapConverter(keyConverter, valueConverter);
                }
                return null;
            }

            @Override
            public <T> Converter caseField(Field<T> field) {
                return null;
            }
        });
        if (converter != null) {
            try {
                value = converter.reverse().convert(value);
            } catch (UnmatchedEnumException e) {
                // ignore - give them the EnumValue object instead
            }
        }
        return value;
    }

// ValidationListener

    private class ValidationListener implements AllChangesListener {

    // SimpleFieldChangeListener

        @Override
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          SimpleField<T> field, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            JTransaction.this.revalidate(referrers);
        }

    // SetFieldChangeListener

        @Override
        public <E> void onSetFieldAdd(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onSetFieldRemove(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            JTransaction.this.revalidate(referrers);
        }

    // ListFieldChangeListener

        @Override
        public <E> void onListFieldAdd(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onListFieldRemove(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onListFieldReplace(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            JTransaction.this.revalidate(referrers);
        }

    // MapFieldChangeListener

        @Override
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            JTransaction.this.revalidate(referrers);
        }
    }
}

