
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import javax.validation.ConstraintViolation;

import org.dellroad.stuff.validation.ValidationUtil;
import org.jsimpledb.annotation.OnVersionChange;
import org.jsimpledb.core.CreateListener;
import org.jsimpledb.core.DeleteListener;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferenceField;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.SetField;
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
 */
public class JTransaction implements VersionChangeListener, CreateListener, DeleteListener {

    private static final ThreadLocal<JTransaction> CURRENT = new ThreadLocal<>();

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final JSimpleDB jdb;
    final Transaction tx;

    private final ValidationMode validationMode;
    private final ValidationListener validationListener = new ValidationListener();
    private final HashSet<ObjId> validationQueue = new HashSet<ObjId>(); // TODO: use a more efficient data structure for longs

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
        this.tx.addCreateListener(this);
        this.tx.addDeleteListener(this);
        this.tx.addVersionChangeListener(this);
        for (JClass<?> jclass : this.jdb.jclasses.values()) {
            for (OnChangeScanner<?>.MethodInfo info : jclass.onChangeMethods) {
                final OnChangeScanner<?>.ChangeMethodInfo changeInfo = (OnChangeScanner<?>.ChangeMethodInfo)info;
                changeInfo.registerChangeListener(this.tx);
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
     * Get all instances of the given type. The ordering of the returned set is based on the object IDs.
     *
     * @param type any Java type with annotated Java object model sub-types
     * @return read-only view of all instances of {@code type}
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> getAll(Class<T> type) {
        final TypeToken<T> typeToken = TypeToken.of(type);
        final ArrayList<NavigableSet<ObjId>> sets = new ArrayList<>();
        for (JClass<?> jclass : this.jdb.jclasses.values()) {
            if (typeToken.isAssignableFrom(jclass.typeToken))
                sets.add(this.tx.getAll(jclass.storageId));
        }
        return sets.isEmpty() ? NavigableSets.<T>empty() :
          (NavigableSet<T>)new ConvertedNavigableSet<JObject, ObjId>(NavigableSets.union(sets), this.jdb.referenceConverter);
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
     * @param path a {@linkplain ReferencePath reference path} with zero intermediate reference fields
     * @return the {@link org.jsimpledb.kv.KVDatabase} key of the field in the specified object
     * @throws IllegalArgumentException if {@code path} is invalid
     * @throws IllegalArgumentException if {@code path} contains one or more intermediate references
     * @throws IllegalArgumentException if the target of {@code path} is a sub-field of a complex field
     * @throws IllegalArgumentException if {@code jobj} does not contain the specified field
     * @throws IllegalArgumentException if either parameter is null
     */
    public byte[] getKey(JObject jobj, String path) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        final TypeToken<?> startType = this.jdb.getJClass(jobj.getObjId().getStorageId()).typeToken;
        final ReferencePath refPath = new ReferencePath(this.jdb, startType, path, false);
        if (refPath.getReferenceFields().length > 0)
            throw new IllegalArgumentException("path `" + path + "' contains one or more reference fields");
        if (!refPath.targetType.getRawType().isInstance(jobj))
            throw new IllegalArgumentException("jobj is not an instance of " + refPath.targetType);
        return this.tx.getKey(jobj.getObjId(), refPath.targetField.storageId);
    }

// Object/Field Access

    /**
     * Create a new instance of the given model class in this transaction.
     *
     * @param type an annotated Java object model type
     * @return newly created instance
     * @throws IllegalArgumentException if {@code type} is not a known Java object model type
     */
    public <T> T create(Class<T> type) {
        final JClass<T> jclass = this.jdb.getJClass(TypeToken.of(type));
        final ObjId id = this.tx.create(jclass.storageId);
        return type.cast(this.jdb.getJObject(id));
    }

    /**
     * Delete the given instance in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#delete} would be used instead.
     * </p>
     *
     * @param jobj Java model object
     * @return true if the object was deleted, false if {@code obj} is null or did not exist
     * @throws NullPointerException if {@code jobj} is null
     */
    public boolean delete(JObject jobj) {
        return this.tx.delete(jobj.getObjId());
    }

    /**
     * Determine whether the given instance exists in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link JObject#exists} would be used instead.
     * </p>
     *
     * @param jobj Java model object
     * @return true if the object exists, false if {@code obj} is null or does not exist
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
     * @param jobj Java model object
     * @return true if the object was recreated, false if {@code obj} already existed
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
     * @param jobj Java model object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws NullPointerException if {@code jobj} is null
     */
    public void revalidate(JObject jobj) {
        this.revalidate(Collections.singleton(jobj.getObjId()));
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
     * Update the schema version of the specified object, if necessary, so that its version matches
     * the schema version associated with this {@link JSimpleDB}.
     *
     * <p>
     * If a version change occurs, matching {@link OnVersionChange &#64;OnVersionChange} methods will be invoked prior
     * to this method returning.
     * </p>
     *
     * @param jobj Java model object
     * @return true if the object's schema version was changed, false if it was already updated
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws org.jsimpledb.core.DeletedObjectException if the object does not exist in this transaction
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public synchronized boolean updateSchemaVersion(JObject jobj) {

        // Sanity check
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");

        // Update version
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
     */
    public Object readSimpleField(ObjId id, int storageId) {
        return this.convert(this.jdb.getJField(storageId, JSimpleField.class).getConverter(this.jdb),
          this.tx.readSimpleField(id, storageId));
    }

    /**
     * Write a simple field. This writes the value via {@link Transaction#writeSimpleField Transaction.writeSimpleField()}
     * after converting {@link JObject}s into {@link ObjId}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JField &#64;JField} setter override methods
     * and not normally invoked directly by user code.
     * </p>
     */
    public void writeSimpleField(ObjId id, int storageId, Object value) {
        final Converter<?, ?> converter = this.jdb.getJField(storageId, JSimpleField.class).getConverter(this.jdb);
        if (converter != null)
            value = this.convert(converter.reverse(), value);
        this.tx.writeSimpleField(id, storageId, value);
    }

    /**
     * Read a set field. This returns the set returned by {@link Transaction#readSetField Transaction.readSetField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JSetField &#64;JSetField}
     * getter override methods and not normally invoked directly by user code.
     * </p>
     */
    public NavigableSet<?> readSetField(ObjId id, int storageId) {
        return this.convert(this.jdb.getJField(storageId, JSetField.class).getConverter(this.jdb),
          this.tx.readSetField(id, storageId));
    }

    /**
     * Read a list field. This returns the list returned by {@link Transaction#readListField Transaction.readListField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JListField &#64;JListField}
     * getter override methods and not normally invoked directly by user code.
     * </p>
     */
    public List<?> readListField(ObjId id, int storageId) {
        return this.convert(this.jdb.getJField(storageId, JListField.class).getConverter(this.jdb),
          this.tx.readListField(id, storageId));
    }

    /**
     * Read a map field. This returns the map returned by {@link Transaction#readMapField Transaction.readMapField()} with
     * {@link ObjId}s converted into {@link JObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link org.jsimpledb.annotation.JMapField &#64;JMapField}
     * getter override methods and not normally invoked directly by user code.
     * </p>
     */
    public NavigableMap<?, ?> readMapField(ObjId id, int storageId) {
        return this.convert(this.jdb.getJField(storageId, JMapField.class).getConverter(this.jdb),
          this.tx.readMapField(id, storageId));
    }

// Reference Path Access

    /**
     * Find all objects that refer to any object in the given target set through the specified path of references.
     *
     * @param startType starting Java type for the path
     * @param path dot-separated path of zero or more reference fields, followed by a final reference field
     * @param targetObjects target objects
     * @return set of objects that refer to any of the {@code targetObjects} via the {@code path} from {@code startType}
     * @throws org.jsimpledb.core.UnknownFieldException if {@code path} is invalid
     * @throws IllegalArgumentException if any parameter is null
     */
    @SuppressWarnings("unchecked")
    public <T> NavigableSet<T> invertReferencePath(Class<T> startType, String path, Iterable<? extends JObject> targetObjects) {
        if (targetObjects == null)
            throw new IllegalArgumentException("null targetObjects");
        final ReferencePath refPath = new ReferencePath(this.jdb, TypeToken.of(startType), path, true);
        final int targetField = refPath.getTargetField();
        if (!(this.jdb.jfields.get(targetField) instanceof JReferenceField)) {
            final String fieldName = path.substring(path.lastIndexOf('.') + 1);
            throw new IllegalArgumentException("last field `" + fieldName + "' of path `" + path + "' is not a reference field");
        }
        final int[] refs = Ints.concat(refPath.getReferenceFields(), new int[] { targetField });
        final NavigableSet<ObjId> ids = this.tx.invertReferencePath(refs,
          Iterables.transform(targetObjects, this.jdb.referenceConverter));
        return (NavigableSet<T>)new ConvertedNavigableSet<JObject, ObjId>(ids, this.jdb.referenceConverter);
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
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, NavigableSet<?>> querySimpleField(int storageId) {
        Converter<?, ?> keyConverter = this.jdb.getJField(storageId, JSimpleField.class).getConverter(this.jdb);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        final NavigableSetConverter<JObject, ObjId> valueConverter = new NavigableSetConverter(this.jdb.referenceConverter);
        final NavigableMap<?, NavigableSet<ObjId>> map = this.tx.querySimpleField(storageId);
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
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, NavigableSet<?>> queryListFieldEntries(int storageId) {
        final JListField setField = this.jdb.getJField(storageId, JListField.class);
        Converter<?, ?> keyConverter = setField.elementField.getConverter(this.jdb);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        final NavigableSetConverter valueConverter = new NavigableSetConverter(
          new ListIndexEntryConverter(this.jdb.referenceConverter));
        final NavigableMap<?, NavigableSet<org.jsimpledb.core.ListIndexEntry>> map = this.tx.queryListFieldEntries(storageId);
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
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, NavigableSet<?>> queryMapFieldKeyEntries(int storageId) {
        final JMapField mapField = this.jdb.getJField(storageId, JMapField.class);
        Converter<?, ?> keyConverter = mapField.keyField.getConverter(this.jdb);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        Converter<?, ?> valueConverter = mapField.valueField.getConverter(this.jdb);
        valueConverter = valueConverter != null ? valueConverter.reverse() : Converter.identity();
        final NavigableMap<?, NavigableSet<org.jsimpledb.core.MapKeyIndexEntry<?>>> map
          = this.tx.queryMapFieldKeyEntries(storageId);
        return new ConvertedNavigableMap(map, keyConverter,
          new NavigableSetConverter(new MapKeyIndexEntryConverter(this.jdb.referenceConverter, valueConverter)));
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
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NavigableMap<?, NavigableSet<?>> queryMapFieldValueEntries(int storageId) {
        final JMapField mapField = this.jdb.getJField(storageId, JMapField.class);
        Converter<?, ?> keyConverter = mapField.keyField.getConverter(this.jdb);
        keyConverter = keyConverter != null ? keyConverter.reverse() : Converter.identity();
        Converter<?, ?> valueConverter = mapField.valueField.getConverter(this.jdb);
        valueConverter = valueConverter != null ? valueConverter.reverse() : Converter.identity();
        final NavigableMap<?, NavigableSet<org.jsimpledb.core.MapValueIndexEntry<?>>> map
          = this.tx.queryMapFieldValueEntries(storageId);
        return new ConvertedNavigableMap(map, valueConverter,
          new NavigableSetConverter(new MapValueIndexEntryConverter(this.jdb.referenceConverter, keyConverter)));
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
        this.runWithCurrent(new Runnable() {
            @Override
            public void run() {
                JTransaction.this.doValidate();
            }
        });
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
            final JObject jobj = this.jdb.getJObject(id);
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

    private void runWithCurrent(Runnable action) {
        final JTransaction previous = CURRENT.get();
        CURRENT.set(this);
        try {
            action.run();
        } finally {
            CURRENT.set(previous);
        }
    }

// CreateListener

    @Override
    public void onCreate(Transaction tx, ObjId id) {
        if (tx != this.tx)
            throw new IllegalArgumentException("wrong tx");
        final JClass<?> jclass = this.jdb.getJClass(id.getStorageId());
        this.doOnCreate(jclass, id);
    }

    // This method exists solely to bind the generic type parameters
    private <T> void doOnCreate(JClass<T> jclass, ObjId id) {
        for (OnCreateScanner<T>.MethodInfo info : jclass.onCreateMethods)
            Util.invoke(info.getMethod(), this.jdb.getJObject(id));
    }

// DeleteListener

    @Override
    public void onDelete(Transaction tx, ObjId id) {
        if (tx != this.tx)
            throw new IllegalArgumentException("wrong tx");
        final JClass<?> jclass = this.jdb.getJClass(id.getStorageId());
        this.doOnDelete(jclass, id);
    }

    // This method exists solely to bind the generic type parameters
    private <T> void doOnDelete(JClass<T> jclass, ObjId id) {
        for (OnDeleteScanner<T>.MethodInfo info : jclass.onDeleteMethods)
            Util.invoke(info.getMethod(), this.jdb.getJObject(id));
    }

// VersionChangeListener

    @Override
    public void onVersionChange(Transaction tx, ObjId id, int oldVersion, int newVersion, Map<Integer, Object> oldFieldValues) {
        if (tx != this.tx)
            throw new IllegalArgumentException("wrong tx");
        final JClass<?> jclass = this.jdb.getJClass(id.getStorageId());
        this.doOnVersionChange(jclass, id, oldVersion, newVersion, oldFieldValues);
    }

    // This method exists solely to bind the generic type parameters
    private <T> void doOnVersionChange(JClass<T> jclass, ObjId id,
      int oldVersion, int newVersion, Map<Integer, Object> oldFieldValues) {
        Object jobj = null;
        final SchemaVersion oldSchema = this.tx.getSchema().getVersion(oldVersion);
        for (OnVersionChangeScanner<T>.MethodInfo info : jclass.onVersionChangeMethods) {
            final OnVersionChange annotation = info.getAnnotation();
            final Method method = info.getMethod();

            // Check old & new version numbers
            if ((annotation.oldVersion() != 0 && annotation.oldVersion() != oldVersion)
              || (annotation.newVersion() != 0 && annotation.newVersion() != newVersion))
                continue;

            // Get Java model object
            if (jobj == null)
                jobj = this.jdb.getJObject(id);

            // Convert old field values so ObjId's become JObjects
            final TreeMap<Integer, Object> convertedValues = new TreeMap<>();
            for (Map.Entry<Integer, Object> entry : oldFieldValues.entrySet()) {
                final int storageId = entry.getKey();
                Object oldValue = entry.getValue();

                // Convert old field value as needed; we only convert ObjId -> JObject
                final Field<?> field = oldSchema.getSchemaItem(storageId, Field.class);
                oldValue = this.convertOldValue(field, oldValue);

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

    @SuppressWarnings("unchecked")
    private <X, Y> Y convert(Converter<X, Y> converter, Object value) {
        try {
            return converter != null ? converter.convert((X)value) : (Y)value;
        } catch (UnmatchedEnumException e) {
            this.log.warn("unable to convert enum value that has disappeared, returning null", e);
            return null;
        }
    }

    // Convert an old value from core database in prior schema version
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object convertOldValue(Field<?> field, Object value) {
        if (value == null)
            return null;
        Converter converter = null;
        if (field instanceof ReferenceField)
            converter = this.jdb.referenceConverter;
        else if (field instanceof SetField && ((SetField)field).getElementField() instanceof ReferenceField)
            converter = new NavigableSetConverter(this.jdb.referenceConverter);
        else if (field instanceof ListField && ((SetField)field).getElementField() instanceof ReferenceField)
            converter = new ListConverter(this.jdb.referenceConverter);
        else if (field instanceof MapField
          && (((MapField)field).getKeyField() instanceof ReferenceField
           || ((MapField)field).getValueField() instanceof ReferenceField)) {
            final MapField<?, ?> mapField = (MapField<?, ?>)field;
            final Converter keyConverter = mapField.getKeyField() instanceof ReferenceField ?
              this.jdb.referenceConverter : Converter.identity();
            final Converter valueConverter = mapField.getValueField() instanceof ReferenceField ?
              this.jdb.referenceConverter : Converter.identity();
            converter = new NavigableMapConverter(keyConverter, valueConverter);
        }
        if (converter != null)
            value = converter.reverse().convert(value);
        return value;
    }

// ValidationListener

    private class ValidationListener implements AllChangesListener {

    // SimpleFieldChangeListener

        @Override
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          int storageId, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            JTransaction.this.revalidate(referrers);
        }

    // SetFieldChangeListener

        @Override
        public <E> void onSetFieldAdd(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onSetFieldRemove(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onSetFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            JTransaction.this.revalidate(referrers);
        }

    // ListFieldChangeListener

        @Override
        public <E> void onListFieldAdd(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onListFieldRemove(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onListFieldReplace(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <E> void onListFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            JTransaction.this.revalidate(referrers);
        }

    // MapFieldChangeListener

        @Override
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V value) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V oldValue, V newValue) {
            JTransaction.this.revalidate(referrers);
        }

        @Override
        public <K, V> void onMapFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            JTransaction.this.revalidate(referrers);
        }
    }
}

