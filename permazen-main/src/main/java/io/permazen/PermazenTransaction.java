
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.OnCreate;
import io.permazen.annotation.OnSchemaChange;
import io.permazen.annotation.PermazenType;
import io.permazen.core.CoreIndex1;
import io.permazen.core.CoreIndex2;
import io.permazen.core.CoreIndex3;
import io.permazen.core.CoreIndex4;
import io.permazen.core.CounterField;
import io.permazen.core.CreateListener;
import io.permazen.core.DeleteAction;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.EnumValue;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitch;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.ReferenceField;
import io.permazen.core.Schema;
import io.permazen.core.SchemaChangeListener;
import io.permazen.core.SchemaMismatchException;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.StaleTransactionException;
import io.permazen.core.Transaction;
import io.permazen.core.TypeNotInSchemaException;
import io.permazen.core.UnknownFieldException;
import io.permazen.core.UnknownTypeException;
import io.permazen.core.util.ObjIdMap;
import io.permazen.core.util.ObjIdSet;
import io.permazen.encoding.Encoding;
import io.permazen.index.Index;
import io.permazen.index.Index1;
import io.permazen.index.Index2;
import io.permazen.index.Index3;
import io.permazen.index.Index4;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVDatabaseException;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.util.AbstractKVNavigableSet;
import io.permazen.schema.SchemaId;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;
import io.permazen.tuple.Tuple4;
import io.permazen.util.ByteData;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
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
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-java.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
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
 *  <li>{@link #getCurrent getCurrent()} - Get the {@link PermazenTransaction} instance associated with the current thread</li>
 *  <li>{@link #setCurrent setCurrent()} - Set the {@link PermazenTransaction} instance associated with the current thread</li>
 *  <li>{@link #isOpen isOpen()} - Test whether transaction is still open</li>
 *  <li>{@link #performAction performAction()} - Perform action with this instance as the current transaction</li>
 * </ul>
 *
 * <p>
 * <b>Object Access</b>
 * <ul>
 *  <li>{@link #get(ObjId, Class) get()} - Get the Java model object in this transaction corresponding to a
 *      specific database object ID</li>
 *  <li>{@link #getSingleton(Class) getSingleton()} - Get a singleton instance in this transaction, creating it on demand
 *      if needed</li>
 *  <li>{@link #getAll getAll()} - Get all Java model objects in this transaction that are instances of a given Java type</li>
 *  <li>{@link #create(Class) create()} - Create a new Java model object in this transaction</li>
 *  <li>{@link #cascade cascade()} - Find all objects reachable through a reference cascade</li>
 * </ul>
 *
 * <p>
 * <b>Copying Objects</b>
 * <ul>
 *  <li>{@link #copyTo(PermazenTransaction, CopyState, Stream) copyTo()} - Copy a {@link Stream} of objects into another
 *      transaction</li>
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
 *  <li>{@link #querySimpleIndex(Class, String, Class) querySimpleIndex()}
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
 *  <li>{@link #querySchemaIndex querySchemaIndex()} - Get database objects grouped by schema</li>
 * </ul>
 *
 * <p>
 * <b>Reference Paths</b>
 * <ul>
 *  <li>{@link #followReferencePath followReferencePath()} - Find all objects reachable by traversing a {@link ReferencePath}</li>
 *  <li>{@link #invertReferencePath invertReferencePath()} - Find all objects reachable by traversing a {@link ReferencePath}
 *      in the reverse direction</li>
 * </ul>
 *
 * <p>
 * <b>Detached Transactions</b>
 * <ul>
 *  <li>{@link #getDetachedTransaction getDetachedTransaction()} - Get the default in-memory detached transaction
 *      associated with this regular transaction</li>
 *  <li>{@link #createDetachedTransaction createDetachedTransaction()} - Create a new in-memory detached transaction</li>
 *  <li>{@link #createSnapshotTransaction createSnapshotTransaction()} - Create a new in-memory detached transaction
 *      pre-populated with a snapshot of this transaction</li>
 *  <li>{@link #isDetached} - Determine whether this transaction is a detached transaction</li>
 * </ul>
 *
 * <p>
 * <b>Lower Layer Access</b>
 * <ul>
 *  <li>{@link #getKey(PermazenObject) getKey()} - Get the {@link KVDatabase} key prefix for an object</li>
 *  <li>{@link PermazenField#getKey(PermazenObject) PermazenField.getKey()} - Get the {@link KVDatabase} key for one field
 *      in an object</li>
 *  <li>{@link #withWeakConsistency withWeakConsistency()} - Perform an operation with weaker transaction consistency</li>
 * </ul>
 *
 * <p>
 * The remaining methods in this class are normally only used by generated Java model object subclasses.
 * Instead of using these methods directly, using the appropriately annotated Java model object method
 * or {@link PermazenObject} interface method is recommended.
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
 *  <li>{@link #registerPermazenObject registerPermazenObject()} - Ensure a {@link PermazenObject} is registered
 *  in the internal object cache</li>
 * </ul>
 *
 * <p>
 * <b>{@link PermazenObject} Methods</b>
 * <ul>
 *  <li>{@link #delete delete()} - Delete an object from this transaction</li>
 *  <li>{@link #exists exists()} - Test whether an object exists in this transaction</li>
 *  <li>{@link #recreate recreate()} - Recreate an object in this transaction</li>
 *  <li>{@link #revalidate revalidate()} - Manually add an object to the validation queue</li>
 *  <li>{@link #migrateSchema migrateSchema()} - Migrate an object's schema to match this transaction's data model</li>
 * </ul>
 */
@ThreadSafe
public class PermazenTransaction {

    private static final ThreadLocal<PermazenTransaction> CURRENT = new ThreadLocal<>();
    private static final Class<?>[] DEFAULT_CLASS_ARRAY = { Default.class };
    private static final Class<?>[] DEFAULT_AND_UNIQUENESS_CLASS_ARRAY = { Default.class, UniquenessConstraints.class };
    private static final int MAX_UNIQUE_CONFLICTORS = 5;

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final Permazen pdb;
    final Transaction tx;
    final ReferenceConverter<PermazenObject> referenceConverter = new ReferenceConverter<>(this, PermazenObject.class);

    private final ValidationMode validationMode;
    @GuardedBy("this")
    private final ObjIdMap<Class<?>[]> validationQueue = new ObjIdMap<>();  // maps object -> groups for pending validation
    private final PermazenObjectCache pobjectCache = new PermazenObjectCache(this);

    @GuardedBy("this")
    private DetachedPermazenTransaction detachedTransaction;
    @GuardedBy("this")
    private boolean commitInvoked;

// Constructor

    /**
     * Constructor.
     *
     * @throws IllegalArgumentException if any parameter is null
     */
    PermazenTransaction(Permazen pdb, Transaction tx, ValidationMode validationMode) {

        // Initialization
        Preconditions.checkArgument(pdb != null, "null pdb");
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        this.pdb = pdb;
        this.tx = tx;
        this.validationMode = validationMode;

        // Set back-reference
        tx.setUserObject(this);

        // Register listeners, or re-use our existing listener set
        final boolean automaticValidation = validationMode == ValidationMode.AUTOMATIC;
        final boolean isDetached = this.isDetached();
        final int listenerSetIndex = (automaticValidation ? 2 : 0) + (isDetached ? 0 : 1);
        final Transaction.ListenerSet listenerSet = pdb.listenerSets[listenerSetIndex];
        if (listenerSet == null) {
            PermazenTransaction.registerListeners(pdb, tx, automaticValidation, isDetached);
            pdb.listenerSets[listenerSetIndex] = tx.snapshotListeners();
        } else
            tx.setListeners(listenerSet);
    }

    // Register listeners for the given situation
    private static void registerListeners(Permazen pdb, Transaction tx, boolean automaticValidation, boolean isDetached) {

        // Register create listeners for @OnCreate
        for (PermazenClass<?> pclass : pdb.pclasses) {
            for (OnCreateScanner<?>.MethodInfo info : pclass.onCreateMethods) {
                final OnCreateScanner<?>.CreateMethodInfo createInfo = (OnCreateScanner<?>.CreateMethodInfo)info;
                createInfo.registerCreateListeners(tx);
            }
        }

        // Register create listeners for automatic validation on creation
        for (PermazenClass<?> pclass : pdb.pclasses) {
            if (automaticValidation && pclass.requiresDefaultValidation)
                tx.addCreateListener(pclass.storageId, new ValidateOnCreateListener());
        }

        // Register delete listeners for @OnDelete
        for (PermazenClass<?> pclass : pdb.pclasses) {
            for (OnDeleteScanner<?>.MethodInfo info : pclass.onDeleteMethods) {
                final OnDeleteScanner<?>.DeleteMethodInfo deleteInfo = (OnDeleteScanner<?>.DeleteMethodInfo)info;
                deleteInfo.registerDeleteListener(tx);
            }
        }

        // Register field change listeners for @OnChange
        for (PermazenClass<?> pclass : pdb.pclasses) {
            for (OnChangeScanner<?>.MethodInfo info : pclass.onChangeMethods) {
                final OnChangeScanner<?>.ChangeMethodInfo changeInfo = (OnChangeScanner<?>.ChangeMethodInfo)info;
                changeInfo.registerChangeListener(tx);
            }
        }

        // Register field change listeners for automatic validation of JSR 303 and uniqueness constraints
        if (automaticValidation) {
            final DefaultValidationListener defaultValidationListener = new DefaultValidationListener();
            pdb.fieldsRequiringDefaultValidation
              .forEach(storageId -> tx.addFieldChangeListener(storageId, new int[0], null, defaultValidationListener));
        }

        // Register schema change listeners for @OnSchemaChange and automatic field conversion and/or validation on upgrade
        for (PermazenClass<?> pclass : pdb.pclasses) {
            if (!pclass.onSchemaChangeMethods.isEmpty()
              || !pclass.upgradeConversionFields.isEmpty()
              || (automaticValidation && pclass.requiresDefaultValidation))
                tx.addSchemaChangeListener(pclass.storageId, new InternalSchemaChangeListener());
        }
    }

// Thread-local Access

    /**
     * Get the {@link PermazenTransaction} associated with the current thread, if any, otherwise throw an exception.
     *
     * @return instance previously associated with the current thread via {@link #setCurrent setCurrent()}
     * @throws IllegalStateException if there is no such instance
     */
    public static PermazenTransaction getCurrent() {
        final PermazenTransaction ptx = CURRENT.get();
        if (ptx == null) {
            throw new IllegalStateException(String.format(
              "there is no %s associated with the current thread", PermazenTransaction.class.getSimpleName()));
        }
        return ptx;
    }

    /**
     * Set the {@link PermazenTransaction} associated with the current thread.
     *
     * @param ptx transaction to associate with the current thread
     */
    public static void setCurrent(PermazenTransaction ptx) {
        CURRENT.set(ptx);
    }

    /**
     * Determine if there is a {@link PermazenTransaction} associated with the current thread.
     *
     * @return true if there is a current {@link PermazenTransaction}, otherwise false
     */
    public static boolean hasCurrent() {
        return CURRENT.get() != null;
    }

// Accessors

    /**
     * Get the {@link Permazen} associated with this instance.
     *
     * @return the associated database
     */
    public Permazen getPermazen() {
        return this.pdb;
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
     * The returned set includes objects from all schemas. Use {@link #querySchemaIndex querySchemaIndex()} to
     * find objects with a specific schema.
     *
     * <p>
     * The returned set is mutable, with the exception that {@link NavigableSet#add add()} is not supported.
     * Deleting an element results in {@linkplain PermazenObject#delete deleting} the corresponding object.
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
        final KeyRanges keyRanges = this.pdb.keyRangesFor(type);
        if (!keyRanges.isFull())
            ids = ((AbstractKVNavigableSet<ObjId>)ids).filterKeys(keyRanges);
        return new ConvertedNavigableSet<T, ObjId>(ids, new ReferenceConverter<T>(this, type));
    }

    /**
     * Get the singleton instance of the given type, creating it on demand if needed.
     *
     * <p>
     * This is a convenience method for accessing the singleton instances of any type annotated with
     * {@link PermazenType &#64;PermazenType}{@code (singleton = true)}. If {@code type} is not a
     * singleton type, an exception is thrown.
     *
     * <p>
     * If no instance exists yet, one is created. So this method is essentially shorthand for:
     *
     * <pre><code class="language-java">
     *  try {
     *      return this.getAll(type).first();
     *  } catch (NoSuchElementException e) {
     *      return this.create(type);
     *  }
     * </code></pre>
     *
     * @param type singleton model class
     * @param <T> singleton type
     * @return the singleton instance of type {@code type}
     * @throws IllegalArgumentException if {@code type} is not a singleton type
     * @throws IllegalArgumentException if {@code type} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     * @see PermazenType#singleton
     */
    @SuppressWarnings("unchecked")
    public <T> T getSingleton(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final PermazenClass<T> pclass = this.pdb.getPermazenClass(type);
        if (!pclass.singleton)
            throw new IllegalArgumentException(String.format("model type is not a singleton type: %s", type));
        final AbstractKVNavigableSet<ObjId> ids = (AbstractKVNavigableSet<ObjId>)this.tx.getAll(pclass.name);
        final ObjId id;
        try {
            id = ids.first();
        } catch (NoSuchElementException e) {
            return this.create(type);
        }
        return type.cast(this.get(id));
    }

    /**
     * Get all instances of the given type, grouped according to schema index.
     *
     * @param type any Java type; use {@link Object Object.class} to return all database objects
     * @param <T> containing Java type
     * @return live, read-only mapping from {@link SchemaId} to objects having that schema
     * @throws IllegalArgumentException if {@code type} is null
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public <T> NavigableMap<SchemaId, NavigableSet<T>> querySchemaIndex(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        CoreIndex1<Integer, ObjId> index = this.tx.querySchemaIndex();
        final KeyRanges keyRanges = this.pdb.keyRangesFor(type);
        if (!keyRanges.isFull())
            index = index.filter(1, keyRanges);
        final Converter<SchemaId, Integer> schemaIndexConverter = Converter.from(
          schemaId -> this.tx.getSchemaBundle().getSchema(schemaId).getSchemaIndex(),
          schemaIndex -> this.tx.getSchemaBundle().getSchema(schemaIndex).getSchemaId());
        final Converter<NavigableSet<T>, NavigableSet<ObjId>> valueConverter
          = new NavigableSetConverter<T, ObjId>(new ReferenceConverter<T>(this, type));
        return new ConvertedNavigableMap<>(index.asMap(), schemaIndexConverter, valueConverter);
    }

    /**
     * Get the {@code byte[]} key in the underlying key/value store corresponding to the specified object.
     *
     * <p>
     * Notes:
     * <ul>
     *  <li>Objects utilize multiple keys; the return value is the common prefix of all such keys.</li>
     *  <li>The {@link KVDatabase} should not be modified directly, otherwise behavior is undefined</li>
     * </ul>
     *
     * @param pobj Java model object
     * @return the {@link KVDatabase} key corresponding to {@code pobj}
     * @throws IllegalArgumentException if {@code pobj} is null
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     * @see Transaction#getKey(ObjId) Transaction.getKey()
     */
    public ByteData getKey(PermazenObject pobj) {
        Preconditions.checkArgument(pobj != null, "null pobj");
        return this.tx.getKey(pobj.getObjId());
    }

// Detached transactions

    /**
     * Determine whether this instance is a {@link DetachedPermazenTransaction}.
     *
     * @return true if this instance is a {@link DetachedPermazenTransaction}, otherwise false
     */
    public boolean isDetached() {
        return false;
    }

    /**
     * Get the default {@link DetachedPermazenTransaction} associated with this instance.
     *
     * <p>
     * The default {@link DetachedPermazenTransaction} uses {@link ValidationMode#MANUAL}.
     *
     * <p>
     * This instance must not itself be a {@link DetachedPermazenTransaction}; use
     * {@link #createDetachedTransaction createDetachedTransaction()} to create additional detached transactions.
     *
     * @return the associated detached transaction
     * @see PermazenObject#copyOut PermazenObject.copyOut()
     * @throws IllegalArgumentException if this instance is itself a {@link DetachedPermazenTransaction}
     */
    public synchronized DetachedPermazenTransaction getDetachedTransaction() {
        Preconditions.checkArgument(!this.isDetached(),
          "getDetachedTransaction() invoked on a detached transaction; use createDetachedTransaction() instead");
        if (this.detachedTransaction == null)
            this.detachedTransaction = this.createDetachedTransaction(ValidationMode.MANUAL);
        return this.detachedTransaction;
    }

    /**
     * Create a new, empty detached transaction.
     *
     * <p>
     * The returned transaction will have the same schema meta-data as this instance.
     * It will be a mutable transaction, but being detached, changes can't be committed.
     *
     * <p>
     * The returned {@link DetachedPermazenTransaction} does not support {@link #commit} or {@link #rollback}.
     * It can be used indefinitely after this transaction closes, but it must be
     * {@link DetachedPermazenTransaction#close close()}'d when no longer needed to release any associated resources.
     *
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return newly created detached transaction
     * @throws IllegalArgumentException if {@code validationMode} is null
     * @throws io.permazen.core.StaleTransactionException if this instance is no longer usable
     */
    public DetachedPermazenTransaction createDetachedTransaction(ValidationMode validationMode) {
        return new DetachedPermazenTransaction(this.pdb, this.tx.createDetachedTransaction(), validationMode);
    }

    /**
     * Create a new detached transaction pre-populated with a snapshot of this transaction.
     *
     * <p>
     * The returned transaction will have the same schema meta-data and object content as this instance.
     * It will be a mutable transaction, but being detached, changes can't be committed.
     *
     * <p>
     * The returned {@link DetachedPermazenTransaction} does not support {@link #commit} or {@link #rollback}.
     * It can be used indefinitely after this transaction closes, but it must be
     * {@link DetachedPermazenTransaction#close close()}'d when no longer needed to release any associated resources.
     *
     * <p>
     * This method requires the underlying key/value transaction to support {@link KVTransaction#readOnlySnapshot}.
     * As with any other information extracted from this transaction, the returned content is not guaranteed to be
     * valid until this transaction has been successfully committed.
     *
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return newly created detached transaction
     * @throws IllegalArgumentException if {@code validationMode} is null
     * @throws UnsupportedOperationException if they underlying key/value transaction doesn't support
     *  {@link KVTransaction#readOnlySnapshot}
     * @throws io.permazen.core.StaleTransactionException if this instance is no longer usable
     */
    public DetachedPermazenTransaction createSnapshotTransaction(ValidationMode validationMode) {
        return new DetachedPermazenTransaction(this.pdb, this.tx.createSnapshotTransaction(), validationMode);
    }

    /**
     * Copy the specified objects into the specified destination transaction.
     *
     * <p>
     * This is a convenience method; equivalent to {@link #copyTo(PermazenTransaction, CopyState, Stream)}}
     * but with the objects specified by object ID.
     *
     * @param dest destination transaction
     * @param copyState tracks which objects have already been copied and whether to remap object ID's
     * @param ids the object ID's of the objects to copy
     * @throws DeletedObjectException if any object to be copied does not actually exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object that does not exist
     *  in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws SchemaMismatchException if the schema corresponding to any copied object is not identical
     *  in both this instance and {@code dest}
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws IllegalArgumentException if any parameter is null
     */
    public void copyTo(PermazenTransaction dest, CopyState copyState, ObjIdSet ids) {
        Preconditions.checkArgument(ids != null, "null ids");
        this.copyIdStreamTo(dest, copyState, ids.stream());
    }

    /**
     * Copy the specified objects into the specified destination transaction.
     *
     * <p>
     * If a target object does not exist, it will be created, otherwise its schema will be migrated to match the source
     * object if necessary (with resulting {@link OnSchemaChange &#64;OnSchemaChange} notifications).
     * If {@link CopyState#isSuppressNotifications()} returns false, {@link OnCreate &#64;OnCreate}
     * and {@link OnChange &#64;OnChange} notifications will also be delivered.
     *
     * <p>
     * The {@code copyState} tracks which objects have already been copied. For a "fresh" copy operation,
     * pass a newly created {@link CopyState}; for a copy operation that is a continuation of a previous copy,
     * reuse the previous {@link CopyState}. The {@link CopyState} may also be configured to remap object ID's.
     *
     * <p>
     * This instance and {@code dest} must be compatible in that for any schemas encountered, those schemas
     * must be identical in both transactions.
     *
     * <p>
     * If {@code dest} is not a {@link DetachedPermazenTransaction} and any copied objects contain reference fields configured with
     * {@link io.permazen.annotation.PermazenField#allowDeleted}{@code = false}, then any objects referenced by those fields must
     * also be copied, or else must already exist in {@code dest}. Otherwise, a {@link DeletedObjectException} is thrown
     * and it is indeterminate which objects were copied.
     *
     * <p>
     * Note: if two threads attempt to copy objects between the same two transactions at the same time but in opposite directions,
     * deadlock could result.
     *
     * @param dest destination transaction
     * @param pobjs the objects to copy; null values are ignored
     * @param copyState tracks which objects have already been copied and whether to remap object ID's
     * @throws DeletedObjectException if any object to be copied does not actually exist
     * @throws DeletedObjectException if any copied object ends up with a reference to an object that does not exist
     *  in {@code dest} through a reference field configured to disallow deleted assignment
     * @throws UnknownTypeException if any source object's type does not exist in {@code dest}
     * @throws SchemaMismatchException if the schema corresponding to any copied object is not identical
     *  in both this instance and {@code dest}
     * @throws StaleTransactionException if this transaction or {@code dest} is no longer usable
     * @throws IllegalArgumentException if any parameter is null
     */
    public void copyTo(PermazenTransaction dest, CopyState copyState, Stream<? extends PermazenObject> pobjs) {
        Preconditions.checkArgument(pobjs != null, "null pobjs");
        this.copyIdStreamTo(dest, copyState, pobjs
          .filter(Objects::nonNull)
          .peek(PermazenTransaction::registerPermazenObject)                    // handle possible re-entrant object cache load
          .map(PermazenObject::getObjId));
    }

    private void copyIdStreamTo(PermazenTransaction dest, CopyState copyState, Stream<ObjId> ids) {

        // Sanity check
        Preconditions.checkArgument(dest != null, "null dest");
        Preconditions.checkArgument(copyState != null, "null copyState");
        Preconditions.checkArgument(ids != null, "null ids");

        // Track deleted assignments while we copy
        final ObjIdMap<DeletedAssignment> deletedAssignments = new ObjIdMap<>();

        // Do the copy
        ids.iterator().forEachRemaining(id -> this.copyTo(copyState, dest, id, deletedAssignments, true));

        // If any deleted assignments remain, grab an arbitrary one and throw an exception
        final Map.Entry<ObjId, DeletedAssignment> entry = deletedAssignments.removeOne();
        if (entry != null) {
            final ObjId targetId = entry.getKey();
            final DeletedAssignment deletedAssignment = entry.getValue();
            final ObjId id = deletedAssignment.getId();
            final ReferenceField field = deletedAssignment.getField();
            throw new DeletedObjectException(targetId, String.format(
              "illegal assignment of deleted object %s (%s) to %s in object %s (%s)",
              targetId, this.tx.getTypeDescription(targetId), field, id, this.tx.getTypeDescription(id)));
        }
    }

    void copyTo(CopyState copyState, PermazenTransaction dest, ObjId srcId,
      ObjIdMap<DeletedAssignment> deletedAssignments, boolean required) {

        // Already copied?
        if (!copyState.markCopied(srcId))
            return;

        // Get source and destination PermazenClass
        final PermazenClass<?> srcPClass = this.pdb.getPermazenClass(srcId);
        final PermazenClass<?> dstPClass = dest.pdb.getPermazenClass(srcPClass.name);

        // Optimization: see if we can disable listener notifications
        boolean enableNotifications = !copyState.isSuppressNotifications();
        if (enableNotifications && dest.isDetached() && dstPClass != null)
            enableNotifications = !dstPClass.onCreateMethods.isEmpty() || !dstPClass.onChangeMethods.isEmpty();

        // Copy object at the core API level
        final ObjIdMap<ReferenceField> coreDeletedAssignments = new ObjIdMap<>();
        try {
            this.tx.copy(srcId, dest.tx, true, enableNotifications, coreDeletedAssignments, copyState.getObjectIdMap());
        } catch (DeletedObjectException e) {
            assert !this.exists(srcId);
            if (required)
                throw e;
            return;
        }

        // Get destination ID
        assert copyState.isCopied(srcId);
        final ObjId dstId = copyState.getDestId(srcId);
        assert dstId.equals(srcId) || copyState.getObjectIdMap().containsKey(srcId);

        // Reset cached fields in the destination PermazenObject, if any
        final PermazenObject dstObject = dest.pobjectCache.getIfExists(dstId);
        if (dstObject != null)
            dstObject.resetCachedFieldValues();

        // Revalidate destination object if needed
        if (dest.validationMode.equals(ValidationMode.AUTOMATIC) && dstPClass != null && dstPClass.requiresDefaultValidation)
            dest.revalidate(Collections.singleton(dstId));

        // Add any deleted assignments from the core API copy to our copy state
        for (Map.Entry<ObjId, ReferenceField> entry : coreDeletedAssignments.entrySet()) {
            assert !copyState.isCopied(entry.getKey());
            deletedAssignments.put(entry.getKey(), new DeletedAssignment(dstId, entry.getValue()));
        }

        // Remove the copied object from the deleted assignments set in our copy state.
        // This fixes up "forward reference" deleted assignments.
        deletedAssignments.remove(dstId);
    }

    /**
     * Find all objects reachable through the specified reference cascades.
     *
     * <p>
     * This method finds all objects reachable from the given starting object through
     * {@linkplain io.permazen.annotation.PermazenField#forwardCascades forward} and
     * {@linkplain io.permazen.annotation.PermazenField#inverseCascades inverse} reference field cascades with the specified names.
     * In other words, a reference field is traversed in the forward or inverse direction if any of the given {@code cascades}
     * are found in the field's
     * {@linkplain io.permazen.annotation.PermazenField#forwardCascades() &#64;PermazenField.forwardCascades()} or
     * {@linkplain io.permazen.annotation.PermazenField#inverseCascades() &#64;PermazenField.inverseCascades()} annotation
     * property, respectively.
     *
     * <p>
     * The {@code visited} set contains the ID's of objects already visited (or is empty if none); these objects will not
     * be traversed. In particular, if {@code id} is in {@code visited}, then this method does nothing.
     * Upon return, {@code visited} will have had all of the new objects found added.
     *
     * <p>
     * All new objects found will be {@linkplain #migrateSchema migrated} to the this transaction's schema if necessary.
     *
     * <p>
     * The {@code maxDistance} parameter can be used to limit the maximum distance of any reachable object, measured
     * in the number of reference field hops from the starting object. If a value other than -1 is given, objects will
     * be visited in breadth-first manner (i.e., ordered by distance) and the search is truncated at {@code maxDistance}
     * hops. If -1 is given, there is no limit and also no implied ordering of the objects in the iteration.
     *
     * @param id starting object ID
     * @param maxDistance the maximum number of reference fields to hop through, or -1 for no limit
     * @param visited on entry objects already visited, on return all objects reached
     * @param cascades zero or more reference cascade names
     * @return iteration of reachable objects
     * @throws DeletedObjectException if any object containing a traversed reference field does not actually exist
     * @throws IllegalArgumentException if {@code maxDistance} is less than -1
     * @throws IllegalArgumentException if any parameter is null
     * @see PermazenObject#cascade PermazenObject.cascade()
     * @see io.permazen.annotation.PermazenField#forwardCascades &#64;PermazenField.forwardCascades()
     * @see io.permazen.annotation.PermazenField#inverseCascades &#64;PermazenField.inverseCascades()
     */
    public Iterator<ObjId> cascade(ObjId id, int maxDistance, ObjIdSet visited, String... cascades) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(cascades != null, "null cascades");
        Preconditions.checkArgument(maxDistance >= -1, "maxDistance < -1");
        Preconditions.checkArgument(visited != null, "null visited");
        for (String cascade : cascades)
            Preconditions.checkArgument(cascade != null, "null cascade");

        // Build initial set
        final ObjIdSet initial = new ObjIdSet();
        if (visited.add(id))
            initial.add(id);

        // Handle breadth-first vs. unordered
        if (maxDistance >= 0) {
            return new AbstractIterator<ObjId>() {

                private ObjIdSet currLevel = initial;
                private ObjIdSet nextLevel = new ObjIdSet();
                private int remainingHops = maxDistance;

                @Override
                protected ObjId computeNext() {

                    // Any remaining at the current level?
                    if (!this.currLevel.isEmpty()) {
                        final ObjId next = this.currLevel.removeOne();
                        if (this.remainingHops > 0)                 // don't expand past the distance limit
                            PermazenTransaction.this.gatherCascadeRefs(next, cascades, visited, this.nextLevel);
                        return next;
                    }

                    // Take it to the next level
                    final ObjIdSet empty = this.currLevel;
                    this.currLevel = this.nextLevel;
                    this.nextLevel = empty;
                    this.remainingHops--;

                    // Anything there?
                    if (this.currLevel.isEmpty())
                        return this.endOfData();

                    // Continue
                    return this.computeNext();
                }
            };
        } else {
            return new AbstractIterator<ObjId>() {
                @Override
                protected ObjId computeNext() {
                    if (!initial.isEmpty()) {
                        final ObjId next = initial.removeOne();
                        PermazenTransaction.this.gatherCascadeRefs(next, cascades, visited, initial);
                        return next;
                    }
                    return this.endOfData();
                }
            };
        }
    }

    // Cascade from object
    private void gatherCascadeRefs(ObjId id, String[] cascades, ObjIdSet visited, ObjIdSet dest) {

        // Migrate schema if needed
        this.tx.migrateSchema(id);

        // Get object type
        final PermazenClass<?> pclass = this.pdb.getPermazenClass(id.getStorageId());

        // Gather references
        this.gatherForwardCascadeRefs(pclass, id, cascades, visited, dest);
        this.gatherInverseCascadeRefs(pclass, id, cascades, visited, dest);
    }

    // Cascade forward from object
    private void gatherForwardCascadeRefs(PermazenClass<?> pclass, ObjId id, String[] cascades, ObjIdSet visited, ObjIdSet dest) {
        for (String cascade : cascades) {
            final List<PermazenReferenceField> fields = pclass.forwardCascadeMap.get(cascade);
            if (fields == null)
                continue;
            fields.forEach(field -> this.addRefs(field.iterateReferences(this.tx, id), visited, dest));
        }
    }

    // Cascade inversely from object
    @SuppressWarnings("unchecked")
    private void gatherInverseCascadeRefs(PermazenClass<?> pclass, ObjId id, String[] cascades, ObjIdSet visited, ObjIdSet dest) {
        for (String cascade : cascades) {
            final Map<Integer, KeyRanges> refMap = pclass.inverseCascadeMap.get(cascade);
            if (refMap == null)
                continue;
            refMap.forEach((storageId, refTypeRanges) -> {

                // Access the index associated with the reference field
                CoreIndex1<ObjId, ObjId> index = (CoreIndex1<ObjId, ObjId>)this.tx.querySimpleIndex(storageId);

                // Restrict references from objects containing the field with the cascade (precomputed via "refTypeRanges")
                index = index.filter(1, refTypeRanges);

                // Find objects referring to "id" through the field
                final NavigableSet<ObjId> refs = index.asMap().get(id);
                if (refs != null)
                    this.addRefs(refs, visited, dest);
            });
        }
    }

    private void addRefs(Iterable<ObjId> refs, ObjIdSet visited, ObjIdSet dest) {
        try (CloseableIterator<ObjId> i = CloseableIterator.wrap(refs.iterator())) {
            while (i.hasNext()) {
                final ObjId ref = i.next();
                if (ref != null && visited.add(ref))
                    dest.add(ref);
            }
        }
    }

// Object/Field Access

    /**
     * Get the Java model object that is associated with this transaction and has the given ID.
     *
     * <p>
     * This method guarantees that for any particular {@code id}, the same Java instance will always be returned
     * by this transaction.
     *
     * <p>
     * <b>A non-null object is always returned, but the corresponding object may not actually exist in this transaction.</b>
     * If not, attempts to access its fields will throw {@link DeletedObjectException}.
     * Use {@link PermazenObject#exists PermazenObject.exists()} to check.
     *
     * <p>
     * Also, it's possible that {@code id} corresponds to an object type that no longer exists in the schema
     * version associated with this transaction. In that case, an {@link UntypedPermazenObject} is returned.
     *
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     * @see #get(ObjId, Class)
     * @see #get(PermazenObject)
     */
    public PermazenObject get(ObjId id) {
        return this.pobjectCache.get(id);
    }

    /**
     * Get the Java model object that is associated with this transaction and has the given ID, cast to the given type.
     *
     * <p>
     * This method guarantees that for any particular {@code id}, the same Java instance will always be returned
     * by this transaction.
     *
     * <p>
     * <b>A non-null object is always returned, but the corresponding object may not actually exist in this transaction.</b>
     * If not, attempts to access its fields will throw {@link DeletedObjectException}.
     * Use {@link PermazenObject#exists PermazenObject.exists()} to check.
     *
     * <p>
     * This method just invokes {@link #get(ObjId)} and then casts the result.
     *
     * @param id object ID
     * @param type expected type
     * @param <T> expected Java model type
     * @return Java model object
     * @throws ClassCastException if the Java model object does not have type {@code type}
     * @throws IllegalArgumentException if either parameter is null
     * @see #get(ObjId)
     * @see #get(PermazenObject)
     */
    public <T> T get(ObjId id, Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        return type.cast(this.get(id));
    }

    /**
     * Get the Java model object with the same object ID as the given {@link PermazenObject} and whose state derives from
     * this transaction.
     *
     * <p>
     * This method can be thought of as a "refresh" operation for objects being imported from other transactions into this one.
     *
     * <p>
     * <b>A non-null object is always returned, but the corresponding object may not actually exist in this transaction.</b>
     * If not, attempts to access its fields will throw {@link DeletedObjectException}.
     * Use {@link PermazenObject#exists PermazenObject.exists()} to check.
     *
     * <p>
     * This method is equivalent to {@code get(pobj.getObjId())} followed by an appropriate cast to type {@code T}.
     *
     * @param pobj Java model object
     * @param <T> expected Java type
     * @return Java model object in this transaction with the same object ID (possibly {@code pobj} itself)
     * @throws IllegalArgumentException if {@code pobj} is null, or not a {@link Permazen} database object
     * @throws ClassCastException if the Java model object in this transaction somehow does not have the same type as {@code pobj}
     * @see #get(ObjId)
     * @see #get(ObjId, Class)
     */
    @SuppressWarnings("unchecked")
    public <T extends PermazenObject> T get(T pobj) {
        return (T)pobj.getModelClass().cast(this.get(pobj.getObjId()));
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
        return this.create(this.pdb.getPermazenClass(type));
    }

    /**
     * Create a new instance of the given type in this transaction.
     *
     * @param pclass object type
     * @param <T> Java model type
     * @return newly created instance
     * @throws IllegalArgumentException if {@code pclass} is null or not valid for this instance
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    public <T> T create(PermazenClass<T> pclass) {
        Preconditions.checkArgument(pclass != null, "null pclass");
        final ObjId id = this.tx.create(pclass.name);
        return pclass.getType().cast(this.get(id));
    }

    /**
     * Delete the object with the given object ID in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link PermazenObject#delete} would be used instead.
     *
     * @param pobj the object to delete
     * @return true if object was found and deleted, false if object was not found
     * @throws io.permazen.core.ReferencedObjectException if the object is referenced by some other object
     *  through a reference field configured for {@link DeleteAction#EXCEPTION}
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code pobj} is null
     */
    public boolean delete(PermazenObject pobj) {

        // Sanity check
        Preconditions.checkArgument(pobj != null, "null pobj");

        // Handle possible re-entrant object cache load
        PermazenTransaction.registerPermazenObject(pobj);

        // Delete object
        final ObjId id = pobj.getObjId();
        final boolean deleted = this.tx.delete(id);

        // Remove object from validation queue if enqueued
        if (deleted) {
            synchronized (this) {
                this.validationQueue.remove(id);
            }
        }

        // Reset cached field values
        if (deleted)
            pobj.resetCachedFieldValues();

        // Done
        return deleted;
    }

    /**
     * Determine whether the object with the given object ID exists in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link PermazenObject#exists} would be used instead.
     *
     * @param id ID of the object to test for existence
     * @return true if object was found, false if object was not found
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code id} is null
     */
    public boolean exists(ObjId id) {
        return this.tx.exists(id);
    }

    /**
     * Recreate the given instance in this transaction.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link PermazenObject#recreate} would be used instead.
     *
     * @param pobj the object to recreate
     * @return true if the object was recreated, false if the object already existed
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code pobj} is null
     */
    public boolean recreate(PermazenObject pobj) {
        PermazenTransaction.registerPermazenObject(pobj);                       // handle possible re-entrant object cache load
        return this.tx.create(pobj.getObjId());
    }

    /**
     * Add the given instance to the validation queue for validation, which will occur either at {@link #commit} time
     * or at the next invocation of {@link #validate}, whichever occurs first.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link PermazenObject#revalidate} would be used instead.
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
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);
        this.validationQueue.clear();
    }

    private synchronized void revalidate(Collection<? extends ObjId> ids, Class<?>... groups) {

        // Sanity checks
        if (!this.tx.isOpen())
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
     * Update the schema of the specified object, if necessary, so that it matches
     * the schema associated with this instance's {@link Permazen}.
     *
     * <p>
     * If a schema change occurs, {@link OnSchemaChange &#64;OnSchemaChange} methods will be invoked
     * prior to this method returning.
     *
     * <p>
     * This method is typically only used by generated classes; normally, {@link PermazenObject#migrateSchema}
     * would be used instead.
     *
     * @param pobj object to update
     * @return true if the object's schema was migrated, false if it was already updated
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code pobj} does not exist in this transaction
     * @throws TypeNotInSchemaException if the current schema does not contain the object's type
     * @throws IllegalArgumentException if {@code pobj} is null
     */
    public boolean migrateSchema(PermazenObject pobj) {
        PermazenTransaction.registerPermazenObject(pobj);                       // handle possible re-entrant object cache load
        return this.tx.migrateSchema(pobj.getObjId());
    }

    /**
     * Ensure the given {@link PermazenObject} is registered in its associated transaction's object cache.
     *
     * <p>
     * This method is used internally, to handle mutations in model class superclass constructors, which will occur
     * before the newly created {@link PermazenObject} is fully constructed and associated with its {@link PermazenTransaction}.
     *
     * @param pobj object to register
     * @throws IllegalArgumentException if {@code pobj} is null
     */
    public static void registerPermazenObject(PermazenObject pobj) {
        pobj.getPermazenTransaction().pobjectCache.register(pobj);
    }

    /**
     * Read a simple field.
     *
     * <p>
     * This returns the value returned by {@link Transaction#readSimpleField Transaction.readSimpleField()}
     * with {@link ObjId}s converted into {@link PermazenObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.PermazenField &#64;PermazenField} getter override methods
     * and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param fieldName the name of the {@link PermazenSimpleField}
     * @param migrateSchema true to automatically migrate the object's schema, false to not change it
     * @return the value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link PermazenSimpleField} corresponding to {@code fieldName} in {@code id} exists
     * @throws TypeNotInSchemaException if {@code migrateSchema} is true but the object has a type
     *  that does not exist in this instance's schema
     * @throws IllegalArgumentException if {@code id} or {@code fieldName} is null
     */
    public Object readSimpleField(ObjId id, String fieldName, boolean migrateSchema) {
        return this.convert(
          this.pdb.getField(id, fieldName, PermazenSimpleField.class).getConverter(this),
          this.tx.readSimpleField(id, fieldName, migrateSchema));
    }

    /**
     * Write a simple field.
     *
     * <p>
     * This writes the value via {@link Transaction#writeSimpleField Transaction.writeSimpleField()}
     * after converting {@link PermazenObject}s into {@link ObjId}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.PermazenField &#64;PermazenField} setter override methods
     * and not normally invoked directly by user code.
     *
     * @param pobj object containing the field
     * @param fieldName the name of the {@link PermazenSimpleField}
     * @param value new value for the field
     * @param migrateSchema true to automatically migrate the object's schema, false to not change it
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if {@code pobj} does not exist in this transaction
     * @throws UnknownFieldException if no {@link PermazenSimpleField} corresponding to {@code fieldName} exists
     * @throws TypeNotInSchemaException if {@code migrateSchema} is true but {@code pobj} has a type
     *  that does not exist in this instance's schema
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for the field
     * @throws IllegalArgumentException if {@code pobj} or {@code fieldName} is null
     */
    public void writeSimpleField(PermazenObject pobj, String fieldName, Object value, boolean migrateSchema) {
        PermazenTransaction.registerPermazenObject(pobj);                       // handle possible re-entrant object cache load
        final ObjId id = pobj.getObjId();
        final Converter<?, ?> converter = this.pdb.getField(id, fieldName, PermazenSimpleField.class).getConverter(this);
        if (converter != null)
            value = this.convert(converter.reverse(), value);
        this.tx.writeSimpleField(id, fieldName, value, migrateSchema);
    }

    /**
     * Read a counter field.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.PermazenField &#64;PermazenField} getter override methods
     * and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param fieldName the name of the {@link PermazenCounterField}
     * @param migrateSchema true to automatically migrate the object's schema, false to not change it
     * @return the value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link PermazenCounterField} corresponding to {@code fieldName} exists
     * @throws TypeNotInSchemaException if {@code migrateSchema} is true but the object has a type
     *  that does not exist in this instance's schema
     * @throws IllegalArgumentException if {@code id} or {@code fieldName} is null
     */
    public Counter readCounterField(ObjId id, String fieldName, boolean migrateSchema) {
        this.pdb.getField(id, fieldName, PermazenCounterField.class);             // validate encoding
        if (migrateSchema)
            this.tx.migrateSchema(id);
        return new Counter(this.tx, id, fieldName, migrateSchema);
    }

    /**
     * Read a set field.
     *
     * <p>
     * This returns the set returned by {@link Transaction#readSetField Transaction.readSetField()} with
     * {@link ObjId}s converted into {@link PermazenObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.PermazenSetField &#64;PermazenSetField}
     * getter override methods and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param fieldName the name of the {@link PermazenSetField}
     * @param migrateSchema true to automatically migrate the object's schema, false to not change it
     * @return the value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link PermazenSetField} corresponding to {@code fieldName} exists
     * @throws TypeNotInSchemaException if {@code migrateSchema} is true but the object has a type
     *  that does not exist in this instance's schema
     * @throws IllegalArgumentException if {@code id} or {@code fieldName} is null
     */
    public NavigableSet<?> readSetField(ObjId id, String fieldName, boolean migrateSchema) {
        return this.convert(
          this.pdb.getField(id, fieldName, PermazenSetField.class).getConverter(this),
          this.tx.readSetField(id, fieldName, migrateSchema));
    }

    /**
     * Read a list field.
     *
     * <p>
     * This returns the list returned by {@link Transaction#readListField Transaction.readListField()} with
     * {@link ObjId}s converted into {@link PermazenObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.PermazenListField &#64;PermazenListField}
     * getter override methods and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param fieldName the name of the {@link PermazenListField}
     * @param migrateSchema true to automatically migrate the object's schema, false to not change it
     * @return the value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link PermazenListField} corresponding to {@code fieldName} exists
     * @throws TypeNotInSchemaException if {@code migrateSchema} is true but the object has a type
     *  that does not exist in this instance's schema
     * @throws IllegalArgumentException if {@code id} or {@code fieldName} is null
     */
    public List<?> readListField(ObjId id, String fieldName, boolean migrateSchema) {
        return this.convert(
          this.pdb.getField(id, fieldName, PermazenListField.class).getConverter(this),
          this.tx.readListField(id, fieldName, migrateSchema));
    }

    /**
     * Read a map field.
     *
     * <p>
     * This returns the map returned by {@link Transaction#readMapField Transaction.readMapField()} with
     * {@link ObjId}s converted into {@link PermazenObject}s, etc.
     *
     * <p>
     * This method is used by generated {@link io.permazen.annotation.PermazenMapField &#64;PermazenMapField}
     * getter override methods and not normally invoked directly by user code.
     *
     * @param id ID of the object containing the field
     * @param fieldName the name of the {@link PermazenMapField}
     * @param migrateSchema true to automatically migrate the object's schema, false to not change it
     * @return the value of the field in the object
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws DeletedObjectException if the object does not exist in this transaction
     * @throws UnknownFieldException if no {@link PermazenMapField} corresponding to {@code fieldName} exists
     * @throws TypeNotInSchemaException if {@code migrateSchema} is true but the object has a type
     *  that does not exist in this instance's schema
     * @throws IllegalArgumentException if {@code id} or {@code fieldName} is null
     */
    public NavigableMap<?, ?> readMapField(ObjId id, String fieldName, boolean migrateSchema) {
        return this.convert(
          this.pdb.getField(id, fieldName, PermazenMapField.class).getConverter(this),
          this.tx.readMapField(id, fieldName, migrateSchema));
    }

// Reference Path Access

    /**
     * Find all target objects that are reachable from the given starting object set through the specified {@link ReferencePath}.
     *
     * @param path reference path
     * @param startObjects starting objects
     * @return read-only set of objects reachable from {@code startObjects} via {@code path}
     * @throws UnknownFieldException if {@code path} contains an unknown field
     * @throws IllegalArgumentException if either parameter is null
     * @see ReferencePath
     */
    public NavigableSet<PermazenObject> followReferencePath(ReferencePath path, Stream<? extends PermazenObject> startObjects) {
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(startObjects != null, "null startObjects");
        final NavigableSet<ObjId> ids = this.tx.followReferencePath(startObjects.map(this.referenceConverter),
          path.getReferenceFields(), path.getPathKeyRanges());
        return new ConvertedNavigableSet<PermazenObject, ObjId>(ids, this.referenceConverter);
    }

    /**
     * Find all starting objects that refer to any object in the given target set through the specified {@link ReferencePath}.
     *
     * @param path reference path
     * @param targetObjects target objects
     * @return read-only set of objects that refer to any of the {@code targetObjects} via {@code path}
     * @throws UnknownFieldException if {@code path} contains an unknown field
     * @throws IllegalArgumentException if either parameter is null
     * @see ReferencePath
     */
    public NavigableSet<PermazenObject> invertReferencePath(ReferencePath path, Stream<? extends PermazenObject> targetObjects) {
        Preconditions.checkArgument(path != null, "null path");
        Preconditions.checkArgument(targetObjects != null, "null targetObjects");
        final NavigableSet<ObjId> ids = this.tx.invertReferencePath(path.getReferenceFields(), path.getPathKeyRanges(),
          targetObjects.map(this.referenceConverter));
        return new ConvertedNavigableSet<PermazenObject, ObjId>(ids, this.referenceConverter);
    }

// Index Access

    /**
     * Get the index on a simple field. The simple field may be a sub-field of a complex field.
     *
     * @param targetType Java type containing the indexed field; may also be any super-type (e.g., an interface type),
     *  as long as {@code fieldName} is not ambiguous among all sub-types
     * @param fieldName name of the indexed field; for complex fields,
     *  must include the sub-field name (e.g., {@code "mylist.element"}, {@code "mymap.key"})
     *  unless there is only one sub-field (i.e., sets and lists but not maps)
     * @param valueType the Java type corresponding to the field value
     * @param <T> Java type containing the field
     * @param <V> Java type corresponding to the indexed field
     * @return read-only, real-time view of field values mapped to sets of objects having that value in the field
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V, T> Index1<V, T> querySimpleIndex(Class<T> targetType, String fieldName, Class<V> valueType) {
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);
        final IndexQuery info = this.pdb.getIndexQuery(new IndexQuery.Key(fieldName, false, targetType, valueType));
        final PermazenSimpleField pfield = (PermazenSimpleField)info.schemaItem;
        final CoreIndex1<?, ObjId> index = info.applyFilters(this.tx.querySimpleIndex(pfield.storageId));
        final Converter<?, ?> valueConverter = Util.reverse(pfield.getConverter(this));
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex1(index, valueConverter, targetConverter);
    }

    /**
     * Get the composite index on a list field that includes list indices.
     *
     * @param targetType type containing the indexed field; may also be any super-type (e.g., an interface type),
     *  as long as {@code fieldName} is not ambiguous among all sub-types
     * @param fieldName name of the indexed field; must include {@code "element"} sub-field name (e.g., {@code "mylist.element"})
     * @param valueType the Java type corresponding to list elements
     * @param <T> Java type containing the field
     * @param <V> Java type corresponding to the indexed list's element field
     * @return read-only, real-time view of field values, objects having that value in the field, and corresponding list indices
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V, T> Index2<V, T, Integer> queryListElementIndex(Class<T> targetType, String fieldName, Class<V> valueType) {
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);
        final IndexQuery info = this.pdb.getIndexQuery(new IndexQuery.Key(fieldName, false, targetType, valueType));
        final PermazenSimpleField pfield = (PermazenSimpleField)info.schemaItem;
        if (!(pfield.getParentField() instanceof PermazenListField))
            throw new IllegalArgumentException(String.format("field \"%s\" is not a list element sub-field", fieldName));
        final CoreIndex2<?, ObjId, Integer> index = info.applyFilters(this.tx.queryListElementIndex(pfield.storageId));
        final Converter<?, ?> valueConverter = Util.reverse(pfield.getConverter(this));
        final Converter<Integer, Integer> indexConverter = Converter.<Integer>identity();
        final Converter<T, ObjId> targetConverter = new ReferenceConverter<T>(this, targetType);
        return new ConvertedIndex2(index, valueConverter, targetConverter, indexConverter);
    }

    /**
     * Get the composite index on a map value field that includes map keys.
     *
     * @param targetType type containing the indexed field; may also be any super-type (e.g., an interface type),
     *  as long as {@code fieldName} is not ambiguous among all sub-types
     * @param fieldName name of the indexed field; must include {@code "value"} sub-field name (e.g., {@code "mymap.value"})
     * @param valueType the Java type corresponding to map values
     * @param keyType the Java type corresponding to map keys
     * @param <T> Java type containing the field
     * @param <V> Java type corresponding to the indexed map's value field
     * @param <K> Java type corresponding to the indexed map's key field
     * @return read-only, real-time view of map values, objects having that value in the map field, and corresponding map keys
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V, T, K> Index2<V, T, K> queryMapValueIndex(Class<T> targetType,
      String fieldName, Class<V> valueType, Class<K> keyType) {
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);
        final IndexQuery info = this.pdb.getIndexQuery(
          new IndexQuery.Key(fieldName, false, targetType, valueType, keyType));
        final PermazenSimpleField pfield = (PermazenSimpleField)info.schemaItem;
        final PermazenMapField parentField;
        if (!(pfield.getParentField() instanceof PermazenMapField)
          || pfield != (parentField = (PermazenMapField)pfield.getParentField()).valueField)
            throw new IllegalArgumentException(String.format("field \"%s\" is not a map value sub-field", fieldName));
        final CoreIndex2<?, ObjId, ?> index = info.applyFilters(this.tx.queryMapValueIndex(pfield.storageId));
        final Converter<?, ?> valueConverter = Util.reverse(pfield.getConverter(this));
        final Converter<?, ?> keyConverter = Util.reverse(parentField.keyField.getConverter(this));
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
     * @param <T> Java type containing the field
     * @param <V1> Java type corresponding to the first indexed field
     * @param <V2> Java type corresponding to the second indexed field
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V1, V2, T> Index2<V1, V2, T> queryCompositeIndex(Class<T> targetType,
      String indexName, Class<V1> value1Type, Class<V2> value2Type) {
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);
        final IndexQuery info = this.pdb.getIndexQuery(
          new IndexQuery.Key(indexName, true, targetType, value1Type, value2Type));
        final PermazenCompositeIndex pindex = (PermazenCompositeIndex)info.schemaItem;
        final CoreIndex2<?, ?, ObjId> index = info.applyFilters(this.tx.queryCompositeIndex2(pindex.storageId));
        final Converter<?, ?> value1Converter = Util.reverse(pindex.pfields.get(0).getConverter(this));
        final Converter<?, ?> value2Converter = Util.reverse(pindex.pfields.get(1).getConverter(this));
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
     * @param <T> Java type containing the field
     * @param <V1> Java type corresponding to the first indexed field
     * @param <V2> Java type corresponding to the second indexed field
     * @param <V3> Java type corresponding to the third indexed field
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V1, V2, V3, T> Index3<V1, V2, V3, T> queryCompositeIndex(Class<T> targetType,
      String indexName, Class<V1> value1Type, Class<V2> value2Type, Class<V3> value3Type) {
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);
        final IndexQuery info = this.pdb.getIndexQuery(
          new IndexQuery.Key(indexName, true, targetType, value1Type, value2Type, value3Type));
        final PermazenCompositeIndex pindex = (PermazenCompositeIndex)info.schemaItem;
        final CoreIndex3<?, ?, ?, ObjId> index = info.applyFilters(this.tx.queryCompositeIndex3(pindex.storageId));
        final Converter<?, ?> value1Converter = Util.reverse(pindex.pfields.get(0).getConverter(this));
        final Converter<?, ?> value2Converter = Util.reverse(pindex.pfields.get(1).getConverter(this));
        final Converter<?, ?> value3Converter = Util.reverse(pindex.pfields.get(2).getConverter(this));
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
     * @param <T> Java type containing the field
     * @param <V1> Java type corresponding to the first indexed field
     * @param <V2> Java type corresponding to the second indexed field
     * @param <V3> Java type corresponding to the third indexed field
     * @param <V4> Java type corresponding to the fourth indexed field
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if any parameter is null, or invalid
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V1, V2, V3, V4, T> Index4<V1, V2, V3, V4, T> queryCompositeIndex(Class<T> targetType,
      String indexName, Class<V1> value1Type, Class<V2> value2Type, Class<V3> value3Type, Class<V4> value4Type) {
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);
        final IndexQuery info = this.pdb.getIndexQuery(
          new IndexQuery.Key(indexName, true, targetType, value1Type, value2Type, value3Type, value4Type));
        final PermazenCompositeIndex pindex = (PermazenCompositeIndex)info.schemaItem;
        final CoreIndex4<?, ?, ?, ?, ObjId> index = info.applyFilters(this.tx.queryCompositeIndex4(pindex.storageId));
        final Converter<?, ?> value1Converter = Util.reverse(pindex.pfields.get(0).getConverter(this));
        final Converter<?, ?> value2Converter = Util.reverse(pindex.pfields.get(1).getConverter(this));
        final Converter<?, ?> value3Converter = Util.reverse(pindex.pfields.get(2).getConverter(this));
        final Converter<?, ?> value4Converter = Util.reverse(pindex.pfields.get(3).getConverter(this));
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
     * @param storageId indexed {@link PermazenSimpleField}'s storage ID
     * @return read-only, real-time view of the fields' values and the objects having those values in the fields
     * @throws IllegalArgumentException if {@code storageId} does not correspond to an indexed field or composite index
     * @throws StaleTransactionException if this transaction is no longer usable
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Index<?> queryIndex(int storageId) {

        // Check transcaction
        if (!this.tx.isOpen())
            throw new StaleTransactionException(this.tx);

        // Find index
        final PermazenSchemaItem schemaItem = this.pdb.indexesByStorageId.get(storageId);
        if (schemaItem == null) {
            throw new IllegalArgumentException(String.format(
              "no composite index or simple indexed field exists with storage ID %d", storageId));
        }

        // Handle a composite index
        if (schemaItem instanceof PermazenCompositeIndex) {
            final PermazenCompositeIndex index = (PermazenCompositeIndex)schemaItem;
            switch (index.pfields.size()) {
            case 2:
            {
                final Converter<?, ?> value1Converter = Util.reverse(index.pfields.get(0).getConverter(this));
                final Converter<?, ?> value2Converter = Util.reverse(index.pfields.get(1).getConverter(this));
                return new ConvertedIndex2(this.tx.queryCompositeIndex2(index.storageId),
                  value1Converter, value2Converter, this.referenceConverter);
            }
            case 3:
            {
                final Converter<?, ?> value1Converter = Util.reverse(index.pfields.get(0).getConverter(this));
                final Converter<?, ?> value2Converter = Util.reverse(index.pfields.get(1).getConverter(this));
                final Converter<?, ?> value3Converter = Util.reverse(index.pfields.get(2).getConverter(this));
                return new ConvertedIndex3(this.tx.queryCompositeIndex3(index.storageId),
                  value1Converter, value2Converter, value3Converter, this.referenceConverter);
            }
            case 4:
            {
                final Converter<?, ?> value1Converter = Util.reverse(index.pfields.get(0).getConverter(this));
                final Converter<?, ?> value2Converter = Util.reverse(index.pfields.get(1).getConverter(this));
                final Converter<?, ?> value3Converter = Util.reverse(index.pfields.get(2).getConverter(this));
                final Converter<?, ?> value4Converter = Util.reverse(index.pfields.get(3).getConverter(this));
                return new ConvertedIndex4(this.tx.queryCompositeIndex4(index.storageId),
                  value1Converter, value2Converter, value3Converter, value4Converter, this.referenceConverter);
            }
            // COMPOSITE-INDEX
            default:
                throw new RuntimeException("internal error");
            }
        }

        // Must be a simple field index
        return ((PermazenSimpleField)schemaItem).getIndex(this);
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
     * @throws io.permazen.kv.RetryKVTransactionException from {@link KVTransaction#commit KVTransaction.commit()}
     * @throws ValidationException if a validation error is detected
     * @throws IllegalStateException if this method is invoked re-entrantly from within a validation check
     */
    public synchronized void commit() {

        // Sanity check
        if (!this.tx.isOpen())
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
     * @see Transaction#isOpen
     */
    public boolean isOpen() {
        return this.tx.isOpen();
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
     * <b>Note:</b> if this transaction was created with {@link ValidationMode#DISABLED}, then this method does nothing.
     *
     * @throws io.permazen.kv.RetryKVTransactionException from {@link KVTransaction#commit KVTransaction.commit()}
     * @throws ValidationException if a validation error is detected
     * @throws IllegalStateException if transaction commit is already in progress
     * @throws StaleTransactionException if this transaction is no longer usable
     *
     * @see PermazenObject#revalidate
     */
    public void validate() {

        // Sanity check
        if (!this.tx.isOpen())
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
        final PermazenTransaction previous = CURRENT.get();
        CURRENT.set(this);
        try {
            action.run();
        } finally {
            CURRENT.set(previous);
        }
    }

    /**
     * Invoke the given {@link Supplier} with this instance as the {@linkplain #getCurrent current transaction}.
     *
     * <p>
     * If another instance is currently associated with the current thread, it is set aside for the duration of
     * {@code action}'s execution, and then restored when {@code action} is finished (regardless of outcome).
     *
     * @param action action to perform
     * @return result from action
     * @throws IllegalArgumentException if {@code action} is null
     */
    public <T> T performAction(Supplier<T> action) {
        Preconditions.checkArgument(action != null, "null action");
        final PermazenTransaction previous = CURRENT.get();
        CURRENT.set(this);
        try {
            return action.get();
        } finally {
            CURRENT.set(previous);
        }
    }

    /**
     * Apply weaker transaction consistency while performing the given action, if supported.
     *
     * <p>
     * Some key/value implementations support reads with weaker consistency guarantees. These reads generate fewer
     * transaction conflicts but return possibly out-of-date information. Depending on the implementation, when operating
     * in this mode writes may not be supported and may generate a {@link IllegalStateException} or just be ignored.
     *
     * <p>
     * The weaker consistency is only applied for the current thread, and it ends when this method returns.
     *
     * <p>
     * <b>This method is for experts only</b>; inappropriate use can result in a corrupted database.
     * You should not make any changes to the database after this method returns based on any information
     * read by the {@code action}.
     *
     * @param action the action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    public void withWeakConsistency(Runnable action) {
        this.tx.withWeakConsistency(action);
    }

// Internal methods

    @SuppressWarnings("unchecked")
    private void doValidate() {
        final ValidatorFactory validatorFactory = this.pdb.validatorFactory;
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

            // Does the object still exist?
            if (!this.tx.exists(id))
                continue;

            // Get object and verify type exists in current schema (if not, the remaining validation is unneccessary)
            final PermazenObject pobj = this.get(id);
            final PermazenClass<?> pclass = this.pdb.pclassesByStorageId.get(id.getStorageId());
            if (pclass == null)
                return;

            // Do early @OnValidate method validation, bailing out if an @OnValidate method deletes the object
            if (!this.doOnValidate(pclass.earlyOnValidateMethods, pobj, validationGroups))
                continue;

            // Do singleton validation
            if (pclass.singleton) {
                final AbstractKVNavigableSet<ObjId> ids = (AbstractKVNavigableSet<ObjId>)this.tx.getAll(pclass.name);
                try (CloseableIterator<ObjId> i = ids.iterator()) {
                    while (i.hasNext()) {
                        final ObjId id2 = i.next();
                        if (id2.equals(id))
                            continue;
                        throw new ValidationException(pobj, String.format(
                          "singleton constraint on type \"%s\" failed for object %s: object %s also exists",
                          pclass.name, id, id2));
                    }
                }
            }

            // Do JSR 303 validation (if any)
            if (validator != null && pclass.elementRequiringJSR303Validation != null) {

                // Run validator
                final Set<ConstraintViolation<PermazenObject>> violations;
                try {
                    violations = new ValidationContext<PermazenObject>(pobj, validationGroups).validate(validator);
                } catch (RuntimeException e) {
                    final Throwable rootCause = Throwables.getRootCause(e);
                    if (rootCause instanceof KVDatabaseException)
                        throw (KVDatabaseException)rootCause;
                    throw e;
                }
                if (!violations.isEmpty()) {
                    throw new ValidationException(pobj, violations, String.format(
                      "validation error for object %s of type \"%s\":%n%s",
                      id, pclass.name, ValidationUtil.describe(violations)));
                }

                // It's posible (though unlikely) that a JSR 303 validation could have deleted the object, so check for that
                if (!this.tx.exists(id))
                    continue;
            }

            // Do simple and composite field uniqueness validation
            if ((!pclass.uniqueConstraintFields.isEmpty() || !pclass.uniqueConstraintCompositeIndexes.isEmpty())
              && Util.isAnyGroupBeingValidated(DEFAULT_AND_UNIQUENESS_CLASS_ARRAY, validationGroups)) {

                // Check simple index uniqueness constraints
                for (PermazenSimpleField pfield : pclass.uniqueConstraintFields) {
                    assert pfield.indexed;
                    assert pfield.unique;

                    // Get field's (core API) value
                    final Object value = this.tx.readSimpleField(id, pfield.name, false);

                    // Is this value excluded from the uniqueness constraint?
                    if (pfield.uniqueExcludes != null && pfield.uniqueExcludes.matches(value))
                        continue;

                    // Query core API index to find other objects with the same value in the field, but restrict the search to
                    // only include those types having the annotated method, not some other method with the same name/storage ID.
                    final IndexQuery info = this.pdb.getIndexQuery(new IndexQuery.Key(pfield));
                    final CoreIndex1<?, ObjId> index = info.applyFilters(this.tx.querySimpleIndex(pfield.storageId));

                    // Seach for other objects with the same value in the field and report violation if any are found
                    final List<ObjId> conflictors = this.findUniqueConflictors(id, index.asMap().get(value));
                    if (!conflictors.isEmpty()) {
                        throw new ValidationException(pobj, String.format(
                          "uniqueness constraint on %s failed for object %s: field value %s is also shared by object(s) %s",
                          pfield, id, value, conflictors));
                    }
                }

                // Check composite index uniqueness constraints
            compositeIndexUniqueLoop:
                for (PermazenCompositeIndex pindex : pclass.uniqueConstraintCompositeIndexes) {
                    assert pindex.unique;

                    // Get field (core API) values
                    final int numFields = pindex.pfields.size();
                    final List<Object> values = new ArrayList<>(numFields);
                    for (PermazenSimpleField pfield : pindex.pfields)
                        values.add(this.tx.readSimpleField(id, pfield.name, false));

                    // Is this combination of values excluded from the uniqueness constraint?
                    for (List<ValueMatch<?>> fieldMatches : pindex.uniqueExcludes) {
                        boolean allFieldsMatched = true;
                        for (int i = 0; i < numFields; i++) {
                            if (!fieldMatches.get(i).matches(values.get(i))) {
                                allFieldsMatched = false;
                                break;
                            }
                        }
                        if (allFieldsMatched)
                            continue compositeIndexUniqueLoop;
                    }

                    // Query core API index to find all objects with the same values in the fields
                    final IndexQuery info = this.pdb.getIndexQuery(new IndexQuery.Key(pindex));
                    final PermazenCompositeIndex index = (PermazenCompositeIndex)info.schemaItem;
                    final NavigableSet<ObjId> ids;
                    switch (numFields) {
                    case 2:
                        final CoreIndex2<Object, Object, ObjId> coreIndex2
                          = (CoreIndex2<Object, Object, ObjId>)this.tx.queryCompositeIndex2(index.storageId);
                        ids = info.applyFilters(coreIndex2).asMap().get(new Tuple2<Object, Object>(values.get(0), values.get(1)));
                        break;
                    case 3:
                        final CoreIndex3<Object, Object, Object, ObjId> coreIndex3
                          = (CoreIndex3<Object, Object, Object, ObjId>)this.tx.queryCompositeIndex3(index.storageId);
                        ids = info.applyFilters(coreIndex3).asMap().get(
                          new Tuple3<Object, Object, Object>(values.get(0), values.get(1), values.get(2)));
                        break;
                    case 4:
                        final CoreIndex4<Object, Object, Object, Object, ObjId> coreIndex4
                          = (CoreIndex4<Object, Object, Object, Object, ObjId>)this.tx.queryCompositeIndex4(index.storageId);
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
                        throw new ValidationException(pobj, String.format(
                          "uniqueness constraint on composite index \"%s\" failed for object %s:"
                          + " field value combination %s is also shared by object(s) %s",
                          pindex.name, id, values, conflictors));
                    }
                }
            }

            // Do late @OnValidate method validation
            this.doOnValidate(pclass.lateOnValidateMethods, pobj, validationGroups);
        }
    }

    // Do @OnValidate method validation; return false if it results in object being deleted
    private <T> boolean doOnValidate(Set<OnValidateScanner<T>.MethodInfo> infos, PermazenObject pobj, Class<?>[] validationGroups) {
        for (OnValidateScanner<?>.MethodInfo info : infos) {
            Class<?>[] methodGroups = info.getAnnotation().groups();
            if (methodGroups.length == 0)
                methodGroups = DEFAULT_CLASS_ARRAY;
            if (!Util.isAnyGroupBeingValidated(methodGroups, validationGroups))
                continue;
            Util.invoke(info.getMethod(), pobj);
            if (!this.tx.exists(pobj.getObjId()))
                return false;
        }
        return true;
    }

    // Find some duplicates that shouldn't be there, if any
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

// ValidateOnCreateListener

    private static class ValidateOnCreateListener implements CreateListener {

        @Override
        public void onCreate(Transaction tx, ObjId id) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            ptx.validateOnCreate(id);
        }
    }

    private void validateOnCreate(ObjId id) {

        // Get PermazenClass, if known
        final PermazenClass<?> pclass;
        try {
            pclass = this.pdb.getPermazenClass(id);
        } catch (TypeNotInSchemaException e) {
            return;                                             // object type does not exist in our schema
        }
        assert this.validationMode == ValidationMode.AUTOMATIC && pclass.requiresDefaultValidation;

        // Enqueue for revalidation
        this.revalidate(Collections.singleton(id));
    }

// InternalSchemaChangeListener

    private static class InternalSchemaChangeListener implements SchemaChangeListener {

        @Override
        public void onSchemaChange(Transaction tx, ObjId id,
          SchemaId oldSchemaId, SchemaId newSchemaId, Map<String, Object> oldValues) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            ptx.doOnSchemaChange(id, oldSchemaId, newSchemaId, oldValues);
        }
    }

    private void doOnSchemaChange(ObjId id, SchemaId oldSchemaId, SchemaId newSchemaId, Map<String, Object> oldValues) {

        // Get PermazenClass, if known
        final PermazenClass<?> pclass;
        try {
            pclass = this.pdb.getPermazenClass(id);
        } catch (TypeNotInSchemaException e) {
            return;                                             // object type does not exist in our schema
        }

        // Enqueue for revalidation
        if (this.validationMode == ValidationMode.AUTOMATIC && pclass.requiresDefaultValidation)
            this.revalidate(Collections.singleton(id));

        // Skip the rest if there are no fields to convert and no @OnSchemaChange methods
        if (pclass.upgradeConversionFields.isEmpty() && pclass.onSchemaChangeMethods.isEmpty())
            return;

        // Get old and new object type info
        final Schema oldSchema = this.tx.getSchemaBundle().getSchema(oldSchemaId);
        final Schema newSchema = this.tx.getSchema();
        assert newSchemaId.equals(newSchema.getSchemaId());
        final ObjType oldObjType = oldSchema.getObjType(id.getStorageId());
        final ObjType newObjType = newSchema.getObjType(id.getStorageId());

        // Auto-convert any upgrade-convertable fields
        for (PermazenField pfield0 : pclass.upgradeConversionFields) {
            final String fieldName = pfield0.name;

            // Find the old version of the field, if any
            final Field<?> oldField0;
            try {
                oldField0 = oldObjType.getField(fieldName);
            } catch (UnknownFieldException e) {
                continue;
            }

            // Get old field value
            final Object oldValue = oldValues.get(fieldName);

            // Handle conversion to counter field from counter field or simple numeric field
            if (pfield0 instanceof PermazenCounterField) {

                // Get new counter field
                final PermazenCounterField pfield = (PermazenCounterField)pfield0;
                assert pfield.upgradeConversion.isConvertsValues();

                // Handle trivial conversion from counter -> counter
                if (oldField0 instanceof CounterField)
                    continue;

                // Handle conversion from numeric simple -> counter
                if (oldField0 instanceof SimpleField) {
                    final SimpleField<?> oldField = (SimpleField)oldField0;
                    final Encoding<?> oldEncoding = oldField.getEncoding();
                    if (Number.class.isAssignableFrom(oldEncoding.getTypeToken().wrap().getRawType())) {
                        final Number value = (Number)oldValue;
                        if (value != null)
                            this.tx.writeCounterField(id, fieldName, value.longValue(), false);
                        continue;
                    }
                }

                // Conversion is not possible
                if (pfield.upgradeConversion.isRequireConversion()) {
                    throw new UpgradeConversionException(id, fieldName, String.format(
                      "automatic conversion from the old schema %s %s to %s is not supported,"
                      + " but the upgrade conversion policy is configured as %s",
                      oldValue != null ? "non-numeric " : "null ", oldField0, pfield, pfield.upgradeConversion));
                }
                continue;
            }

            // Get new field, which must be simple
            final PermazenSimpleField pfield = (PermazenSimpleField)pfield0;
            assert pfield.upgradeConversion.isConvertsValues();
            final SimpleField<?> newField = (SimpleField)newObjType.getField(fieldName);

            // Handle conversion from counter field to numeric simple field
            if (oldField0 instanceof CounterField) {
                this.doConvertAndSetField(id, oldField0,
                  this.tx.getDatabase().getEncodingRegistry().getEncoding(TypeToken.of(long.class)),
                  newField, oldValue, pfield.upgradeConversion);
                continue;
            }

            // If the old field is not a simple field, we can't convert
            if (!(oldField0 instanceof SimpleField))
                continue;
            final SimpleField<?> oldField = (SimpleField<?>)oldField0;

            // Convert the old field value and update the field
            this.convertAndSetField(id, oldField, newField, oldValue, pfield.upgradeConversion);
        }

        // Skip the rest if there are no @OnSchemaChange methods
        if (pclass.onSchemaChangeMethods.isEmpty())
            return;

        // Convert old field values from core API objects to JDB layer objects, but do not convert EnumValue objects
        final Map<String, Object> convertedOldValues = Maps.transformEntries(oldValues,
          (fieldName, oldValue) -> this.convertOldSchemaValue(id, oldObjType.getField(fieldName), oldValue));

        // Get the object that was upgraded
        final PermazenObject pobj = this.get(id);

        // Invoke listener methods
        for (OnSchemaChangeScanner<?>.MethodInfo info0 : pclass.onSchemaChangeMethods) {
            final OnSchemaChangeScanner<?>.SchemaChangeMethodInfo info = (OnSchemaChangeScanner<?>.SchemaChangeMethodInfo)info0;
            info.invoke(pobj, convertedOldValues, oldSchemaId, newSchemaId);
        }
    }

    private <OT, NT> void convertAndSetField(ObjId id, SimpleField<OT> oldField,
      SimpleField<NT> newField, Object oldValue, UpgradeConversionPolicy policy) {
        assert policy.isConvertsValues();

        // If the old and new encodings are equal, there's nothing to do
        final Encoding<OT> oldEncoding = oldField.getEncoding();
        final Encoding<NT> newEncoding = newField.getEncoding();
        if (newEncoding.equals(oldEncoding))
            return;

        // Perform conversion
        this.doConvertAndSetField(id, oldField, oldEncoding, newField, oldValue, policy);
    }

    private <OT, NT> void doConvertAndSetField(ObjId id, Field<?> oldField,
      Encoding<OT> oldEncoding, SimpleField<NT> newField, Object oldValue0, UpgradeConversionPolicy policy) {

        // Validate old value
        final OT oldValue = oldEncoding.validate(oldValue0);

        // Get the new encoding
        final Encoding<NT> newEncoding = newField.getEncoding();

        // Attempt conversion
        final NT newValue;
        try {
            newValue = newEncoding.convert(oldEncoding, oldValue);
        } catch (IllegalArgumentException e) {
            if (policy.isRequireConversion()) {
                throw new UpgradeConversionException(id, newField.getName(), String.format(
                  "the value %s in the old schema %s could not be automatically converted to the new type %s,"
                  + " but the upgrade conversion policy is configured as %s",
                  oldEncoding.toString(oldValue), oldField, newEncoding.getTypeToken(), policy), e);
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
     * Convert a core API field value from a different schema.
     * We need to do this to convert the old field values after a schema migration.
     *
     * @param id originating object
     * @param field originating core API field
     * @param value field value
     * @return converted value
     */
    private Object convertOldSchemaValue(ObjId id, Field<?> field, Object value) {

        // Null always converts to null
        if (value == null)
            return null;

        // Convert the value
        return this.convert(field.visit(new OldSchemaValueConverterBuilder()), value);
    }

// OldSchemaValueConverterBuilder

    /**
     * Builds a {@link Converter} for core API {@link Field} that converts, in the forward direction, core API values
     * into {@link Permazen} values, based only on the core API {@link Field}. That means we don't convert
     * {@link EnumValue}s. In the case of reference fields, the original Java type may no
     * longer be available; such values are converted to {@link UntypedPermazenObject}.
     *
     * <p>
     * Returns null if no conversion is necessary.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private class OldSchemaValueConverterBuilder implements FieldSwitch<Converter<?, ?>> {

        @Override
        public Converter<?, ?> caseReferenceField(ReferenceField field) {
            return PermazenTransaction.this.referenceConverter.reverse();
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
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenField pfield = ptx.pdb.getField(id, field.getName(), PermazenField.class);
            if (pfield.requiresDefaultValidation)
                ptx.revalidate(referrers);
        }
    }
}
