
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.validation.Validation;
import javax.validation.ValidatorFactory;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SnapshotTransaction;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.TypeNotInSchemaVersionException;
import org.jsimpledb.core.UnknownFieldException;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.schema.SchemaObjectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSimpleDB Java persistence layer.
 *
 * <p>
 * JSimpleDB is a Java persistence solution built on three layers of abstraction:
 *  <ul>
 *  <li>At the bottom layer is a simple {@code byte[]} <b>key/value</b> database represented by the
 *      {@link org.jsimpledb.kv.KVDatabase} class. Transactions are supported at this layer and are accessed
 *      through the {@link org.jsimpledb.kv.KVTransaction} interface.
 *      There are several available {@link org.jsimpledb.kv.KVDatabase} implementations, including "wrappers"
 *      for several third party key/value stores.</li>
 *  <li>On top of that sits the <b>core API</b> layer, which provides a rigourous database abstraction on top of the
 *      key/value store. It supports simple fields of any atomic Java type, as well as list, set, and map complex fields,
 *      tightly controlled schema versioning, simple and composite indexes, and lifecycle and change notifications.
 *      It is not Java-specific or explicitly object-oriented. The core API is accessed through the {@link Database}
 *      and {@link org.jsimpledb.core.Transaction} classes.</li>
 *  <li>The top layer is a Java-centric, type safe, object-oriented persistence layer for Java applications.
 *      It sits on top of the core API layer and provides a fully type-safe Java view of a core API {@link Transaction},
 *      where all access is through user-supplied Java model classes. Database types and fields, and Java listener methods
 *      are all declared using {@linkplain org.jsimpledb.annotation Java annotations}. Incremental JSR 303 validation is supported.
 *      The {@link JSimpleDB} class represents an instance of this top layer database, and {@link JTransaction}
 *      represents the corresonding transactions.</li>
 *  </ul>
 *
 * <p>
 * User-provided Java model classes define database fields by declaring abstract Java bean property methods.
 * {@link JSimpleDB} generates concrete subclasses of the user-provided abstract model classes at runtime.
 * These runtime classes implement the abstract bean property methods, as well as the {@link JObject} interface.
 * Java model class instances are always associated with a specific {@link JTransaction}, and all of their database
 * state derives from that the underlying key/value {@link org.jsimpledb.kv.KVTransaction}.
 *
 * <p>
 * All Java model class instances have a unique {@link ObjId} which represents database identity. {@link JSimpleDB}
 * guarantees that at most one Java model class instance instance will exist for any given {@link JTransaction} and {@link ObjId}.
 * Instance creation, index queries, and certain other database-related tasks are initiated using a {@link JTransaction}.
 *
 * <p>
 * Normal database transactions are created via {@link #createTransaction createTransaction()}. "Snapshot" transactions are
 * purely in-memory transactions that are detached from the database and may persist indefinitely. Their purpose is to hold a
 * snapshot of some (user-defined) portion of the database content for use outside of a regular transaction. Otherwise,
 * they function like normal transactions, with support for index queries, listener callbacks, etc. See
 * {@link JTransaction#createSnapshotTransaction JTransaction.createSnapshotTransaction()},
 * {@link JTransaction#getSnapshotTransaction}, {@link JObject#copyOut JObject.copyOut()}, and
 * {@link JObject#copyIn JObject.copyIn()}.
 *
 * <p>
 * Instances of this class are usually created using a {@link JSimpleDBFactory}.
 *
 * @see JObject
 * @see JTransaction
 * @see JSimpleDBFactory
 * @see org.jsimpledb.annotation
 */
public class JSimpleDB {

    /**
     * The suffix that is appended to Java model class names to get the corresponding JSimpleDB generated class name.
     */
    public static final String GENERATED_CLASS_NAME_SUFFIX = "$$JSimpleDB";

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final TreeMap<Integer, JClass<?>> jclasses = new TreeMap<>();
    final HashMap<Class<?>, JClass<?>> jclassesByType = new HashMap<>();
    final TreeMap<Integer, JFieldInfo> jfieldInfos = new TreeMap<>();
    final TreeMap<Integer, JCompositeIndexInfo> jcompositeIndexInfos = new TreeMap<>();
    final ReferencePathCache referencePathCache = new ReferencePathCache(this);
    final ClassGenerator<UntypedJObject> untypedClassGenerator;
    final ArrayList<ClassGenerator<?>> classGenerators;
    final ClassLoader loader = new Loader();
    final Database db;
    final int configuredVersion;
    final StorageIdGenerator storageIdGenerator;

    final boolean hasOnCreateMethods;
    final boolean hasOnDeleteMethods;
    final boolean hasOnVersionChangeMethods;
    final boolean anyJClassRequiresDefaultValidation;
    final AnnotatedElement elementRequiringJSR303Validation;

    ValidatorFactory validatorFactory;

    volatile int actualVersion;

    private final LoadingCache<IndexInfoKey, IndexInfo> indexInfoCache = CacheBuilder.newBuilder()
      .maximumSize(1000).build(new CacheLoader<IndexInfoKey, IndexInfo>() {
        @Override
        public IndexInfo load(IndexInfoKey key) {
            return key.getIndexInfo(JSimpleDB.this);
        }
    });

    private SchemaModel schemaModel;
    private NameIndex nameIndex;

// Constructors

    /**
     * Create an instance using an initially empty, in-memory {@link SimpleKVDatabase}.
     * Generates a database schema by introspecting the {@code classes}; schema version number {@code 1} is assumed
     * and a {@link DefaultStorageIdGenerator} is used to auto-generate storage ID's where necessary.
     *
     * <p>
     * This constructor can also be used just to validate the annotations on the given classes.
     * </p>
     *
     * @param classes classes annotated with {@link JSimpleClass &#64;JSimpleClass} annotations
     * @throws IllegalArgumentException if {@code classes} is null
     * @throws IllegalArgumentException if {@code classes} contains a null class or a class with invalid annotation(s)
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    public JSimpleDB(Iterable<? extends Class<?>> classes) {
        this(new Database(new SimpleKVDatabase()), 1, new DefaultStorageIdGenerator(), classes);
    }

    /**
     * Create an instance using an initially empty, in-memory {@link SimpleKVDatabase}.
     *
     * <p>
     * Equivalent to {@link #JSimpleDB(Iterable) JSimpleDB}{@code (Arrays.asList(classes))}.
     * </p>
     *
     * @param classes classes annotated with {@link JSimpleClass &#64;JSimpleClass} annotations
     * @see #JSimpleDB(Iterable)
     */
    public JSimpleDB(Class<?>... classes) {
        this(Arrays.asList(classes));
    }

    /**
     * Primary constructor.
     *
     * @param database core database to use
     * @param version schema version number of the schema derived from {@code classes},
     *  or zero to use the highest version already recorded in the database
     * @param storageIdGenerator generator for auto-generated storage ID's, or null to disallow auto-generation of storage ID's
     * @param classes classes annotated with {@link JSimpleClass &#64;JSimpleClass} annotations; non-annotated classes are ignored
     * @throws IllegalArgumentException if {@code database} or {@code classes} is null
     * @throws IllegalArgumentException if {@code version} is not greater than zero
     * @throws IllegalArgumentException if {@code classes} contains a null class or a class with invalid annotation(s)
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    public JSimpleDB(Database database, int version, StorageIdGenerator storageIdGenerator, Iterable<? extends Class<?>> classes) {

        // Initialize
        Preconditions.checkArgument(database != null, "null database");
        Preconditions.checkArgument(version >= 0, "invalid negative schema version");
        Preconditions.checkArgument(classes != null, "null classes");
        this.db = database;
        this.configuredVersion = version;
        this.storageIdGenerator = storageIdGenerator;

        // Inventory classes; automatically add all @JSimpleClass-annotated superclasses of @JSimpleClass-annotated classes
        final HashSet<Class<?>> jsimpleClasses = new HashSet<>();
        for (Class<?> type : classes) {

            // Sanity check
            Preconditions.checkArgument(type != null, "null class found in classes");

            // Add type and all @JSimpleClass-annotated superclasses
            do {

                // Find annotation
                final JSimpleClass annotation = type.getAnnotation(JSimpleClass.class);
                if (annotation == null)
                    continue;

                // Sanity check type
                if (type.isPrimitive() || type.isInterface() || type.isArray()) {
                    throw new IllegalArgumentException("illegal type " + type + " for @"
                      + JSimpleClass.class.getSimpleName() + " annotation: not a normal class");
                }

                // Add class
                jsimpleClasses.add(type);
            } while ((type = type.getSuperclass()) != null);
        }

        // Add Java model classes
        for (Class<?> type : jsimpleClasses) {

            // Create JClass
            final JSimpleClass annotation = type.getAnnotation(JSimpleClass.class);
            final String name = annotation.name().length() != 0 ? annotation.name() : type.getSimpleName();
            if (this.log.isTraceEnabled()) {
                this.log.trace("found @" + JSimpleClass.class.getSimpleName() + " annotation on " + type
                  + " defining object type `" + name + "'");
            }

            // Get storage ID
            int storageId = annotation.storageId();
            if (storageId == 0)
                storageId = this.getStorageIdGenerator(annotation, type).generateClassStorageId(type, name);

            // Create JClass
            JClass<?> jclass;
            try {
                jclass = this.createJClass(name, storageId, type);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid @" + JSimpleClass.class.getSimpleName()
                  + " annotation on " + type + ": " + e, e);
            }

            // Add jclass
            this.addJClass(jclass);
            this.log.debug("added Java model class `" + jclass.name + "' with storage ID " + jclass.storageId);
        }

        // Inventory class generators
        this.classGenerators = new ArrayList<>(this.jclasses.size() + 1);
        for (JClass<?> jclass : this.jclasses.values())
            this.classGenerators.add(jclass.classGenerator);
        this.untypedClassGenerator = new ClassGenerator<UntypedJObject>(this, UntypedJObject.class);
        this.classGenerators.add(this.untypedClassGenerator);

        // Create fields
        for (JClass<?> jclass : this.jclasses.values())
            jclass.createFields(this);

        // Create canonical field info structures
        final HashMap<Integer, String> fieldDescriptionMap = new HashMap<>();
        for (JClass<?> jclass : this.jclasses.values()) {
            for (JField jfield : jclass.jfields.values()) {
                if (jfield instanceof JComplexField) {
                    final JComplexField complexField = (JComplexField)jfield;
                    final JComplexFieldInfo complexFieldInfo = (JComplexFieldInfo)jfield.toJFieldInfo();
                    for (JSimpleField subField : complexField.getSubFields()) {
                        this.addJFieldInfo(subField, subField.toJFieldInfo(jfield.storageId), fieldDescriptionMap);
                        final JSimpleFieldInfo subFieldInfo = (JSimpleFieldInfo)this.jfieldInfos.get(subField.storageId);
                        complexFieldInfo.getSubFieldInfos().add(subFieldInfo);
                    }
                    this.addJFieldInfo(complexField, complexFieldInfo, fieldDescriptionMap);
                } else
                    this.addJFieldInfo(jfield, jfield.toJFieldInfo(), fieldDescriptionMap);
            }
        }

        // Witness all simple fields to corresponding simple field info's
        for (JClass<?> jclass : this.jclasses.values()) {
            for (JField jfield : jclass.jfields.values()) {
                if (jfield instanceof JSimpleField) {
                    final JSimpleField jsimpleField = (JSimpleField)jfield;
                    final JSimpleFieldInfo jsimpleFieldInfo = (JSimpleFieldInfo)this.jfieldInfos.get(jfield.storageId);
                    jsimpleFieldInfo.witness(jsimpleField);
                }
                if (jfield instanceof JComplexField) {
                    final JComplexField complexField = (JComplexField)jfield;
                    final JComplexFieldInfo complexFieldInfo = (JComplexFieldInfo)this.jfieldInfos.get(jfield.storageId);
                    for (int i = 0; i < complexField.getSubFields().size(); i++) {
                        final JSimpleField subField = complexField.getSubFields().get(i);
                        final JSimpleFieldInfo subFieldInfo = (JSimpleFieldInfo)this.jfieldInfos.get(subField.storageId);
                        subFieldInfo.witness(subField);
                    }
                }
            }
        }

        // Add composite indexes to class; like fields, indexes are inherited (duplicated) from superclasses
        for (JClass<?> jclass : this.jclasses.values()) {
            for (Class<?> type = jclass.type; type != null; type = type.getSuperclass()) {
                final JSimpleClass annotation = type.getAnnotation(JSimpleClass.class);
                if (annotation != null) {
                    for (org.jsimpledb.annotation.JCompositeIndex indexAnnotation : annotation.compositeIndexes())
                        jclass.addCompositeIndex(this, indexAnnotation);
                }
            }
        }

        // Create canonical info instances for indexes
        final HashMap<Integer, String> indexDescriptionMap = new HashMap<>();
        for (JClass<?> jclass : this.jclasses.values()) {
            for (JCompositeIndex index : jclass.jcompositeIndexes.values()) {
                final JCompositeIndexInfo indexInfo = index.toJCompositeIndexInfo();
                for (JSimpleField jfield : index.jfields) {
                    final JSimpleFieldInfo jfieldInfo = (JSimpleFieldInfo)this.jfieldInfos.get(jfield.storageId);
                    indexInfo.getJFieldInfos().add(jfieldInfo);
                }
                this.addJCompositeIndexInfo(index, indexInfo, indexDescriptionMap);
            }
        }

        // Scan for other method-level annotations
        for (JClass<?> jclass : this.jclasses.values())
            jclass.scanAnnotations();

        // Determine which JClass's have validation requirement(s) on creation
        for (JClass<?> jclass : this.jclasses.values())
            jclass.calculateValidationRequirement();
        boolean anyDefaultValidation = false;
        AnnotatedElement someElementRequiringJSR303Validation = null;
        for (JClass<?> jclass : this.jclasses.values()) {
            anyDefaultValidation |= jclass.requiresDefaultValidation;
            if (someElementRequiringJSR303Validation == null)
                someElementRequiringJSR303Validation = jclass.elementRequiringJSR303Validation;
        }
        this.anyJClassRequiresDefaultValidation = anyDefaultValidation;
        this.elementRequiringJSR303Validation = someElementRequiringJSR303Validation;

        // Detect whether we have any @OnCreate, @OnDelete, and/or @OnVersionChange methods
        boolean anyOnCreateMethods = false;
        boolean anyOnDeleteMethods = false;
        boolean anyOnVersionChangeMethods = false;
        for (JClass<?> jclass : this.jclasses.values()) {
            anyOnCreateMethods |= !jclass.onCreateMethods.isEmpty();
            anyOnDeleteMethods |= !jclass.onDeleteMethods.isEmpty();
            anyOnVersionChangeMethods |= !jclass.onVersionChangeMethods.isEmpty();
        }
        this.hasOnCreateMethods = anyOnCreateMethods;
        this.hasOnDeleteMethods = anyOnDeleteMethods;
        this.hasOnVersionChangeMethods = anyOnVersionChangeMethods;

        // Validate schema
        this.db.validateSchema(this.getSchemaModel());

        // Eagerly load all generated Java classes so we "fail fast" if there are any loading errors
        this.untypedClassGenerator.generateClass();
        for (JClass<?> jclass : this.jclasses.values())
            jclass.getClassGenerator().generateClass();
    }

    // This method exists solely to bind the generic type parameters
    private <T> JClass<T> createJClass(String name, int storageId, Class<T> type) {
        return new JClass<T>(this, name, storageId, type);
    }

    StorageIdGenerator getStorageIdGenerator(Annotation annotation, AnnotatedElement target) {
        if (this.storageIdGenerator == null) {
            throw new IllegalArgumentException("invalid @" + annotation.annotationType().getSimpleName()
              + " annotation on " + target + ": no storage ID is given, but storage ID auto-generation is disabled"
              + " because no " + StorageIdGenerator.class.getSimpleName() + " is configured");
        }
        return this.storageIdGenerator;
    }

// Accessors

    /**
     * Get the core API {@link Database} underlying this instance.
     *
     * @return underlying {@link Database}
     */
    public Database getDatabase() {
        return this.db;
    }

    /**
     * Get the schema version that this instance was configured to use. This will either be a specific non-zero
     * schema version number, or else zero, indicating that the highest schema version found in the database should
     * be used.
     *
     * @return the schema version that this instance will use when opening transactions via
     *  {@link Database#createTransaction Database.createTransaction()}
     */
    public int getConfiguredVersion() {
        return this.configuredVersion;
    }

    /**
     * Get the schema version that this instance used for the most recently created transaction.
     *
     * <p>
     * If no transactions have been created yet, this returns zero. Otherwise, it returns the schema version
     * used by the most recently created transaction.
     * </p>
     *
     * <p>
     * If the {@code version} passed to the constructor was zero, this method can be used to read the highest schema
     * version seen in the database by the most recently created transaction.
     * </p>
     *
     * <p>
     * If the {@code version} passed to the constructor was non-zero, and at least one transaction has been created,
     * this method will return the same value.
     * </p>
     *
     * @return the schema version that this instance used in the most recently created transaction
     */
    public int getActualVersion() {
        return this.actualVersion;
    }

    /**
     * Get the Java model class of the given {@link JObject}.
     *
     * <p>
     * If {@code jobj} is an instance of a JSimpleDB-generated subclass of a user-supplied Java model class,
     * this returns the original Java model class. Otherwise, it returns {@code obj}'s type.
     *
     * @param jobj database instance
     * @return lowest ancestor class of {@code jobj}'s class that is not a JSimpleDB-generated subclass
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public static Class<?> getModelClass(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        for (Class<?> type = jobj.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().indexOf(GENERATED_CLASS_NAME_SUFFIX) == -1)
                return type;
        }
        return null;
    }

// Transactions

    /**
     * Create a new transaction.
     *
     * <p>
     * Convenience method; equivalent to:
     *  <blockquote><pre>
     *  createTransaction(allowNewSchema, validationMode, null)
     *  </pre></blockquote>
     *
     * @param allowNewSchema whether creating a new schema version is allowed
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return the newly created transaction
     * @throws org.jsimpledb.core.InvalidSchemaException if {@code schemaModel} does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is false
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is true, but {@code schemaModel} is incompatible
     *  with one or more previous schemas alread recorded in the database (i.e., the same storage ID is used
     *  incompatibly between schema versions)
     * @throws org.jsimpledb.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code validationMode} is null
     */
    public JTransaction createTransaction(boolean allowNewSchema, ValidationMode validationMode) {
        return this.createTransaction(allowNewSchema, validationMode, null);
    }

    /**
     * Create a new transaction with key/value transaction options.
     *
     * <p>
     * This does not invoke {@link JTransaction#setCurrent JTransaction.setCurrent()}: the caller is responsible
     * for doing that if necessary. However, this method does arrange for
     * {@link JTransaction#setCurrent JTransaction.setCurrent}{@code (null)} to be invoked as soon as the
     * returned transaction is committed (or rolled back), assuming {@link JTransaction#getCurrent} returns the
     * {@link JTransaction} returned here at that time.
     * </p>
     *
     * @param allowNewSchema whether creating a new schema version is allowed
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @param kvoptions optional {@link org.jsimpledb.kv.KVDatabase}-specific transaction options; may be null
     * @return the newly created transaction
     * @throws org.jsimpledb.core.InvalidSchemaException if {@code schemaModel} does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is false
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is true, but {@code schemaModel} is incompatible
     *  with one or more previous schemas alread recorded in the database (i.e., the same storage ID is used
     *  incompatibly between schema versions)
     * @throws org.jsimpledb.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code validationMode} is null
     */
    public JTransaction createTransaction(boolean allowNewSchema, ValidationMode validationMode, Map<String, ?> kvoptions) {
        return this.createTransaction(
          this.db.createTransaction(this.getSchemaModel(), this.configuredVersion, allowNewSchema, kvoptions), validationMode);
    }

    /**
     * Create a new transaction using an already-opened {@link KVTransaction}.
     *
     * <p>
     * This does not invoke {@link JTransaction#setCurrent JTransaction.setCurrent()}: the caller is responsible
     * for doing that if necessary. However, this method does arrange for
     * {@link JTransaction#setCurrent JTransaction.setCurrent}{@code (null)} to be invoked as soon as the
     * returned transaction is committed (or rolled back), assuming {@link JTransaction#getCurrent} returns the
     * {@link JTransaction} returned here at that time.
     * </p>
     *
     * @param kvt already opened key/value store transaction
     * @param allowNewSchema whether creating a new schema version is allowed
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return the newly created transaction
     * @throws org.jsimpledb.core.InvalidSchemaException if {@code schemaModel} does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is false
     * @throws org.jsimpledb.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is true, but {@code schemaModel} is incompatible
     *  with one or more previous schemas alread recorded in the database (i.e., the same storage ID is used
     *  incompatibly between schema versions)
     * @throws org.jsimpledb.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code kvt} or {@code validationMode} is null
     */
    public JTransaction createTransaction(KVTransaction kvt, boolean allowNewSchema, ValidationMode validationMode) {
        return this.createTransaction(
          this.db.createTransaction(kvt, this.getSchemaModel(), this.configuredVersion, allowNewSchema), validationMode);
    }

    private JTransaction createTransaction(Transaction tx, ValidationMode validationMode) {
        assert tx != null;
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        this.actualVersion = tx.getSchema().getVersionNumber();
        final JTransaction jtx = new JTransaction(this, tx, validationMode);
        tx.addCallback(new CleanupCurrentCallback(jtx));
        return jtx;
    }

    /**
     * Create a new, empty {@link SnapshotJTransaction} backed by a {@link NavigableMapKVStore}.
     *
     * <p>
     * The returned {@link SnapshotJTransaction} does not support {@link SnapshotJTransaction#commit commit()} or
     * {@link SnapshotJTransaction#rollback rollback()}, and can be used indefinitely.
     *
     * @param validationMode the {@link ValidationMode} to use for the snapshot transaction
     * @return initially empty snapshot transaction
     */
    public SnapshotJTransaction createSnapshotTransaction(ValidationMode validationMode) {
        return this.createSnapshotTransaction(new NavigableMapKVStore(), true, validationMode);
    }

    /**
     * Create a new {@link SnapshotJTransaction} based on the provided key/value store.
     *
     * <p>
     * The key/value store will be initialized if necessary (i.e., {@code kvstore} may be empty), otherwise it will be
     * validated against the schema information associated with this instance.
     *
     * <p>
     * The returned {@link SnapshotJTransaction} does not support {@link SnapshotJTransaction#commit commit()} or
     * {@link SnapshotJTransaction#rollback rollback()}, and can be used indefinitely.
     *
     * @param kvstore key/value store, empty or having content compatible with this transaction's {@link JSimpleDB}
     * @param allowNewSchema whether creating a new schema version in {@code kvstore} is allowed
     * @param validationMode the {@link ValidationMode} to use for the snapshot transaction
     * @return snapshot transaction based on {@code kvstore}
     * @throws org.jsimpledb.core.SchemaMismatchException if {@code kvstore} contains incompatible or missing schema information
     * @throws org.jsimpledb.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public SnapshotJTransaction createSnapshotTransaction(KVStore kvstore, boolean allowNewSchema, ValidationMode validationMode) {
        final SnapshotTransaction stx = this.db.createSnapshotTransaction(kvstore,
          this.getSchemaModel(), this.configuredVersion, allowNewSchema);
        return new SnapshotJTransaction(this, stx, validationMode);
    }

// Schema

    /**
     * Get the {@link SchemaModel} associated with this instance, derived from the annotations on the scanned classes.
     *
     * @return the associated schema model
     */
    public SchemaModel getSchemaModel() {
        if (this.schemaModel == null) {
            final SchemaModel model = new SchemaModel();
            for (JClass<?> jclass : this.jclasses.values()) {
                final SchemaObjectType schemaObjectType = jclass.toSchemaItem(this);
                model.getSchemaObjectTypes().put(schemaObjectType.getStorageId(), schemaObjectType);
            }
            this.schemaModel = model;
            this.log.debug("JSimpleDB schema generated from annotated classes:\n{}", this.schemaModel);
        }
        return this.schemaModel.clone();
    }

    /**
     * Get a {@link NameIndex} based on {@linkplain #getSchemaModel this instance's schema model}.
     *
     * @return a name index on this instance's schema model
     */
    public NameIndex getNameIndex() {
        if (this.nameIndex == null)
            this.nameIndex = new NameIndex(this.getSchemaModel());
        return this.nameIndex;
    }

// JClass access

    /**
     * Get all {@link JClass}'s associated with this instance, indexed by storage ID.
     *
     * @return read-only mapping from storage ID to {@link JClass}
     */
    public SortedMap<Integer, JClass<?>> getJClasses() {
        return Collections.unmodifiableSortedMap(this.jclasses);
    }

    /**
     * Get all {@link JClass}'s associated with this instance, indexed by Java model type.
     *
     * @return read-only mapping from Java model type to {@link JClass}
     */
    public Map<Class<?>, JClass<?>> getJClassesByType() {
        return Collections.unmodifiableMap(this.jclassesByType);
    }

    /**
     * Get the {@link JClass} modeled by the given type.
     *
     * @param type an annotated Java object model type
     * @param <T> Java model type
     * @return associated {@link JClass}
     * @throws IllegalArgumentException if {@code type} is not equal to a known Java object model type
     */
    @SuppressWarnings("unchecked")
    public <T> JClass<T> getJClass(Class<T> type) {
        final JClass<?> jclass = this.jclassesByType.get(type);
        if (jclass == null)
            throw new IllegalArgumentException("java model type is not recognized: " + type);
        return (JClass<T>)jclass;
    }

    /**
     * Find the most specific {@link JClass} for which the give type is a sub-type of the corresponding Java model type.
     *
     * @param type (sub)type of some Java object model type
     * @param <T> Java model type or subtype thereof
     * @return narrowest {@link JClass} whose Java object model type is a supertype of {@code type}, or null if none found
     */
    @SuppressWarnings("unchecked")
    public <T> JClass<? super T> findJClass(Class<T> type) {
        for (Class<? super T> superType = type; superType != null; superType = superType.getSuperclass()) {
            final JClass<?> jclass = this.jclassesByType.get(superType);
            if (jclass != null)
                return (JClass<? super T>)jclass;
        }
        return null;
    }

    /**
     * Get the {@link JClass} associated with the object ID.
     *
     * @param id object ID
     * @return {@link JClass} instance
     * @throws TypeNotInSchemaVersionException if {@code id} has a type that does not exist in this instance's schema version
     * @throws IllegalArgumentException if {@code id} is null
     */
    public JClass<?> getJClass(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        final JClass<?> jclass = this.jclasses.get(id.getStorageId());
        if (jclass == null)
            throw new TypeNotInSchemaVersionException(id, this.actualVersion);
        return jclass;
    }

    /**
     * Get the {@link JClass} associated with the given storage ID.
     *
     * @param storageId object type storage ID
     * @return {@link JClass} instance
     * @throws UnknownTypeException if {@code storageId} does not represent an object type
     */
    public JClass<?> getJClass(int storageId) {
        final JClass<?> jclass = this.jclasses.get(storageId);
        if (jclass == null)
            throw new UnknownTypeException(storageId, this.actualVersion);
        return jclass;
    }

    /**
     * Get all {@link JClass}es which sub-type the given type.
     *
     * @param type type restriction, or null for no restrction
     * @param <T> Java model type
     * @return list of {@link JClass}es whose type is {@code type} or a sub-type, ordered by storage ID
     */
    @SuppressWarnings("unchecked")
    public <T> List<JClass<? extends T>> getJClasses(Class<T> type) {
        final ArrayList<JClass<? extends T>> list = new ArrayList<>();
        for (JClass<?> jclass : this.jclasses.values()) {
            if (type == null || type.isAssignableFrom(jclass.type))
                list.add((JClass<? extends T>)jclass);
        }
        return list;
    }

    /**
     * Generate the {@link KeyRanges} restricting objects to the specified type.
     *
     * @param type any Java type, or null for no restriction
     * @return key restriction for {@code type}
     */
    KeyRanges keyRangesFor(Class<?> type) {
        if (type == null)
            return KeyRanges.FULL;
        final ArrayList<KeyRange> list = new ArrayList<>(this.jclasses.size());
        for (JClass<?> jclass : this.getJClasses(type))
            list.add(ObjId.getKeyRange(jclass.storageId));
        return new KeyRanges(list);
    }

// Reference Paths

    /**
     * Parse a {@link ReferencePath} in {@link String} form.
     *
     * @param startType starting Java type for the path
     * @param path dot-separated path of zero or more reference fields, followed by a target field
     * @return parsed reference path
     * @throws IllegalArgumentException if {@code path} is invalid
     * @throws IllegalArgumentException if {@code startType} or {@code path} is null
     * @see ReferencePath
     */
    public ReferencePath parseReferencePath(Class<?> startType, String path) {
        return this.parseReferencePath(startType, path, null);
    }

    ReferencePath parseReferencePath(Class<?> startType, String path, Boolean lastIsSubField) {
        return this.referencePathCache.get(startType, path, lastIsSubField);
    }

// Validation

    /**
     * Configure a custom {@link ValidatorFactory} used to create {@link javax.validation.Validator}s
     * for validation within transactions.
     *
     * @param validatorFactory factory for validators
     * @throws IllegalArgumentException if {@code validatorFactory} is null
     */
    public void setValidatorFactory(ValidatorFactory validatorFactory) {
        Preconditions.checkArgument(validatorFactory != null, "null validatorFactory");
        this.validatorFactory = validatorFactory;
    }

    /**
     * Get the {@link ValidatorFactory}, if needed.
     *
     * @return {@link ValidatorFactory} for JSR 303 validation, or null if JSR 303 validation is not being used
     */
    ValidatorFactory getValidatorFactory() {

        // Already created or configured?
        if (this.validatorFactory != null)
            return this.validatorFactory;

        // Are we doing any JSR 303 validation?
        if (this.elementRequiringJSR303Validation == null)
            return null;

        // Create it
        try {
            this.validatorFactory = Validation.buildDefaultValidatorFactory();
        } catch (Exception e) {
            throw new JSimpleDBException("JSR 303 validation constraint found on " + this.elementRequiringJSR303Validation
              + " but creation of default ValidatorFactory failed; is there a JSR 303 validation implementation on the classpath?",
              e);
        }

        // Done
        return this.validatorFactory;
    }

// Misc utility

    /**
     * Utility method to get all of the objects directly referenced by a given object via any field.
     *
     * <p>
     * Note: the returned {@link Iterable} may contain duplicates; these can be eliminated using an
     * {@link org.jsimpledb.core.util.ObjIdSet} if necessary.
     * </p>
     *
     * @param jobj starting object
     * @return all objects directly referenced by {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public Iterable<JObject> getReferencedObjects(final JObject jobj) {

        // Sanity check
        Preconditions.checkArgument(jobj != null, "null jobj");
        final ObjId id = jobj.getObjId();

        // Visit fields
        final ArrayList<Iterable<JObject>> iterables = new ArrayList<>();
        for (JField jfield : this.getJClass(id).getJFieldsByStorageId().values()) {
            jfield.visit(new JFieldSwitchAdapter<Void>() {

                @Override
                public Void caseJReferenceField(JReferenceField field) {
                    final JObject ref = field.getValue(jobj);
                    if (ref != null)
                        iterables.add(Collections.singleton(ref));
                    return null;
                }

                @Override
                public Void caseJMapField(JMapField field) {
                    if (field.getKeyField() instanceof JReferenceField)
                        iterables.add(Iterables.filter(field.getValue(jobj).keySet(), JObject.class));
                    if (field.getValueField() instanceof JReferenceField)
                        iterables.add(Iterables.filter(field.getValue(jobj).values(), JObject.class));
                    return null;
                }

                @Override
                protected Void caseJCollectionField(JCollectionField field) {
                    if (field.getElementField() instanceof JReferenceField)
                        iterables.add(Iterables.filter(field.getValue(jobj), JObject.class));
                    return null;
                }

                @Override
                protected Void caseJField(JField field) {
                    return null;
                }
            });
        }

        // Done
        return Iterables.concat(iterables);
    }

// IndexInfo Cache

    IndexInfo getIndexInfo(IndexInfoKey key) {
        try {
            return this.indexInfoCache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            final Throwable t = e.getCause();
            if (t instanceof RuntimeException)
                throw (RuntimeException)t;
            if (t instanceof Error)
                throw (Error)t;
            throw e;
        }
    }

// Internal Stuff

    // Get class generator for "untyped" JObject's
    ClassGenerator<UntypedJObject> getUntypedClassGenerator() {
        return this.untypedClassGenerator;
    }

    /**
     * Get the {@link JFieldInfo} associated with the given storage ID.
     *
     * @param storageId field storage ID
     * @param type required type
     * @return {@link JField} instance
     * @throws UnknownFieldException if {@code storageId} does not represent a field
     */
    <T extends JFieldInfo> T getJFieldInfo(int storageId, Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final JFieldInfo jfieldInfo = this.jfieldInfos.get(storageId);
        if (jfieldInfo == null) {
            throw new UnknownFieldException(storageId, "no JSimpleDB field exists with storage ID "
              + storageId + " in schema version " + this.actualVersion);
        }
        try {
            return type.cast(jfieldInfo);
        } catch (ClassCastException e) {
            throw new UnknownFieldException(storageId, "no JSimpleDB fields exist with storage ID "
              + storageId + " in schema version " + this.actualVersion + " (found field " + jfieldInfo + " instead)");
        }
    }

    // Add new JClass, checking for storage ID conflicts
    private void addJClass(JClass<?> jclass) {

        // Check for storage ID conflict
        final JClass<?> other = this.jclasses.get(jclass.storageId);
        if (other != null) {
            throw new IllegalArgumentException("illegal duplicate use of storage ID "
              + jclass.storageId + " for both " + other + " and " + jclass);
        }
        this.jclasses.put(jclass.storageId, jclass);
        assert !this.jclassesByType.containsKey(jclass.type);                   // this should never conflict, no need to check
        this.jclassesByType.put(jclass.type, jclass);
    }

    // Add new JFieldInfo, checking for conflicts
    private <T extends JFieldInfo> void addJFieldInfo(JField jfield, T fieldInfo, Map<Integer, String> descriptionMap) {
        this.addInfo(jfield, fieldInfo, this.jfieldInfos, descriptionMap);
    }

    // Add new JCompositeIndexInfo, checking for conflicts
    private void addJCompositeIndexInfo(JCompositeIndex index, JCompositeIndexInfo indexInfo, Map<Integer, String> descriptionMap) {
        this.addInfo(index, indexInfo, this.jcompositeIndexInfos, descriptionMap);
    }

    // Add new info, checking for storage ID conflicts
    private <T> void addInfo(JSchemaObject item, T info, Map<Integer, T> infoMap, Map<Integer, String> descriptionMap) {
        final T existing = infoMap.get(item.storageId);
        if (existing == null) {
            infoMap.put(item.storageId, info);
            descriptionMap.put(item.storageId, item.description);
        } else if (!info.equals(existing)) {
            throw new IllegalArgumentException("incompatible duplicate use of storage ID " + item.storageId
              + " for " + descriptionMap.get(item.storageId) + " and " + item.description);
        }
    }

// Loader

    private class Loader extends ClassLoader {

        // Set up class loader
        Loader() {
            super(Thread.currentThread().getContextClassLoader());
        }

        // Find matching ClassGenerator, if any, otherwise defer to parent
        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            byte[] bytes = null;
            for (ClassGenerator<?> generator : JSimpleDB.this.classGenerators) {
                if (name.equals(generator.getClassName().replace('/', '.'))) {
                    bytes = generator.generateBytecode();
                    break;
                }
            }
            return bytes != null ? this.defineClass(name, bytes, 0, bytes.length) : super.findClass(name);
        }
    }

// CleanupCurrentCallback

    private static final class CleanupCurrentCallback extends Transaction.CallbackAdapter {

        private final JTransaction jtx;

        CleanupCurrentCallback(JTransaction jtx) {
            assert jtx != null;
            this.jtx = jtx;
        }

        @Override
        public void afterCompletion(boolean committed) {
            final JTransaction current;
            try {
                current = JTransaction.getCurrent();
            } catch (IllegalStateException e) {
                return;
            }
            if (current == this.jtx)
                JTransaction.setCurrent(null);
        }

        @Override
        public int hashCode() {
            return this.jtx.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final CleanupCurrentCallback that = (CleanupCurrentCallback)obj;
            return this.jtx.equals(that.jtx);
        }
    }
}

