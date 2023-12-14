
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.permazen.annotation.JCompositeIndexes;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.DetachedTransaction;
import io.permazen.core.ObjId;
import io.permazen.core.ReferenceEncoding;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.core.TypeNotInSchemaVersionException;
import io.permazen.core.UnknownFieldException;
import io.permazen.core.UnknownTypeException;
import io.permazen.core.util.ObjIdSet;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.kv.util.NavigableMapKVStore;
import io.permazen.schema.NameIndex;
import io.permazen.schema.SchemaModel;
import io.permazen.schema.SchemaObjectType;
import io.permazen.util.ApplicationClassLoader;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dellroad.stuff.util.LongMap;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Permazen Java persistence layer.
 *
 * <p>
 * Permazen is a Java persistence solution built on three layers of abstraction:
 *  <ul>
 *  <li>At the bottom is a simple <b>Key/Value Layer</b> represented by {@link KVDatabase}.
 *      Transactions are supported at this layer and are accessed via {@link KVTransaction}.
 *      There are several available {@link KVDatabase} implementations, including "wrappers"
 *      for several third party key/value stores.</li>
 *  <li>On top of that sits the <b>Core API Layer</b>, which provides a rigourous "object database" abstraction on top of
 *      a {@link KVDatabase}. It supports simple fields of any Java type that can be (de)serialized to/from
 *      a {@code byte[]} array, as well as list, set, and map complex fields, tightly controlled schema versioning,
 *      simple and composite indexes, and lifecycle and change notifications.
 *      It is not Java-specific or explicitly object-oriented: an "object" at this layer is just a structure with defined fields.
 *      The core API layer may be accessed through the {@link Database} and {@link Transaction} classes.</li>
 *  <li>The <b>Java Layer</b> is a Java-centric, object-oriented persistence layer for Java applications.
 *      It sits on top of the core API layer and provides a fully "Java" view of the underlying data
 *      where all data access is through user-supplied Java model classes. All schema definition and listeners are specified
 *      through {@linkplain io.permazen.annotation Java annotations}. Incremental JSR 303 validation is supported.
 *      The {@link Permazen} class represents an instance of this top layer database, and {@link JTransaction}
 *      represents the corresonding transactions.</li>
 *  </ul>
 *
 * <p>
 * User-provided Java model classes define database fields by declaring abstract Java bean methods.
 * {@link Permazen} generates concrete subclasses of the user-provided abstract model classes at runtime.
 * These runtime classes implement the Java bean methods as well as the {@link JObject} interface.
 * Instances of these classes are always associated with a specific {@link JTransaction}, and all of their database
 * state derives from that the underlying key/value {@link KVTransaction}.
 *
 * <p>
 * All Java model class instances have a unique {@link ObjId} which represents database identity.
 * {@link Permazen} guarantees that at most one Java instance will exist for any given {@link JTransaction} and {@link ObjId}.
 * Instance creation, index queries, and certain other database-related tasks are initiated using a {@link JTransaction}.
 *
 * <p>
 * Normal database transactions are created via {@link #createTransaction createTransaction()}. Detached transactions are
 * purely in-memory transactions that are detached from the database and may persist indefinitely; their purpose is to hold a
 * snapshot of some (user-defined) portion of the database content for use outside of a regular transaction. Otherwise,
 * they function like normal transactions, with support for index queries, listener callbacks, etc. See
 * {@link JTransaction#createDetachedTransaction JTransaction.createDetachedTransaction()},
 * {@link JTransaction#getDetachedTransaction}, {@link JObject#copyOut JObject.copyOut()}, and
 * {@link JObject#copyIn JObject.copyIn()}.
 *
 * <p>
 * Instances of this class are usually created using a {@link PermazenFactory}.
 *
 * @see JObject
 * @see JTransaction
 * @see PermazenFactory
 * @see io.permazen.annotation
 * @see <a href="https://github.com/permazen/permazen/">Permazen GitHub Page</a>
 */
public class Permazen {

    /**
     * The suffix that is appended to Java model class names to get the corresponding Permazen generated class name.
     */
    public static final String GENERATED_CLASS_NAME_SUFFIX = "$$Permazen";

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final TreeMap<Integer, JClass<?>> jclasses = new TreeMap<>();
    final HashMap<Class<?>, JClass<?>> jclassesByType = new HashMap<>();
    final HashMap<Integer, IndexInfo> indexInfoMap = new HashMap<>();
    final LongMap<JField> typeFieldMap = new LongMap<>();
    final HashSet<Integer> fieldsRequiringDefaultValidation = new HashSet<>();
    final ReferencePathCache referencePathCache = new ReferencePathCache(this);
    final ClassGenerator<UntypedJObject> untypedClassGenerator;
    final ArrayList<ClassGenerator<?>> classGenerators;
    final ClassLoader loader;
    final Database db;
    final StorageIdGenerator storageIdGenerator;

    final boolean hasOnCreateMethods;
    final boolean hasOnDeleteMethods;
    final boolean hasOnVersionChangeMethods;
    final boolean hasUpgradeConversions;
    final boolean anyJClassRequiresDefaultValidation;
    final AnnotatedElement elementRequiringJSR303Validation;

    // Cached listener sets used by JTransaction.<init>()
    final Transaction.ListenerSet[] listenerSets = new Transaction.ListenerSet[4];

    ValidatorFactory validatorFactory;

    volatile int configuredVersion;
    volatile int actualVersion;

    private final LoadingCache<IndexQueryInfoKey, IndexQueryInfo> indexQueryInfoCache = CacheBuilder.newBuilder()
      .maximumSize(1000).build(new CacheLoader<IndexQueryInfoKey, IndexQueryInfo>() {
        @Override
        public IndexQueryInfo load(IndexQueryInfoKey key) {
            return key.getIndexQueryInfo(Permazen.this);
        }
    });

    private TransactionConfig txConfig;
    private SchemaModel schemaModel;
    private NameIndex nameIndex;

// Constructors

    /**
     * Create an instance using an initially empty, in-memory {@link SimpleKVDatabase}.
     * Generates a database schema by introspecting the {@code classes}; auto-generates a schema version number
     * and uses a {@link DefaultStorageIdGenerator} to auto-generate storage ID's.
     *
     * <p>
     * This constructor can also be used just to validate the annotations on the given classes.
     *
     * @param classes classes annotated with {@link PermazenType &#64;PermazenType} annotations
     * @throws IllegalArgumentException if {@code classes} is null
     * @throws IllegalArgumentException if {@code classes} contains a null class or a class with invalid annotation(s)
     * @throws IllegalArgumentException if {@code classes} contains a class with no suitable subclass constructor
     * @throws io.permazen.core.InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    public Permazen(Collection<Class<?>> classes) {
        this(new Database(new SimpleKVDatabase()), -1, new DefaultStorageIdGenerator(), classes);
    }

    /**
     * Create an instance using an initially empty, in-memory {@link SimpleKVDatabase}.
     *
     * <p>
     * Equivalent to {@link #Permazen(Collection) Permazen}{@code (Arrays.asList(classes).stream())}.
     *
     * @param classes classes annotated with {@link PermazenType &#64;PermazenType} annotations
     * @see #Permazen(Collection)
     */
    public Permazen(Class<?>... classes) {
        this(Arrays.asList(classes));
    }

    /**
     * Primary constructor.
     *
     * @param database core database to use
     * @param version schema version number of the schema derived from {@code classes},
     *  zero to use the highest version already recorded in the database,
     *  or -1 to use an {@linkplain SchemaModel#autogenerateVersion auto-generated} schema version
     * @param storageIdGenerator generator for auto-generated storage ID's, or null to disallow auto-generation of storage ID's
     * @param classes classes annotated with {@link PermazenType &#64;PermazenType} annotations; non-annotated classes are ignored
     * @throws IllegalArgumentException if {@code database} or {@code classes} is null
     * @throws IllegalArgumentException if {@code version} is less than -1
     * @throws IllegalArgumentException if {@code classes} contains a null class or a class with invalid annotation(s)
     * @throws io.permazen.core.InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    public Permazen(Database database, int version, StorageIdGenerator storageIdGenerator, Collection<Class<?>> classes) {

        // Initialize
        Preconditions.checkArgument(database != null, "null database");
        Preconditions.checkArgument(version >= -1, "invalid schema version");
        Preconditions.checkArgument(classes != null, "null classes");
        this.db = database;
        this.storageIdGenerator = storageIdGenerator;
        this.loader = new Loader();

        // Inventory classes; automatically add all @PermazenType-annotated superclasses of @PermazenType-annotated classes
        final HashSet<Class<?>> permazenTypes = new HashSet<>();
        for (Class<?> type : classes) {

            // Sanity check
            Preconditions.checkArgument(type != null, "null class found in classes");

            // Add type and all @PermazenType-annotated superclasses
            do {

                // Find annotation
                final PermazenType annotation = Util.getAnnotation(type, PermazenType.class);
                if (annotation == null)
                    continue;

                // Sanity check type
                if (type.isPrimitive() || type.isArray()) {
                    throw new IllegalArgumentException(String.format(
                      "illegal type %s for @%s annotation: not a normal class or interface",
                      type, PermazenType.class.getSimpleName()));
                }

                // Add class
                permazenTypes.add(type);
            } while ((type = type.getSuperclass()) != null);
        }

        // Add Java model classes
        for (Class<?> type : permazenTypes) {

            // Get annotation
            final PermazenType annotation = Util.getAnnotation(type, PermazenType.class);
            final String name = annotation.name().length() != 0 ? annotation.name() : type.getSimpleName();
            if (this.log.isTraceEnabled()) {
                this.log.trace("found @{} annotation on {} defining object type \"{}\"",
                  PermazenType.class.getSimpleName(), type, name);
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
                throw new IllegalArgumentException(String.format(
                  "invalid @%s annotation on %s: %s", PermazenType.class.getSimpleName(), type, e), e);
            }

            // Add jclass
            this.addJClass(jclass);
            this.log.debug("added Java model class \"{}\" with storage ID {}", jclass.name, jclass.storageId);
        }

        // Inventory class generators
        this.classGenerators = this.jclasses.values().stream()
          .map(jclass -> jclass.classGenerator)
          .collect(Collectors.toCollection(ArrayList::new));
        this.untypedClassGenerator = new ClassGenerator<>(this, UntypedJObject.class);
        this.classGenerators.add(this.untypedClassGenerator);

        // Create fields
        for (JClass<?> jclass : this.jclasses.values())
            jclass.createFields(this);

        // Add composite indexes to class; like fields, indexes are inherited (duplicated) from supertypes
        for (JClass<?> jclass : this.jclasses.values()) {
            for (Class<?> supertype : TypeToken.of(jclass.type).getTypes().rawTypes()) {
                final io.permazen.annotation.JCompositeIndex[] annotations;
                final JCompositeIndexes container = Util.getAnnotation(supertype, JCompositeIndexes.class);
                if (container != null)
                    annotations = container.value();
                else {
                    io.permazen.annotation.JCompositeIndex annotation = Util.getAnnotation(supertype,
                      io.permazen.annotation.JCompositeIndex.class);
                    if (annotation == null)
                        continue;
                    annotations = new io.permazen.annotation.JCompositeIndex[] { annotation };
                }
                for (io.permazen.annotation.JCompositeIndex annotation : annotations) {
                    if (annotation.uniqueExclude().length > 0 && !annotation.unique()) {
                        throw new IllegalArgumentException(String.format(
                          "invalid @%s annotation on %s: use of uniqueExclude() requires unique = true",
                          io.permazen.annotation.JCompositeIndex.class.getSimpleName(), supertype));
                    }
                    jclass.addCompositeIndex(this, supertype, annotation);
                }
            }
        }

        // Build schema
        this.getSchemaModel();

        // Find all fields that require default validation
        for (JClass<?> jclass : this.jclasses.values()) {
            for (JField jfield : jclass.jfields.values()) {
                if (jfield.requiresDefaultValidation)
                    this.fieldsRequiringDefaultValidation.add(jfield.storageId);
            }
        }

        // Populate this.indexInfoMap
        final Map<Integer, String> descriptionMap = new HashMap<>();
        for (JClass<?> jclass : this.jclasses.values()) {

            // Find simple field indexes
            for (JField jfield : jclass.jfields.values()) {
                if (jfield instanceof JSimpleField) {
                    final JSimpleField simpleField = (JSimpleField)jfield;
                    if (simpleField.indexed)
                        this.addIndexInfo(simpleField, descriptionMap);
                } else if (jfield instanceof JComplexField) {
                    final JComplexField parentField = (JComplexField)jfield;
                    for (JSimpleField subField : parentField.getSubFields()) {
                        if (subField.indexed)
                            this.addIndexInfo(subField, descriptionMap);
                    }
                }
            }

            // Find composite indexes
            for (JCompositeIndex index : jclass.jcompositeIndexes.values())
                this.addIndexInfo(index, descriptionMap);
        }

        // Populate this.typeFieldMap
        for (JClass<?> jclass : this.jclasses.values()) {
            for (JField jfield : jclass.jfields.values()) {
                this.typeFieldMap.put(this.getTypeFieldKey(jclass.storageId, jfield.storageId), jfield);
                if (jfield instanceof JComplexField) {
                    final JComplexField parentField = (JComplexField)jfield;
                    for (JSimpleField subField : parentField.getSubFields())
                        this.typeFieldMap.put(this.getTypeFieldKey(jclass.storageId, subField.storageId), subField);
                }
            }
        }

        // Populate jclass forwardCascadeMap and inverseCascadeMap
        for (JClass<?> jclass0 : this.jclasses.values()) {
            final JClass<?> jclass = jclass0;
            for (JField jfield : jclass.jfields.values()) {
                jfield.visit(new JFieldSwitch<Void>() {

                    @Override
                    public Void caseJReferenceField(JReferenceField field) {

                        // Do forward cascades
                        for (String cascadeName : field.forwardCascades) {
                            if (cascadeName == null)
                                continue;
                            jclass.forwardCascadeMap.computeIfAbsent(cascadeName, s -> new ArrayList<>()).add(field);
                        }

                        // Do inverse cascades
                        for (String cascadeName : field.inverseCascades) {
                            if (cascadeName == null)
                                continue;
                            for (JClass<?> targetClass : Permazen.this.getJClasses(field.typeToken.getRawType())) {
                                targetClass.inverseCascadeMap
                                  .computeIfAbsent(cascadeName, s -> new HashMap<>())
                                  .computeIfAbsent(field.storageId, i -> new KeyRanges())
                                  .add(ObjId.getKeyRange(jclass.storageId));
                            }
                        }
                        return null;
                    }

                    @Override
                    public Void caseJMapField(JMapField field) {
                        if (field.getKeyField() instanceof JReferenceField)
                            this.caseJReferenceField((JReferenceField)field.getKeyField());
                        if (field.getValueField() instanceof JReferenceField)
                            this.caseJReferenceField((JReferenceField)field.getValueField());
                        return null;
                    }

                    @Override
                    public Void caseJCollectionField(JCollectionField field) {
                        if (field.getElementField() instanceof JReferenceField)
                            this.caseJReferenceField((JReferenceField)field.getElementField());
                        return null;
                    }

                    @Override
                    public Void caseJField(JField field) {
                        return null;
                    }
                });
            }
        }

        // Scan for other method-level annotations
        this.jclasses.values()
          .forEach(JClass::scanAnnotations);

        // Determine which JClass's have validation requirement(s) on creation
        this.jclasses.values()
          .forEach(JClass::calculateValidationRequirement);
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
        boolean anyUpgradeConversions = false;
        for (JClass<?> jclass : this.jclasses.values()) {
            anyOnCreateMethods |= !jclass.onCreateMethods.isEmpty();
            anyOnDeleteMethods |= !jclass.onDeleteMethods.isEmpty();
            anyOnVersionChangeMethods |= !jclass.onVersionChangeMethods.isEmpty();
            anyUpgradeConversions |= !jclass.upgradeConversionFields.isEmpty();
        }
        this.hasOnCreateMethods = anyOnCreateMethods;
        this.hasOnDeleteMethods = anyOnDeleteMethods;
        this.hasOnVersionChangeMethods = anyOnVersionChangeMethods;
        this.hasUpgradeConversions = anyUpgradeConversions;

        // Validate schema
        this.db.validateSchema(this.getSchemaModel());

        // Auto-generate schema version if requested
        this.configuredVersion = version == -1 ? this.schemaModel.autogenerateVersion() : version;

        // Eagerly load all generated Java classes so we "fail fast" if there are any loading errors
        this.untypedClassGenerator.generateClass();
        for (JClass<?> jclass : this.jclasses.values())
            jclass.getClassGenerator().generateClass();
    }

    // This method exists solely to bind the generic type parameters
    private <T> JClass<T> createJClass(String name, int storageId, Class<T> type) {
        return new JClass<>(this, name, storageId, type);
    }

    StorageIdGenerator getStorageIdGenerator(Annotation annotation, AnnotatedElement target) {
        if (this.storageIdGenerator == null) {
            throw new IllegalArgumentException(String.format("invalid @%s annotation on %s:"
              + " no storage ID is given, but storage ID auto-generation is disabled because no %s is configured",
              annotation.annotationType().getSimpleName(), target, StorageIdGenerator.class.getSimpleName()));
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
     * Get the schema version that this instance is configured to use.
     *
     * <p>
     * This will either be a specific non-zero schema version number, or zero, indicating that the highest schema version
     * found in the database should be used.
     *
     * <p>
     * If -1 was configured, this will return the actual {@linkplain SchemaModel#autogenerateVersion auto-generated}
     * schema version.
     *
     * @return the schema version that this instance will use when opening transactions via
     *  {@link Database#createTransaction Database.createTransaction()}
     */
    public int getConfiguredVersion() {
        return this.configuredVersion;
    }

    /**
     * Change the schema version that this instance is configured to use.
     *
     * @param version schema version number of the schema derived from {@code classes},
     *  zero to use the highest version already recorded in the database,
     *  or -1 to use an {@linkplain SchemaModel#autogenerateVersion auto-generated} schema version
     * @throws IllegalArgumentException if {@code version} is less than -1
     */
    public void setConfiguredVersion(int version) {
        Preconditions.checkArgument(version >= -1, "invalid schema version");
        this.configuredVersion = version == -1 ? this.schemaModel.autogenerateVersion() : version;
    }

    /**
     * Get the schema version that this instance used for the most recently created transaction.
     *
     * <p>
     * If no transactions have been created yet, this returns zero. Otherwise, it returns the schema version
     * used by the most recently created transaction.
     *
     * <p>
     * If the {@code version} passed to the constructor was zero, this method can be used to read the highest schema
     * version seen in the database by the most recently created transaction.
     *
     * <p>
     * If the {@code version} passed to the constructor was non-zero, and at least one transaction has been created,
     * this method will return the same value.
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
     * If {@code jobj} is an instance of a Permazen-generated subclass of a user-supplied Java model class,
     * this returns the original Java model class.
     *
     * @param jobj database instance
     * @return the original Java model class of which {@code jobj} is an instance
     * @throws IllegalArgumentException if {@code jobj} is null
     * @deprecated Use {@link JObject#getModelClass} instead
     */
    @Deprecated
    public static Class<?> getModelClass(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return jobj.getModelClass();
    }

// Transactions

    /**
     * Create a new transaction.
     *
     * <p>
     * Convenience method; equivalent to:
     *  <blockquote><pre>
     *  {@link #createTransaction(ValidationMode, Map) createTransaction}({@link ValidationMode#AUTOMATIC}, null)
     *  </pre></blockquote>
     *
     * @return the newly created transaction
     * @throws io.permazen.core.InvalidSchemaException if the schema does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws io.permazen.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database but the schema is incompatible with one or more previous schemas
     *  already recorded in the database (i.e., the same storage ID is used incompatibly between schema versions)
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     */
    public JTransaction createTransaction() {
        return this.createTransaction(ValidationMode.AUTOMATIC, null);
    }

    /**
     * Create a new transaction.
     *
     * <p>
     * Convenience method; equivalent to:
     *  <blockquote><pre>
     *  {@link #createTransaction(ValidationMode, Map) createTransaction}(validationMode, null)
     *  </pre></blockquote>
     *
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return the newly created transaction
     * @throws io.permazen.core.InvalidSchemaException if the schema does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws io.permazen.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database but the schema is incompatible with one or more previous schemas
     *  already recorded in the database (i.e., the same storage ID is used incompatibly between schema versions)
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code validationMode} is null
     */
    public JTransaction createTransaction(ValidationMode validationMode) {
        return this.createTransaction(validationMode, null);
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
     *
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @param kvoptions {@link KVDatabase}-specific transaction options, or null for none
     * @return the newly created transaction
     * @throws io.permazen.core.InvalidSchemaException if the schema does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws io.permazen.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database, but the schema is incompatible with one or more previous schemas
     *  alread recorded in the database (i.e., the same storage ID is used incompatibly between schema versions)
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code validationMode} is null
     */
    public JTransaction createTransaction(ValidationMode validationMode, Map<String, ?> kvoptions) {
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        return this.createTransaction(this.db.createTransaction(this.buildTransactionConfig(kvoptions)), validationMode);
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
     *
     * @param kvt already opened key/value store transaction
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return the newly created transaction
     * @throws io.permazen.core.InvalidSchemaException if the schema does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws io.permazen.core.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database but the schema is incompatible with one or more previous schemas
     *  already recorded in the database (i.e., the same storage ID is used incompatibly between schema versions)
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code kvt} or {@code validationMode} is null
     */
    public JTransaction createTransaction(KVTransaction kvt, ValidationMode validationMode) {
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        return this.createTransaction(this.db.createTransaction(kvt, this.buildTransactionConfig(null)), validationMode);
    }

    private JTransaction createTransaction(Transaction tx, ValidationMode validationMode) {
        assert tx != null;
        assert validationMode != null;
        this.actualVersion = tx.getSchema().getVersionNumber();
        final JTransaction jtx = new JTransaction(this, tx, validationMode);
        tx.addCallback(new CleanupCurrentCallback(jtx));
        return jtx;
    }

    /**
     * Create a new, empty {@link DetachedJTransaction} backed by a {@link NavigableMapKVStore}.
     *
     * <p>
     * The returned {@link DetachedJTransaction} does not support {@link DetachedJTransaction#commit commit()} or
     * {@link DetachedJTransaction#rollback rollback()}, and can be used indefinitely.
     *
     * @param validationMode the {@link ValidationMode} to use for the detached transaction
     * @return initially empty detached transaction
     */
    public DetachedJTransaction createDetachedTransaction(ValidationMode validationMode) {
        return this.createDetachedTransaction(new NavigableMapKVStore(), validationMode);
    }

    /**
     * Create a new {@link DetachedJTransaction} based on the provided key/value store.
     *
     * <p>
     * The key/value store will be initialized if necessary (i.e., {@code kvstore} may be empty), otherwise it will be
     * validated against the schema information associated with this instance.
     *
     * <p>
     * The returned {@link DetachedJTransaction} does not support {@link DetachedJTransaction#commit commit()} or
     * {@link DetachedJTransaction#rollback rollback()}, and can be used indefinitely.
     *
     * <p>
     * If {@code kvstore} is a {@link CloseableKVStore}, then it will be {@link CloseableKVStore#close close()}'d
     * if/when the returned {@link DetachedJTransaction} is.
     *
     * @param kvstore key/value store, empty or having content compatible with this transaction's {@link Permazen}
     * @param validationMode the {@link ValidationMode} to use for the detached transaction
     * @return detached transaction based on {@code kvstore}
     * @throws io.permazen.core.SchemaMismatchException if {@code kvstore} contains incompatible or missing schema information
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code kvstore} or {@code validationMode} is null
     */
    public DetachedJTransaction createDetachedTransaction(KVStore kvstore, ValidationMode validationMode) {
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        final DetachedTransaction dtx = this.db.createDetachedTransaction(kvstore, this.buildTransactionConfig(null));
        return new DetachedJTransaction(this, dtx, validationMode);
    }

    /**
     * Build the {@link TransactionConfig} for a new core API transaction.
     *
     * @return core API transaction config
     */
    protected TransactionConfig buildTransactionConfig(Map<String, ?> kvoptions) {
        return TransactionConfig.builder()
          .schemaModel(this.getSchemaModel())
          .schemaVersion(this.configuredVersion)
          .allowNewSchema(true)
          .garbageCollectSchemas(true)
          .kvOptions(kvoptions)
          .build();
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
            this.schemaModel.lockDown();
            this.log.debug("Permazen schema generated from annotated classes:\n{}", this.schemaModel);
        }
        return this.schemaModel;
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
        return this.jclasses.values().stream()
          .filter(jclass -> type == null || type.isAssignableFrom(jclass.type))
          .map(jclass -> (JClass<? extends T>)jclass)
          .collect(Collectors.toList());
    }

    /**
     * Quick lookup for the {@link JField} corresponding to the given object and field storage ID.
     *
     * @param id object ID
     * @param storageId field storage ID
     * @param <T> expected encoding
     * @return list of {@link JClass}es whose type is {@code type} or a sub-type, ordered by storage ID
     * @throws TypeNotInSchemaVersionException if {@code id} has a type that does not exist in this instance's schema version
     * @throws UnknownFieldException if {@code storageId} does not correspond to any field in the object's type
     */
    @SuppressWarnings("unchecked")
    <T extends JField> T getJField(ObjId id, int storageId, Class<T> type) {
        final JField jfield = this.typeFieldMap.get(this.getTypeFieldKey(id.getStorageId(), storageId));
        if (jfield == null) {
            this.getJClass(id.getStorageId()).getJField(storageId, type);   // should always throw the appropriate exception
            assert false;
        }
        try {
            return type.cast(jfield);
        } catch (ClassCastException e) {
            throw new UnknownFieldException(storageId, jfield + " is not a "
              + type.getSimpleName().replaceAll("^J(.*)Field$", "").toLowerCase() + " field");
        }
    }

    private long getTypeFieldKey(int typeStorageId, int fieldStorageId) {
        return ((long)typeStorageId << 32) | (long)fieldStorageId & 0xffffffffL;
    }

    /**
     * Generate the {@link KeyRanges} restricting objects to the specified type.
     *
     * @param type any Java type, or null for no restriction
     * @return key restriction for {@code type}
     */
    KeyRanges keyRangesFor(Class<?> type) {
        if (type == null)
            return KeyRanges.full();
        final ArrayList<KeyRange> list = new ArrayList<>(this.jclasses.size());
        boolean invert = false;
        if (type == UntypedJObject.class) {
            type = null;
            invert = true;
        }
        this.getJClasses(type).stream()
          .map(jclass -> ObjId.getKeyRange(jclass.storageId))
          .iterator()
          .forEachRemaining(list::add);
        final KeyRanges keyRanges = new KeyRanges(list);
        return invert ? keyRanges.inverse() : keyRanges;
    }

// Reference Paths

    /**
     * Parse a {@link ReferencePath} starting from a Java type.
     *
     * <p>
     * Roughly equivalent to: {@code this.parseReferencePath(this.getJClasses(startType), path)}.
     *
     * @param startType starting Java type for the path
     * @param path reference path in string form
     * @return parsed reference path
     * @throws IllegalArgumentException if no model types are instances of {@code startType}
     * @throws IllegalArgumentException if {@code path} is invalid
     * @throws IllegalArgumentException if either parameter is null
     * @see ReferencePath
     */
    public ReferencePath parseReferencePath(Class<?> startType, String path) {
        Preconditions.checkArgument(startType != null, "null startType");
        final HashSet<JClass<?>> startTypes = new HashSet<>(this.getJClasses(startType));
        if (startTypes.isEmpty())
            throw new IllegalArgumentException(String.format("no model type is an instance of %s", startType));
        if (startType.isAssignableFrom(UntypedJObject.class))
            startTypes.add(null);
        return this.parseReferencePath(startTypes, path);
    }

    /**
     * Parse a {@link ReferencePath} starting from a set of model object types.
     *
     * @param startTypes starting model types for the path, with null meaning {@link UntypedJObject}
     * @param path reference path in string form
     * @return parsed reference path
     * @throws IllegalArgumentException if {@code startTypes} is empty or contains null
     * @throws IllegalArgumentException if {@code path} is invalid
     * @throws IllegalArgumentException if either parameter is null
     * @see ReferencePath
     */
    public ReferencePath parseReferencePath(Set<JClass<?>> startTypes, String path) {
        return this.referencePathCache.get(startTypes, path);
    }

// Validation

    /**
     * Configure a custom {@link ValidatorFactory} used to create {@link Validator}s
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
            try {
                this.validatorFactory = Validation.byDefaultProvider()
                  .configure()
                  .messageInterpolator(new ParameterMessageInterpolator())
                  .buildValidatorFactory();
            } catch (Exception e2) {
                throw new PermazenException(String.format(
                  "JSR 303 validation constraint found on %s but creation of default ValidatorFactory failed;"
                  + " is there a JSR 303 validation implementation on the classpath?", this.elementRequiringJSR303Validation), e2);
            }
        }

        // Done
        return this.validatorFactory;
    }

// Misc utility

    /**
     * Utility method to get all of the objects directly referenced by a given object via any field.
     *
     * <p>
     * Note: the returned {@link Iterable} may contain duplicates; these can be eliminated using an {@link ObjIdSet} if necessary.
     *
     * @param jobj starting object
     * @return all objects directly referenced by {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public Stream<JObject> getReferencedObjects(final JObject jobj) {

        // Sanity check
        Preconditions.checkArgument(jobj != null, "null jobj");
        final ObjId id = jobj.getObjId();

        // Visit fields
        final ArrayList<Stream<JObject>> streamList = new ArrayList<>();
        for (JField jfield : this.getJClass(id).getJFieldsByStorageId().values()) {
            jfield.visit(new JFieldSwitch<Void>() {

                @Override
                public Void caseJReferenceField(JReferenceField field) {
                    final JObject ref = field.getValue(jobj);
                    if (ref != null)
                        streamList.add(Stream.of(ref));
                    return null;
                }

                @Override
                public Void caseJMapField(JMapField field) {
                    if (field.getKeyField() instanceof JReferenceField) {
                        streamList.add(field.getValue(jobj).keySet().stream()
                          .filter(JObject.class::isInstance)
                          .map(JObject.class::cast));
                    }
                    if (field.getValueField() instanceof JReferenceField) {
                        streamList.add(field.getValue(jobj).values().stream()
                          .filter(JObject.class::isInstance)
                          .map(JObject.class::cast));
                    }
                    return null;
                }

                @Override
                public Void caseJCollectionField(JCollectionField field) {
                    if (field.getElementField() instanceof JReferenceField) {
                        streamList.add(field.getValue(jobj).stream()
                          .filter(JObject.class::isInstance)
                          .map(JObject.class::cast));
                    }
                    return null;
                }

                @Override
                public Void caseJField(JField field) {
                    return null;
                }
            });
        }

        // Done
        return streamList.stream().flatMap(Function.identity());
    }

// IndexQueryInfo Cache

    IndexQueryInfo getIndexQueryInfo(IndexQueryInfoKey key) {
        try {
            return this.indexQueryInfoCache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw e;
        }
    }

// Internal Stuff

    // Get class generator for "untyped" JObject's
    ClassGenerator<UntypedJObject> getUntypedClassGenerator() {
        return this.untypedClassGenerator;
    }

    /**
     * Determine whether the specified field is a reference field.
     */
    boolean isReferenceField(int storageId) {
        final IndexInfo info = this.indexInfoMap.get(storageId);
        return info instanceof SimpleFieldIndexInfo && ((SimpleFieldIndexInfo)info).getEncoding() instanceof ReferenceEncoding;
    }

    /**
     * Get the index info associated with the given storage ID.
     *
     * @param storageId index storage ID
     * @param type required type
     * @return {@link IndexInfo} instance
     * @throws IllegalArgumentException if {@code storageId} does not represent an index
     */
    <T extends IndexInfo> T getIndexInfo(int storageId, Class<? extends T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final IndexInfo indexInfo = this.indexInfoMap.get(storageId);
        if (indexInfo == null) {
            throw new IllegalArgumentException(String.format("no %s with storage ID %d exists in schema version %d",
              this.describe(type), storageId, this.actualVersion));
        }
        try {
            return type.cast(indexInfo);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(
              "no %s with storage ID %d exists in schema version %d (found field %s instead)",
              this.describe(type), storageId, this.actualVersion, this.describe(type)));
        }
    }

    // Add new JClass, checking for storage ID conflicts
    private void addJClass(JClass<?> jclass) {

        // Check for storage ID conflict
        final JClass<?> other = this.jclasses.get(jclass.storageId);
        if (other != null) {
            throw new IllegalArgumentException(String.format(
              "illegal duplicate use of storage ID %d for both %s and %s", jclass.storageId, other, jclass));
        }
        this.jclasses.put(jclass.storageId, jclass);
        assert !this.jclassesByType.containsKey(jclass.type);                   // this should never conflict, no need to check
        this.jclassesByType.put(jclass.type, jclass);
    }

    // Add new index info, checking for storage ID conflicts
    private void addIndexInfo(JSchemaObject item, Map<Integer, String> descriptionMap) {
        final IndexInfo info = item.toIndexInfo();
        final int storageId = info.storageId;
        final IndexInfo existing = this.indexInfoMap.get(storageId);
        if (existing == null) {
            this.indexInfoMap.put(storageId, info);
            descriptionMap.put(storageId, item.description);
        } else if (!info.equals(existing)) {
            throw new IllegalArgumentException(String.format(
              "incompatible duplicate use of storage ID %d for %s and %s",
              item.storageId, descriptionMap.get(storageId), item.description));
        }
    }

    private String describe(Class<? extends IndexInfo> type) {
        return type.getSimpleName()
          .replaceAll("^(.*Index)Info$", "$1")
          .replaceAll("([a-z])([A-Z])", "$1 $2")
          .toLowerCase();
    }

// Loader

    private class Loader extends ClassLoader {

        static {
            ClassLoader.registerAsParallelCapable();
        }

        // Set up class loader
        Loader() {
            super(ApplicationClassLoader.getInstance());
        }

        // Find matching ClassGenerator, if any, otherwise defer to parent
        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            for (ClassGenerator<?> generator : Permazen.this.classGenerators) {
                if (name.equals(generator.getClassName().replace('/', '.'))) {
                    final byte[] bytes = generator.generateBytecode();
                    return this.defineClass(name, bytes, 0, bytes.length);
                }
            }
            return super.findClass(name);
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
            return this.getClass().hashCode() ^ this.jtx.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final CleanupCurrentCallback that = (CleanupCurrentCallback)obj;
            return this.jtx.equals(that.jtx);
        }
    }
}
