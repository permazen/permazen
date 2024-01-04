
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.DetachedTransaction;
import io.permazen.core.ObjId;
import io.permazen.core.Schema;
import io.permazen.core.SchemaBundle;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.core.UnknownFieldException;
import io.permazen.core.UnknownTypeException;
import io.permazen.core.util.ObjIdSet;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaItem;
import io.permazen.schema.SchemaModel;
import io.permazen.tuple.Tuple2;
import io.permazen.util.ApplicationClassLoader;

import jakarta.validation.MessageInterpolator;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;

import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

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
 *      There are several available {@link KVDatabase} implementations, including wrappers
 *      for many third party key/value stores.</li>
 *  <li>On top of that sits the <b>Core API Layer</b>, which provides a rigourous "object database" abstraction on top of
 *      a {@link KVDatabase}. It supports a flat hierarchy of "object" types, where an object consists of fields that are
 *      either simple (i.e., any Java type that can be (de)serialized to/from a {@code byte[]} array), list, set, or map.
 *      It also includes tightly controlled schema tracking, simple and composite indexes, and lifecycle and change notifications.
 *      It is not Java-specific or explicitly object-oriented: an "object" at this layer is just a structure with defined fields.
 *      The core API layer may be accessed through the {@link Database} and {@link Transaction} classes.</li>
 *  <li>The <b>Java Layer</b> is a Java-centric, object-oriented persistence layer for Java applications.
 *      It sits on top of the core API layer and provides a fully "Java" view of the underlying data where all data access
 *      is through user-supplied Java model classes. All schema definition and listener registrations are inferred from
 *      {@linkplain io.permazen.annotation Java annotations}. Incremental JSR 303 validation is supported.
 *      The {@link Permazen} class represents an instance of this top layer database, and {@link JTransaction}
 *      represents the corresonding transactions.</li>
 *  </ul>
 *
 * <p>
 * User-provided Java model classes define database fields by declaring abstract Java bean methods;
 * {@link Permazen} generates concrete subclasses of the user-provided abstract model classes at runtime.
 * These runtime classes will implement the Java bean methods as well as the {@link JObject} interface.
 * Instances of Java model classes are always associated with a specific {@link JTransaction}, and all of
 * their state derives from the underlying key/value {@link KVTransaction}.
 *
 * <p>
 * Java model class instances have a unique {@link ObjId} which represents database identity.
 * {@link Permazen} guarantees that at most one Java instance will exist for any given {@link JTransaction} and {@link ObjId}.
 *
 * <p>
 * New instance creation, index queries, and certain other database-related tasks are initiated through a {@link JTransaction}.
 * Normal database transactions are created via {@link #createTransaction createTransaction()}. Detached transactions are
 * purely in-memory transactions that are detached from the database and may persist indefinitely; their purpose is to hold a
 * snapshot of some (user-defined) portion of the database content for use outside of a regular transaction. Otherwise,
 * they function like normal transactions, with support for index queries, listener callbacks, etc. See
 * {@link JTransaction#createDetachedTransaction JTransaction.createDetachedTransaction()},
 * {@link JTransaction#getDetachedTransaction}, {@link JObject#copyOut JObject.copyOut()}, and
 * {@link JObject#copyIn JObject.copyIn()}.
 *
 * @see JObject
 * @see JTransaction
 * @see PermazenConfig
 * @see io.permazen.annotation
 * @see <a href="https://github.com/permazen/permazen/">Permazen GitHub Page</a>
 */
@ThreadSafe
public class Permazen {

    /**
     * The suffix that is appended to Java model class names to get the corresponding Permazen generated class name.
     */
    public static final String GENERATED_CLASS_NAME_SUFFIX = "$$Permazen";

    private static final int MAX_INDEX_QUERY_INFO_CACHE_SIZE = 1000;

    private static final String HIBERNATE_PARAMETER_MESSAGE_INTERPOLATOR_CLASS_NAME
      = "org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator";

    final Logger log = LoggerFactory.getLogger(this.getClass());
    final ArrayList<JClass<?>> jclasses = new ArrayList<>();
    final TreeMap<String, JClass<?>> jclassesByName = new TreeMap<>();
    final HashMap<Class<?>, JClass<?>> jclassesByType = new HashMap<>();
    final TreeMap<Integer, JClass<?>> jclassesByStorageId = new TreeMap<>();
    final HashMap<SchemaId, JSchemaItem> schemaItemsBySchemaId = new HashMap<>();
    final HashSet<Integer> fieldsRequiringDefaultValidation = new HashSet<>();
    final HashMap<Integer, JSchemaItem> indexesByStorageId = new HashMap<>();       // contains REPRESENTATIVE schema items
    final HashMap<Tuple2<Integer, String>, JField> typeFieldMap = new HashMap<>();
    @SuppressWarnings("this-escape")
    final ReferencePathCache referencePathCache = new ReferencePathCache(this);
    final ClassGenerator<UntypedJObject> untypedClassGenerator;
    final ArrayList<ClassGenerator<?>> classGenerators;
    final ClassLoader loader = new Loader();
    final ValidatorFactory validatorFactory;
    final SchemaModel origSchemaModel;                                              // does not include storage ID assignments
    final SchemaModel schemaModel;                                                  // includes storage ID assignments
    final Database db;

    final boolean hasOnCreateMethods;
    final boolean hasOnDeleteMethods;
    final boolean hasOnSchemaChangeMethods;
    final boolean hasUpgradeConversions;
    final boolean anyJClassRequiresDefaultValidation;
    final AnnotatedElement elementRequiringJSR303Validation;

    // Cached listener sets used by JTransaction.<init>()
    final Transaction.ListenerSet[] listenerSets = new Transaction.ListenerSet[4];

    @SuppressWarnings("this-escape")
    private final LoadingCache<IndexQuery.Key, IndexQuery> indexQueryCache = CacheBuilder.newBuilder()
      .maximumSize(MAX_INDEX_QUERY_INFO_CACHE_SIZE)
      .build(CacheLoader.from(key -> key.getIndexQuery(this)));

// Constructor

    /**
     * Create a new instance using the given configuration.
     *
     * @param config configuration to use
     * @throws IllegalArgumentException if {@code config} contains a null class or a class with invalid annotation(s)
     * @throws IllegalArgumentException if {@code config} is null
     * @throws io.permazen.core.InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    @SuppressWarnings("this-escape")
    public Permazen(PermazenConfig config) {

        // Initialize
        Preconditions.checkArgument(config != null, "null config");
        this.db = config.getDatabase();

        // Inventory classes; automatically add all @PermazenType-annotated superclasses of @PermazenType-annotated classes
        final HashMap<Class<?>, PermazenType> permazenTypes = new HashMap<>();
        for (Class<?> type : config.getModelClasses()) {
            do {

                // Find @PermazenType annotation
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
                permazenTypes.put(type, annotation);
            } while ((type = type.getSuperclass()) != null);
        }

        // Create JClass objects
        permazenTypes.forEach((type, annotation) -> {

            // Get object type name
            final String typeName = !annotation.name().isEmpty() ? annotation.name() : type.getSimpleName();
            if (this.log.isTraceEnabled()) {
                this.log.trace("found @{} annotation on {} defining object type \"{}\"",
                  PermazenType.class.getSimpleName(), type, typeName);
            }

            // Check for name conflict
            final JClass<?> other = this.jclassesByName.get(typeName);
            if (other != null) {
                throw new IllegalArgumentException(String.format(
                  "illegal duplicate use of object type name \"%s\" for both %s and %s",
                  typeName, other.type.getName(), type.getName()));
            }

            // Create JClass
            final JClass<?> jclass;
            try {
                jclass = new JClass<>(this, typeName, annotation.storageId(), type);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                  "invalid @%s annotation on %s: %s", PermazenType.class.getSimpleName(), type, e), e);
            }

            // Add JClass
            this.jclassesByName.put(jclass.name, jclass);
            this.jclassesByType.put(jclass.type, jclass);                       // this should never conflict, no need to check
            if (typeName.equals(type.getSimpleName()))
                this.log.debug("added Java model class for object type \"{}\"", typeName);
            else
                this.log.debug("added Java model class {} for object type \"{}\"", type, typeName);
        });
        this.jclassesByName.values().forEach(this.jclasses::add);               // note: this.jclasses will be sorted by name

        // Inventory class generators
        this.classGenerators = this.jclasses.stream()
          .map(jclass -> jclass.classGenerator)
          .collect(Collectors.toCollection(ArrayList::new));
        this.untypedClassGenerator = new ClassGenerator<>(this, UntypedJObject.class);
        this.classGenerators.add(this.untypedClassGenerator);

        // Create fields
        this.jclasses.forEach(jclass -> jclass.createFields(this.db.getEncodingRegistry(), this.jclasses));

        // Create composite indexes
        this.jclasses.forEach(JClass::createCompositeIndexes);

        // Build and validate initial schema model
        this.schemaModel = new SchemaModel();
        this.jclassesByName.forEach((name, jclass) -> this.schemaModel.getSchemaObjectTypes().put(name, jclass.toSchemaItem()));
        this.schemaModel.lockDown(false);
        this.schemaModel.validate();
        if (!this.schemaModel.isEmpty())
            this.log.debug("Permazen schema generated from annotated classes:\n{}", this.schemaModel);

        // Copy it now so we have a pre-storage ID assignment version
        this.origSchemaModel = this.schemaModel.clone();
        this.origSchemaModel.lockDown(true);

        // Connect to database and register schema
        final Transaction tx = this.db.createTransaction(this.buildTransactionConfig(null));
        final Schema schema;
        final SchemaBundle schemaBundle;
        try {
            schema = tx.getSchema();
            schemaBundle = tx.getSchemaBundle();
            tx.commit();
        } finally {
            tx.rollback();      // does nothing if transaction succeeded
        }

        // Copy storage ID assignments into the corresponding SchemaItem's and JSchemaItem's
        this.jclasses.forEach(jclass -> jclass.visitSchemaItems(item -> {
            final SchemaItem schemaItem = (SchemaItem)item.schemaItem;
            final SchemaId schemaId = schemaItem.getSchemaId();
            final int storageId = schemaBundle.getStorageId(schemaId);
            schemaItem.setStorageId(storageId);
            item.storageId = storageId;
        }));
        this.schemaModel.lockDown(true);
        this.schemaModel.validate();            // should always be valid, but just to be sure
        if (this.log.isTraceEnabled())
            this.log.trace("Permazen schema with storage ID assignments:\n{}", this.schemaModel);

        // Populate JClass and JField maps keyed by storage ID
        this.jclasses.forEach(jclass -> this.jclassesByStorageId.put(jclass.storageId, jclass));
        this.jclasses.forEach(jclass -> jclass.jfieldsByName.values().forEach(
          jfield -> jclass.jfieldsByStorageId.put(jfield.storageId, jfield)));
        this.jclasses.forEach(jclass -> jclass.jsimpleFieldsByName.values().forEach(
          jfield -> jclass.jsimpleFieldsByStorageId.put(jfield.storageId, jfield)));

        // Update all JSchemaItem's to point to core API SchemaItem instead of SchemaModel SchemaItem
        this.jclasses.forEach(jclass -> jclass.replaceSchemaItems(schema));

        // Find all fields that require default validation
        for (JClass<?> jclass : this.jclasses) {
            for (JField jfield : jclass.jfieldsByName.values()) {
                if (jfield.requiresDefaultValidation)
                    this.fieldsRequiringDefaultValidation.add(jfield.storageId);
            }
        }

        // Populate this.indexesByStorageId
        this.jclasses.forEach(jclass -> {

            // Add simple field indexes
            jclass.jsimpleFieldsByName.values().forEach(jfield -> {
                if (jfield.indexed)
                    this.indexesByStorageId.put(jfield.storageId, jfield);
            });

            // Add composite indexes
            jclass.jcompositeIndexesByName.values().forEach(index -> this.indexesByStorageId.put(index.storageId, index));
        });

        // Populate this.typeFieldMap
        this.jclasses.forEach(jclass -> jclass.jfieldsByName.values().forEach(jfield -> {
            this.typeFieldMap.put(new Tuple2<>(jclass.storageId, jfield.name), jfield);
            if (jfield instanceof JComplexField) {
                final JComplexField parentField = (JComplexField)jfield;
                for (JSimpleField subField : parentField.getSubFields())
                    this.typeFieldMap.put(new Tuple2<>(jclass.storageId, subField.getFullName()), subField);
            }
        }));

        // Populate jclass forwardCascadeMap and inverseCascadeMap
        this.jclasses.forEach(jclass -> jclass.jsimpleFieldsByName.values().forEach(jfield0 -> {

            // Filter for reference fields
            if (!(jfield0 instanceof JReferenceField))
                return;
            final JReferenceField jfield = (JReferenceField)jfield0;

            // Do forward cascades
            for (String cascadeName : jfield.forwardCascades)
                jclass.forwardCascadeMap.computeIfAbsent(cascadeName, s -> new ArrayList<>()).add(jfield);

            // Do inverse cascades
            for (String cascadeName : jfield.inverseCascades) {
                for (JClass<?> refJClass : this.getJClasses(jfield.typeToken.getRawType())) {
                    refJClass.inverseCascadeMap
                      .computeIfAbsent(cascadeName, s -> new HashMap<>())
                      .computeIfAbsent(jfield.storageId, i -> new KeyRanges())
                      .add(ObjId.getKeyRange(jclass.storageId));
                }
            }
        }));

        // Scan for various method-level annotations
        this.jclasses.forEach(JClass::scanAnnotations);

        // Determine which JClass's have validation requirement(s) on creation
        this.jclasses.forEach(JClass::calculateValidationRequirement);
        boolean anyDefaultValidation = false;
        AnnotatedElement someElementRequiringJSR303Validation = null;
        for (JClass<?> jclass : this.jclasses) {
            anyDefaultValidation |= jclass.requiresDefaultValidation;
            if (someElementRequiringJSR303Validation == null)
                someElementRequiringJSR303Validation = jclass.elementRequiringJSR303Validation;
        }
        this.anyJClassRequiresDefaultValidation = anyDefaultValidation;
        this.elementRequiringJSR303Validation = someElementRequiringJSR303Validation;

        // Detect whether we have any @OnCreate, @OnDelete, and/or @OnSchemaChange methods
        boolean anyOnCreateMethods = false;
        boolean anyOnDeleteMethods = false;
        boolean anyOnSchemaChangeMethods = false;
        boolean anyUpgradeConversions = false;
        for (JClass<?> jclass : this.jclasses) {
            anyOnCreateMethods |= !jclass.onCreateMethods.isEmpty();
            anyOnDeleteMethods |= !jclass.onDeleteMethods.isEmpty();
            anyOnSchemaChangeMethods |= !jclass.onSchemaChangeMethods.isEmpty();
            anyUpgradeConversions |= !jclass.upgradeConversionFields.isEmpty();
        }
        this.hasOnCreateMethods = anyOnCreateMethods;
        this.hasOnDeleteMethods = anyOnDeleteMethods;
        this.hasOnSchemaChangeMethods = anyOnSchemaChangeMethods;
        this.hasUpgradeConversions = anyUpgradeConversions;

        // Eagerly load all generated Java classes so we "fail fast" if there are any loading errors
        this.untypedClassGenerator.generateClass();
        for (JClass<?> jclass : this.jclasses)
            jclass.getClassGenerator().generateClass();

        // Initialize ValidatorFactory (only if needed)
        ValidatorFactory optionalValidatorFactory = config.getValidatorFactory();
        if (optionalValidatorFactory == null && elementRequiringJSR303Validation != null) {
            try {
                optionalValidatorFactory = Validation.buildDefaultValidatorFactory();
            } catch (Exception e) {
                try {
                    final MessageInterpolator messageInterpolator = (MessageInterpolator)Class.forName(
                      HIBERNATE_PARAMETER_MESSAGE_INTERPOLATOR_CLASS_NAME, false, Thread.currentThread().getContextClassLoader())
                      .getConstructor()
                      .newInstance();
                    optionalValidatorFactory = Validation.byDefaultProvider()
                      .configure()
                      .messageInterpolator(messageInterpolator)
                      .buildValidatorFactory();
                } catch (Exception e2) {
                    throw new PermazenException(String.format(
                      "JSR 303 validation constraint found on %s but creation of default ValidatorFactory failed;"
                      + " is there a JSR 303 validation implementation on the classpath?", elementRequiringJSR303Validation), e2);
                }
            }
        }
        this.validatorFactory = optionalValidatorFactory;
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
        final JTransaction jtx = new JTransaction(this, tx, validationMode);
        tx.addCallback(new CleanupCurrentCallback(jtx));
        return jtx;
    }

    /**
     * Create a new, empty {@link DetachedJTransaction} backed by a {@link MemoryKVStore}.
     *
     * <p>
     * The returned {@link DetachedJTransaction} does not support {@link DetachedJTransaction#commit commit()} or
     * {@link DetachedJTransaction#rollback rollback()}, and can be used indefinitely.
     *
     * @param validationMode the {@link ValidationMode} to use for the detached transaction
     * @return initially empty detached transaction
     */
    public DetachedJTransaction createDetachedTransaction(ValidationMode validationMode) {
        return this.createDetachedTransaction(new MemoryKVStore(), validationMode);
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
          .schemaModel(this.schemaModel)
          .allowNewSchema(true)
          .garbageCollectSchemas(true)
          .kvOptions(kvoptions)
          .build();
    }

// Schema

    /**
     * Get the {@link SchemaModel} associated with this instance derived from the annotations on the scanned classes,
     * including actual storage ID assignments.
     *
     * <p>
     * Equivalent to: {@link #getSchemaModel(boolean) getSchemaModel}{@code (true)}.
     *
     * @return schema model used by this database
     */
    public SchemaModel getSchemaModel() {
        return this.getSchemaModel(true);
    }

    /**
     * Get the {@link SchemaModel} associated with this instance derived from the annotations on the scanned classes.
     *
     * @param withStorageIds true to include actual storage ID assignments
     * @return schema model used by this database
     */
    public SchemaModel getSchemaModel(boolean withStorageIds) {
        return withStorageIds ? this.schemaModel : this.origSchemaModel;
    }

// JClass access

    /**
     * Get all {@link JClass}'s associated with this instance, indexed by object type name.
     *
     * @return read-only mapping from object type name to {@link JClass}
     */
    public NavigableMap<String, JClass<?>> getJClassesByName() {
        return Collections.unmodifiableNavigableMap(this.jclassesByName);
    }

    /**
     * Get the {@link JClass} associated with the given object type name.
     *
     * @param typeName object type name
     * @return {@link JClass} instance
     * @throws UnknownTypeException if {@code typeName} is unknown
     * @throws IllegalArgumentException if {@code typeName} is null
     */
    public JClass<?> getJClass(String typeName) {
        final JClass<?> jclass = this.jclassesByName.get(typeName);
        if (jclass == null)
            throw new UnknownTypeException(typeName, null);
        return jclass;
    }

    /**
     * Get all {@link JClass}'s associated with this instance, indexed by storage ID.
     *
     * @return read-only mapping from storage ID to {@link JClass}
     */
    public NavigableMap<Integer, JClass<?>> getJClassesByStorageId() {
        return Collections.unmodifiableNavigableMap(this.jclassesByStorageId);
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
     * @throws UnknownTypeException if {@code id} has a type that is not defined in this instance's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public JClass<?> getJClass(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return this.getJClass(id.getStorageId());
    }

    /**
     * Get the {@link JClass} associated with the given storage ID.
     *
     * @param storageId object type storage ID
     * @return {@link JClass} instance
     * @throws UnknownTypeException if {@code storageId} does not represent an object type
     */
    public JClass<?> getJClass(int storageId) {
        final JClass<?> jclass = this.jclassesByStorageId.get(storageId);
        if (jclass == null)
            throw new UnknownTypeException("storage ID " + storageId, null);
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
        return this.jclasses.stream()
          .filter(jclass -> type == null || type.isAssignableFrom(jclass.type))
          .map(jclass -> (JClass<? extends T>)jclass)
          .collect(Collectors.toList());
    }

    /**
     * Quick lookup for the {@link JField} corresponding to the given object and field name.
     *
     * @param id object ID
     * @param fieldName field name; sub-fields of complex fields may be specified like {@code "mymap.key"}
     * @param <T> expected encoding
     * @throws UnknownTypeException if {@code id} has a type that is not defined in this instance's schema
     * @throws UnknownFieldException if {@code fieldName} does not correspond to any field in the object's type
     * @throws IllegalArgumentException if any parameter is null
     */
    @SuppressWarnings("unchecked")
    <T extends JField> T getJField(ObjId id, String fieldName, Class<T> type) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        Preconditions.checkArgument(type != null, "null type");
        final JField jfield = this.typeFieldMap.get(new Tuple2<>(id.getStorageId(), fieldName));
        if (jfield == null) {
            this.getJClass(id.getStorageId()).getJField(fieldName, type);   // should always throw the appropriate exception
            assert false;
        }
        try {
            return type.cast(jfield);
        } catch (ClassCastException e) {
            throw new UnknownFieldException(fieldName, String.format(
              "%s is not a %s field", jfield, type.getSimpleName().replaceAll("^J(.*)Field$", "").toLowerCase()));
        }
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

// IndexQuery Cache

    IndexQuery getIndexQuery(IndexQuery.Key key) {
        try {
            return this.indexQueryCache.getUnchecked(key);
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
