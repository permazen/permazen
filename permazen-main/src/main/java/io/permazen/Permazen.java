
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
import io.permazen.core.InvalidSchemaException;
import io.permazen.core.ObjId;
import io.permazen.core.Schema;
import io.permazen.core.SchemaBundle;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.core.UnknownFieldException;
import io.permazen.core.UnknownTypeException;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.mvcc.BranchedKVTransaction;
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
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;
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
 *      The {@link Permazen} class represents an instance of this top layer database, and {@link PermazenTransaction}
 *      represents the corresonding transactions.</li>
 *  </ul>
 *
 * <p>
 * User-provided Java model classes define database fields by declaring abstract Java bean methods;
 * {@link Permazen} generates concrete subclasses of the user-provided abstract model classes at runtime.
 * These runtime classes will implement the Java bean methods as well as the {@link PermazenObject} interface.
 * Instances of Java model classes are always associated with a specific {@link PermazenTransaction}, and all of
 * their state derives from the underlying key/value {@link KVTransaction}.
 *
 * <p>
 * Java model class instances have a unique {@link ObjId} which represents database identity. {@link Permazen} guarantees that
 * at most one Java instance will exist for any given {@link PermazenTransaction} and {@link ObjId}.
 *
 * <p><b>Transactions</b>
 *
 * <p>
 * New instance creation, index queries, and certain other database-related tasks are initiated through a
 * {@link PermazenTransaction}. <b>Normal</b> transactions are created via {@link #createTransaction createTransaction()}.
 *
 * <p>
 * <b>Detached</b> transactions are in-memory transactions that are completely detached from the database. They are never
 * committed and may persist indefinitely. Their purpose is to hold a database objects in memory where they can be manipulated
 * like normal Java objects, but with the usual functionality available to open transactions added, such as index queries,
 * schem tracking, change notifications, reference cascades, etc. Detached transactions are initially empty, and are often
 * used to hold a copy of some small portion of the database. Because their state is encoded entirely by in-memory key/value
 * pairs, they are easily serialized/deserialized. See
 * {@link PermazenTransaction#createDetachedTransaction PermazenTransaction.createDetachedTransaction()},
 * {@link PermazenTransaction#getDetachedTransaction}, {@link PermazenObject#copyOut PermazenObject.copyOut()}, and
 * {@link PermazenObject#copyIn PermazenObject.copyIn()}.
 *
 * <p>
 * <b>Branched</b> transactions are like normal transactions, in that when opened they contain an up-to-date view of
 * the database, but after that they operate entirely in-memory, without any underlying key/value transaction, until
 * {@code commit()} is invoked. At that time a new key/value transaction is opened, a conflict check is performed,
 * and if successful all of the transaction's writes are then flushed. Branched transactions require that the underlying
 * key/value database support {@link KVTransaction#readOnlySnapshot}; see {@link BranchedKVTransaction} for details
 * and other caveats. Branched transactions are created via {@link #createBranchedTransaction() createBranchedTransaction()}.
 *
 * <p><b>Initialization</b>
 *
 * <p>
 * Instances of this class must be {@linkplain #initialize initialized} before use. This involves accessing the underlying
 * database and registering the Java data model's schema. This will happen automatically (if needed) when any method that
 * requires the registered schema is invoked (including creating a transaction), or by explicit invocation of {@link #initialize}.
 * See also {@link PermazenConfig.Builder#initializeOnCreation(boolean) PermazenConfig.Builder.initializeOnCreation()}.
 *
 * @see PermazenObject
 * @see PermazenTransaction
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
    final ArrayList<PermazenClass<?>> pclasses = new ArrayList<>();
    final TreeMap<String, PermazenClass<?>> pclassesByName = new TreeMap<>();
    final HashMap<Class<?>, PermazenClass<?>> pclassesByType = new HashMap<>();
    final TreeMap<Integer, PermazenClass<?>> pclassesByStorageId = new TreeMap<>();
    final HashMap<SchemaId, PermazenSchemaItem> schemaItemsBySchemaId = new HashMap<>();
    final HashSet<Integer> fieldsRequiringDefaultValidation = new HashSet<>();
    final HashMap<Integer, PermazenSchemaItem> indexesByStorageId = new HashMap<>();    // contains REPRESENTATIVE schema items
    final HashMap<Tuple2<Integer, String>, PermazenField> typeFieldMap = new HashMap<>();
    @SuppressWarnings("this-escape")
    final ReferencePathCache referencePathCache = new ReferencePathCache(this);
    final ClassGenerator<UntypedPermazenObject> untypedClassGenerator;
    final ArrayList<ClassGenerator<?>> classGenerators;
    final ClassLoader loader = new Loader();
    final ValidatorFactory validatorFactory;
    final SchemaModel origSchemaModel;                                              // does not include storage ID assignments
    final SchemaModel schemaModel;                                                  // includes storage ID assignments
    final Database db;

    final AnnotatedElement elementRequiringJSR303Validation;

    volatile boolean hasOnCreateMethods;
    volatile boolean hasOnDeleteMethods;
    volatile boolean hasOnSchemaChangeMethods;
    volatile boolean hasUpgradeConversions;
    volatile boolean anyClassRequiresDefaultValidation;

    // Cached listener sets used by PermazenTransaction.<init>()
    final Transaction.ListenerSet[] listenerSets = new Transaction.ListenerSet[4];

    @GuardedBy("this")
    boolean initializing;
    @GuardedBy("this")
    boolean initialized;

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
     * @throws InvalidSchemaException if the schema implied by {@code classes} is invalid
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

        // Create PermazenClass objects
        permazenTypes.forEach((type, annotation) -> {

            // Get object type name
            final String typeName = !annotation.name().isEmpty() ? annotation.name() : type.getSimpleName();
            if (this.log.isTraceEnabled()) {
                this.log.trace("found @{} annotation on {} defining object type \"{}\"",
                  PermazenType.class.getSimpleName(), type, typeName);
            }

            // Check for name conflict
            final PermazenClass<?> other = this.pclassesByName.get(typeName);
            if (other != null) {
                throw new IllegalArgumentException(String.format(
                  "illegal duplicate use of object type name \"%s\" for both %s and %s",
                  typeName, other.type.getName(), type.getName()));
            }

            // Create PermazenClass
            final PermazenClass<?> pclass;
            try {
                pclass = new PermazenClass<>(this, typeName, annotation.storageId(), type);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                  "invalid @%s annotation on %s: %s", PermazenType.class.getSimpleName(), type, e), e);
            }

            // Add PermazenClass
            this.pclassesByName.put(pclass.name, pclass);
            this.pclassesByType.put(pclass.type, pclass);                       // this should never conflict, no need to check
            if (typeName.equals(type.getSimpleName()))
                this.log.debug("added Java model class for object type \"{}\"", typeName);
            else
                this.log.debug("added Java model class {} for object type \"{}\"", type, typeName);
        });
        this.pclassesByName.values().forEach(this.pclasses::add);               // note: this.pclasses will be sorted by name

        // Inventory class generators
        this.classGenerators = this.pclasses.stream()
          .map(pclass -> pclass.classGenerator)
          .collect(Collectors.toCollection(ArrayList::new));
        this.untypedClassGenerator = new ClassGenerator<>(this, UntypedPermazenObject.class);
        this.classGenerators.add(this.untypedClassGenerator);

        // Create fields
        this.pclasses.forEach(pclass -> pclass.createFields(this.db.getEncodingRegistry(), this.pclasses));

        // Create composite indexes
        this.pclasses.forEach(PermazenClass::createCompositeIndexes);

        // Build and validate initial schema model
        this.schemaModel = new SchemaModel();
        this.pclassesByName.forEach((name, pclass) -> this.schemaModel.getSchemaObjectTypes().put(name, pclass.toSchemaItem()));
        this.schemaModel.lockDown(false);
        this.schemaModel.validate();
        if (!this.schemaModel.isEmpty())
            this.log.debug("Permazen schema generated from annotated classes:\n{}", this.schemaModel);

        // Copy it now so we have a pre-storage ID assignment version
        this.origSchemaModel = this.schemaModel.clone();
        this.origSchemaModel.lockDown(true);

        // Determine which PermazenClass's have validation requirement(s) on creation
        this.pclasses.forEach(PermazenClass::calculateValidationRequirement);
        boolean anyDefaultValidation = false;
        AnnotatedElement someElementRequiringJSR303Validation = null;
        for (PermazenClass<?> pclass : this.pclasses) {
            anyDefaultValidation |= pclass.requiresDefaultValidation;
            if (someElementRequiringJSR303Validation == null)
                someElementRequiringJSR303Validation = pclass.elementRequiringJSR303Validation;
        }
        this.anyClassRequiresDefaultValidation = anyDefaultValidation;
        this.elementRequiringJSR303Validation = someElementRequiringJSR303Validation;

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

        // Auto-initialize?
        if (config.isInitializeOnCreation())
            this.initialize();
    }

// Initialization

    /**
     * Determine whether this instance is {@linkplain #initialize initialized}.
     *
     * @return true if initialized, otherwise false
     */
    public synchronized boolean isInitialized() {
        return this.initialized;
    }

    /**
     * Initialize this instance if needed.
     *
     * @return true if this instance was actually initialized, false if it was already initialized and so nothing happened
     * @throws InvalidSchemaException if the data model schema conflicts with what's registered in the database
     */
    public synchronized boolean initialize() {

        // Already initialized?
        if (this.initialized)
            return false;

        // Set "initialized" flag but unset if initialization fails
        boolean success = false;
        this.initialized = true;
        try {
            this.doInitialize();
            success = true;
        } finally {
            if (!success)
                this.initialized = false;
        }

        // Done
        return true;
    }

    private void doInitialize() {

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

        // Copy storage ID assignments into the corresponding SchemaItem's and PermazenSchemaItem's
        this.pclasses.forEach(pclass -> pclass.visitSchemaItems(item -> {
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

        // Populate PermazenClass and PermazenField maps keyed by storage ID
        this.pclasses.forEach(pclass -> this.pclassesByStorageId.put(pclass.storageId, pclass));
        this.pclasses.forEach(pclass -> pclass.fieldsByName.values().forEach(
          pfield -> pclass.fieldsByStorageId.put(pfield.storageId, pfield)));
        this.pclasses.forEach(pclass -> pclass.simpleFieldsByName.values().forEach(
          pfield -> pclass.simpleFieldsByStorageId.put(pfield.storageId, pfield)));

        // Update all PermazenSchemaItem's to point to core API SchemaItem instead of SchemaModel SchemaItem
        this.pclasses.forEach(pclass -> pclass.replaceSchemaItems(schema));

        // Find all fields that require default validation
        for (PermazenClass<?> pclass : this.pclasses) {
            for (PermazenField pfield : pclass.fieldsByName.values()) {
                if (pfield.requiresDefaultValidation)
                    this.fieldsRequiringDefaultValidation.add(pfield.storageId);
            }
        }

        // Populate this.indexesByStorageId
        this.pclasses.forEach(pclass -> {

            // Add simple field indexes
            pclass.simpleFieldsByName.values().forEach(pfield -> {
                if (pfield.indexed)
                    this.indexesByStorageId.put(pfield.storageId, pfield);
            });

            // Add composite indexes
            pclass.jcompositeIndexesByName.values().forEach(index -> this.indexesByStorageId.put(index.storageId, index));
        });

        // Populate this.typeFieldMap
        this.pclasses.forEach(pclass -> pclass.fieldsByName.values().forEach(pfield -> {
            this.typeFieldMap.put(new Tuple2<>(pclass.storageId, pfield.name), pfield);
            if (pfield instanceof PermazenComplexField) {
                final PermazenComplexField parentField = (PermazenComplexField)pfield;
                for (PermazenSimpleField subField : parentField.getSubFields())
                    this.typeFieldMap.put(new Tuple2<>(pclass.storageId, subField.getFullName()), subField);
            }
        }));

        // Populate pclass forwardCascadeMap and inverseCascadeMap
        this.pclasses.forEach(pclass -> pclass.simpleFieldsByName.values().forEach(pfield0 -> {

            // Filter for reference fields
            if (!(pfield0 instanceof PermazenReferenceField))
                return;
            final PermazenReferenceField pfield = (PermazenReferenceField)pfield0;

            // Do forward cascades
            for (String cascadeName : pfield.forwardCascades)
                pclass.forwardCascadeMap.computeIfAbsent(cascadeName, s -> new ArrayList<>()).add(pfield);

            // Do inverse cascades
            for (String cascadeName : pfield.inverseCascades) {
                for (PermazenClass<?> refPClass : this.getPermazenClasses(pfield.typeToken.getRawType())) {
                    refPClass.inverseCascadeMap
                      .computeIfAbsent(cascadeName, s -> new HashMap<>())
                      .computeIfAbsent(pfield.storageId, i -> new KeyRanges())
                      .add(ObjId.getKeyRange(pclass.storageId));
                }
            }
        }));

        // Scan for various method-level annotations
        this.pclasses.forEach(PermazenClass::scanAnnotations);

        // Detect whether we have any @OnCreate, @OnDelete, and/or @OnSchemaChange methods
        boolean anyOnCreateMethods = false;
        boolean anyOnDeleteMethods = false;
        boolean anyOnSchemaChangeMethods = false;
        boolean anyUpgradeConversions = false;
        for (PermazenClass<?> pclass : this.pclasses) {
            anyOnCreateMethods |= !pclass.onCreateMethods.isEmpty();
            anyOnDeleteMethods |= !pclass.onDeleteMethods.isEmpty();
            anyOnSchemaChangeMethods |= !pclass.onSchemaChangeMethods.isEmpty();
            anyUpgradeConversions |= !pclass.upgradeConversionFields.isEmpty();
        }
        this.hasOnCreateMethods = anyOnCreateMethods;
        this.hasOnDeleteMethods = anyOnDeleteMethods;
        this.hasOnSchemaChangeMethods = anyOnSchemaChangeMethods;
        this.hasUpgradeConversions = anyUpgradeConversions;

        // Eagerly load all generated Java classes so we "fail fast" if there are any loading errors
        this.untypedClassGenerator.generateClass();
        for (PermazenClass<?> pclass : this.pclasses)
            pclass.getClassGenerator().generateClass();
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
    public PermazenTransaction createTransaction() {
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
    public PermazenTransaction createTransaction(ValidationMode validationMode) {
        return this.createTransaction(validationMode, null);
    }

    /**
     * Create a new transaction with key/value transaction options.
     *
     * <p>
     * This does not invoke {@link PermazenTransaction#setCurrent PermazenTransaction.setCurrent()}: the caller is responsible
     * for doing that if necessary. However, this method does arrange for
     * {@link PermazenTransaction#setCurrent PermazenTransaction.setCurrent}{@code (null)} to be invoked as soon as the
     * returned transaction is committed (or rolled back), assuming {@link PermazenTransaction#getCurrent} returns the
     * {@link PermazenTransaction} returned here at that time.
     *
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @param kvoptions {@link KVDatabase}-specific transaction options, or null for none
     * @return the newly created transaction
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code validationMode} is null
     */
    public PermazenTransaction createTransaction(ValidationMode validationMode, Map<String, ?> kvoptions) {
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        this.initialize();
        return this.createTransaction(this.db.createTransaction(this.buildTransactionConfig(kvoptions)), validationMode);
    }

    /**
     * Create a new branched transaction.
     *
     * <p>
     * Convenience method; equivalent to:
     *  <blockquote><pre>
     *  {@link #createBranchedTransaction(ValidationMode, Map, Map) createBranchedTransaction}({@link ValidationMode#AUTOMATIC},
     *      null, null)
     *  </pre></blockquote>
     *
     * @return the newly created branched transaction
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws UnsupportedOperationException if the key/value database doesn't support {@link KVTransaction#readOnlySnapshot}
     */
    public PermazenTransaction createBranchedTransaction() {
        return this.createBranchedTransaction(ValidationMode.AUTOMATIC, null, null);
    }

    /**
     * Create a new branched transaction.
     *
     * <p>
     * Convenience method; equivalent to:
     *  <blockquote><pre>
     *  {@link #createBranchedTransaction(ValidationMode, Map, Map) createBranchedTransaction}(validationMode, null, null)
     *  </pre></blockquote>
     *
     * @return the newly created branched transaction
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws UnsupportedOperationException if the key/value database doesn't support {@link KVTransaction#readOnlySnapshot}
     */
    public PermazenTransaction createBranchedTransaction(ValidationMode validationMode) {
        return this.createBranchedTransaction(validationMode, null, null);
    }

    /**
     * Create a new branched transaction with the given key/value transaction options.
     *
     * <p>
     * This does not invoke {@link PermazenTransaction#setCurrent PermazenTransaction.setCurrent()}: the caller is responsible
     * for doing that if necessary. However, this method does arrange for
     * {@link PermazenTransaction#setCurrent PermazenTransaction.setCurrent}{@code (null)} to be invoked as soon as the
     * returned transaction is committed (or rolled back), assuming {@link PermazenTransaction#getCurrent} returns the
     * {@link PermazenTransaction} returned here at that time.
     *
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @param openOptions {@link KVDatabase}-specific transaction options for the branch's opening transaction, or null for none
     * @param syncOptions {@link KVDatabase}-specific transaction options for the branch's commit transaction, or null for none
     * @return the newly created branched transaction
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws UnsupportedOperationException if the key/value database doesn't support {@link KVTransaction#readOnlySnapshot}
     * @throws IllegalArgumentException if {@code validationMode} is null
     */
    public PermazenTransaction createBranchedTransaction(ValidationMode validationMode,
      Map<String, ?> openOptions, Map<String, ?> syncOptions) {
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        this.initialize();
        final BranchedKVTransaction kvt = new BranchedKVTransaction(this.db.getKVDatabase(), openOptions, syncOptions);
        Transaction tx = null;
        try {
            kvt.open();
            tx = this.db.createTransaction(kvt, this.buildTransactionConfig(openOptions));
        } finally {
            if (tx == null) {
                try {
                    kvt.rollback();
                } catch (KVTransactionException e) {
                    // ignore
                }
            }
        }
        return this.createTransaction(tx, validationMode);
    }

    /**
     * Create a new transaction using an already-opened {@link KVTransaction}.
     *
     * <p>
     * This does not invoke {@link PermazenTransaction#setCurrent PermazenTransaction.setCurrent()}: the caller is responsible
     * for doing that if necessary. However, this method does arrange for
     * {@link PermazenTransaction#setCurrent PermazenTransaction.setCurrent}{@code (null)} to be invoked as soon as the
     * returned transaction is committed (or rolled back), assuming {@link PermazenTransaction#getCurrent} returns the
     * {@link PermazenTransaction} returned here at that time.
     *
     * @param kvt already opened key/value store transaction
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @return the newly created transaction
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code kvt} or {@code validationMode} is null
     */
    public PermazenTransaction createTransaction(KVTransaction kvt, ValidationMode validationMode) {
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        this.initialize();
        return this.createTransaction(this.db.createTransaction(kvt, this.buildTransactionConfig(null)), validationMode);
    }

    private PermazenTransaction createTransaction(Transaction tx, ValidationMode validationMode) {
        assert tx != null;
        assert validationMode != null;
        synchronized (this) {
            assert this.initialized;
        }
        final PermazenTransaction ptx = new PermazenTransaction(this, tx, validationMode);
        tx.addCallback(new CleanupCurrentCallback(ptx));
        return ptx;
    }

    /**
     * Create a new, empty {@link DetachedPermazenTransaction} backed by a {@link MemoryKVStore}.
     *
     * <p>
     * The returned {@link DetachedPermazenTransaction} does not support {@link DetachedPermazenTransaction#commit commit()} or
     * {@link DetachedPermazenTransaction#rollback rollback()}, and can be used indefinitely.
     *
     * @param validationMode the {@link ValidationMode} to use for the detached transaction
     * @return initially empty detached transaction
     */
    public DetachedPermazenTransaction createDetachedTransaction(ValidationMode validationMode) {
        return this.createDetachedTransaction(new MemoryKVStore(), validationMode);
    }

    /**
     * Create a new {@link DetachedPermazenTransaction} based on the provided key/value store.
     *
     * <p>
     * The key/value store will be initialized if necessary (i.e., {@code kvstore} may be empty), otherwise it will be
     * validated against the schema information associated with this instance.
     *
     * <p>
     * The returned {@link DetachedPermazenTransaction} does not support {@link DetachedPermazenTransaction#commit commit()} or
     * {@link DetachedPermazenTransaction#rollback rollback()}, and can be used indefinitely.
     *
     * <p>
     * If {@code kvstore} is a {@link CloseableKVStore}, then it will be {@link CloseableKVStore#close close()}'d
     * if/when the returned {@link DetachedPermazenTransaction} is.
     *
     * @param kvstore key/value store, empty or having content compatible with this transaction's {@link Permazen}
     * @param validationMode the {@link ValidationMode} to use for the detached transaction
     * @return detached transaction based on {@code kvstore}
     * @throws io.permazen.core.SchemaMismatchException if {@code kvstore} contains incompatible or missing schema information
     * @throws io.permazen.core.InconsistentDatabaseException if inconsistent or invalid meta-data is detected in the database
     * @throws IllegalArgumentException if {@code kvstore} or {@code validationMode} is null
     */
    public DetachedPermazenTransaction createDetachedTransaction(KVStore kvstore, ValidationMode validationMode) {
        Preconditions.checkArgument(validationMode != null, "null validationMode");
        this.initialize();
        final DetachedTransaction dtx = this.db.createDetachedTransaction(kvstore, this.buildTransactionConfig(null));
        return new DetachedPermazenTransaction(this, dtx, validationMode);
    }

    /**
     * Build the {@link TransactionConfig} for a new core API transaction.
     *
     * @return core API transaction config
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    protected TransactionConfig buildTransactionConfig(Map<String, ?> kvoptions) {

        // Protect schema model while it's not yet locked down
        SchemaModel txModel = this.schemaModel;
        if (!txModel.isLockedDown(true))
            txModel = txModel.clone();

        // Build config
        return TransactionConfig.builder()
          .schemaModel(txModel)
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
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public SchemaModel getSchemaModel() {
        return this.getSchemaModel(true);
    }

    /**
     * Get the {@link SchemaModel} associated with this instance derived from the annotations on the scanned classes.
     *
     * @param withStorageIds true to include actual storage ID assignments
     * @return schema model used by this database
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public SchemaModel getSchemaModel(boolean withStorageIds) {
        if (withStorageIds)
            this.initialize();
        return withStorageIds ? this.schemaModel : this.origSchemaModel;
    }

// PermazenClass access

    /**
     * Get all {@link PermazenClass}'s associated with this instance, indexed by object type name.
     *
     * @return read-only mapping from object type name to {@link PermazenClass}
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public NavigableMap<String, PermazenClass<?>> getPermazenClassesByName() {
        this.initialize();
        return Collections.unmodifiableNavigableMap(this.pclassesByName);
    }

    /**
     * Get the {@link PermazenClass} associated with the given object type name.
     *
     * @param typeName object type name
     * @return {@link PermazenClass} instance
     * @throws UnknownTypeException if {@code typeName} is unknown
     * @throws IllegalArgumentException if {@code typeName} is null
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public PermazenClass<?> getPermazenClass(String typeName) {
        this.initialize();
        final PermazenClass<?> pclass = this.pclassesByName.get(typeName);
        if (pclass == null)
            throw new UnknownTypeException(typeName, null);
        return pclass;
    }

    /**
     * Get all {@link PermazenClass}'s associated with this instance, indexed by storage ID.
     *
     * @return read-only mapping from storage ID to {@link PermazenClass}
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public NavigableMap<Integer, PermazenClass<?>> getPermazenClassesByStorageId() {
        this.initialize();
        return Collections.unmodifiableNavigableMap(this.pclassesByStorageId);
    }

    /**
     * Get all {@link PermazenClass}'s associated with this instance, indexed by Java model type.
     *
     * @return read-only mapping from Java model type to {@link PermazenClass}
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public Map<Class<?>, PermazenClass<?>> getPermazenClassesByType() {
        this.initialize();
        return Collections.unmodifiableMap(this.pclassesByType);
    }

    /**
     * Get the {@link PermazenClass} modeled by the given type.
     *
     * @param type an annotated Java object model type
     * @param <T> Java model type
     * @return associated {@link PermazenClass}
     * @throws IllegalArgumentException if {@code type} is not equal to a known Java object model type
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    @SuppressWarnings("unchecked")
    public <T> PermazenClass<T> getPermazenClass(Class<T> type) {
        this.initialize();
        final PermazenClass<?> pclass = this.pclassesByType.get(type);
        if (pclass == null)
            throw new IllegalArgumentException("java model type is not recognized: " + type);
        return (PermazenClass<T>)pclass;
    }

    /**
     * Find the most specific {@link PermazenClass} for which the give type is a sub-type of the corresponding Java model type.
     *
     * @param type (sub)type of some Java object model type
     * @param <T> Java model type or subtype thereof
     * @return narrowest {@link PermazenClass} whose Java object model type is a supertype of {@code type}, or null if none found
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    @SuppressWarnings("unchecked")
    public <T> PermazenClass<? super T> findPermazenClass(Class<T> type) {
        this.initialize();
        for (Class<? super T> superType = type; superType != null; superType = superType.getSuperclass()) {
            final PermazenClass<?> pclass = this.pclassesByType.get(superType);
            if (pclass != null)
                return (PermazenClass<? super T>)pclass;
        }
        return null;
    }

    /**
     * Get the {@link PermazenClass} associated with the object ID.
     *
     * @param id object ID
     * @return {@link PermazenClass} instance
     * @throws UnknownTypeException if {@code id} has a type that is not defined in this instance's schema
     * @throws IllegalArgumentException if {@code id} is null
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public PermazenClass<?> getPermazenClass(ObjId id) {
        Preconditions.checkArgument(id != null, "null id");
        return this.getPermazenClass(id.getStorageId());
    }

    /**
     * Get the {@link PermazenClass} associated with the given storage ID.
     *
     * @param storageId object type storage ID
     * @return {@link PermazenClass} instance
     * @throws UnknownTypeException if {@code storageId} does not represent an object type
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    public PermazenClass<?> getPermazenClass(int storageId) {
        this.initialize();
        final PermazenClass<?> pclass = this.pclassesByStorageId.get(storageId);
        if (pclass == null)
            throw new UnknownTypeException("storage ID " + storageId, null);
        return pclass;
    }

    /**
     * Get all {@link PermazenClass}es which sub-type the given type.
     *
     * @param type type restriction, or null for no restrction
     * @param <T> Java model type
     * @return list of {@link PermazenClass}es whose type is {@code type} or a sub-type, ordered by storage ID
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     */
    @SuppressWarnings("unchecked")
    public <T> List<PermazenClass<? extends T>> getPermazenClasses(Class<T> type) {
        this.initialize();
        return this.pclasses.stream()
          .filter(pclass -> type == null || type.isAssignableFrom(pclass.type))
          .map(pclass -> (PermazenClass<? extends T>)pclass)
          .collect(Collectors.toList());
    }

    /**
     * Quick lookup for the {@link PermazenField} corresponding to the given object and field name.
     *
     * @param id object ID
     * @param fieldName field name; sub-fields of complex fields may be specified like {@code "mymap.key"}
     * @param <T> expected encoding
     * @throws UnknownTypeException if {@code id} has a type that is not defined in this instance's schema
     * @throws UnknownFieldException if {@code fieldName} does not correspond to any field in the object's type
     * @throws IllegalArgumentException if any parameter is null
     */
    @SuppressWarnings("unchecked")
    <T extends PermazenField> T getField(ObjId id, String fieldName, Class<T> type) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        Preconditions.checkArgument(type != null, "null type");
        final PermazenField pfield = this.typeFieldMap.get(new Tuple2<>(id.getStorageId(), fieldName));
        if (pfield == null) {
            this.getPermazenClass(id.getStorageId()).getField(fieldName, type);   // should always throw the appropriate exception
            assert false;
        }
        try {
            return type.cast(pfield);
        } catch (ClassCastException e) {
            throw new UnknownFieldException(fieldName, String.format(
              "%s is not a %s field", pfield, type.getSimpleName().replaceAll("^J(.*)Field$", "").toLowerCase()));
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
        final ArrayList<KeyRange> list = new ArrayList<>(this.pclasses.size());
        boolean invert = false;
        if (type == UntypedPermazenObject.class) {
            type = null;
            invert = true;
        }
        this.getPermazenClasses(type).stream()
          .map(pclass -> ObjId.getKeyRange(pclass.storageId))
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
     * Roughly equivalent to: {@code this.parseReferencePath(this.getPermazenClasses(startType), path)}.
     *
     * @param startType starting Java type for the path
     * @param path reference path in string form
     * @return parsed reference path
     * @throws IllegalArgumentException if no model types are instances of {@code startType}
     * @throws IllegalArgumentException if {@code path} is invalid
     * @throws IllegalArgumentException if either parameter is null
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     * @see ReferencePath
     */
    public ReferencePath parseReferencePath(Class<?> startType, String path) {
        Preconditions.checkArgument(startType != null, "null startType");
        final HashSet<PermazenClass<?>> startTypes = new HashSet<>(this.getPermazenClasses(startType));
        if (startTypes.isEmpty())
            throw new IllegalArgumentException(String.format("no model type is an instance of %s", startType));
        if (startType.isAssignableFrom(UntypedPermazenObject.class))
            startTypes.add(null);
        return this.parseReferencePath(startTypes, path);
    }

    /**
     * Parse a {@link ReferencePath} starting from a set of model object types.
     *
     * @param startTypes starting model types for the path, with null meaning {@link UntypedPermazenObject}
     * @param path reference path in string form
     * @return parsed reference path
     * @throws IllegalArgumentException if {@code startTypes} is empty or contains null
     * @throws IllegalArgumentException if {@code path} is invalid
     * @throws IllegalArgumentException if either parameter is null
     * @throws InvalidSchemaException if this instance is not yet {@link #initialize initialized} and schema registration fails
     * @see ReferencePath
     */
    public ReferencePath parseReferencePath(Set<PermazenClass<?>> startTypes, String path) {
        return this.referencePathCache.get(startTypes, path);
    }

// Misc utility

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

    // Get class generator for "untyped" PermazenObject's
    ClassGenerator<UntypedPermazenObject> getUntypedClassGenerator() {
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

        private final PermazenTransaction ptx;

        CleanupCurrentCallback(PermazenTransaction ptx) {
            assert ptx != null;
            this.ptx = ptx;
        }

        @Override
        public void afterCompletion(boolean committed) {
            final PermazenTransaction current;
            try {
                current = PermazenTransaction.getCurrent();
            } catch (IllegalStateException e) {
                return;
            }
            if (current == this.ptx)
                PermazenTransaction.setCurrent(null);
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode() ^ this.ptx.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final CleanupCurrentCallback that = (CleanupCurrentCallback)obj;
            return this.ptx.equals(that.ptx);
        }
    }
}
