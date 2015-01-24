
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.collect.Iterables;

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

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.TypeNotInSchemaVersionException;
import org.jsimpledb.core.UnknownFieldException;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.schema.SchemaObjectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JSimpleDB database.
 *
 * <p>
 * JSimpleDB consists of two main layers. The lower level "core" API provides a rigourous database abstraction on top of
 * a key/value store. It supports simple fields of any atomic Java type, as well as list, set, and map complex fields,
 * tightly controlled schema versioning, simple and composite indexes, and lifecycle and change notifications.
 * The core API is provided via the {@link Database} class.
 * </p>
 *
 * <p>
 * This {@link JSimpleDB} class represents the upper layer. It sits on top of the core {@link Database} class and provides
 * a fully type-safe Java view of a {@link Database}, where all access is through user-supplied Java model classes.
 * In addition, a {@link JSimpleDB} provides automatic incremental JSR 303 validation. Database types and fields,
 * as well as listener methods, are all declared using various {@linkplain org.jsimpledb.annotation Java annotations}.
 * </p>
 *
 * <p>
 * User-provided Java model classes are typically abstract and declare fields as abstract Java bean methods. In any case,
 * {@link JSimpleDB} generates concrete subclasses at runtime; these runtime classes will implement the {@link JObject} interface.
 * The corresponding Java model objects are stateless; all database field state is derived from whichever transaction is
 * {@linkplain JTransaction#getCurrent associated with the current thread}.
 * </p>
 *
 * <p>
 * Not counting "snapshot" objects (see below), this class guarantees that for each {@link ObjId} there will only ever be
 * a single, globally unique, {@link JObject} Java model object. Therefore, the same Java model objects can be used in and
 * out of any transaction.
 * </p>
 *
 * <p>
 * "Snapshot" objects are in-memory copies of regular database objects that may be imported/exported to/from transactions.
 * Snapshot {@link JObject}s are distinct from regular database {@link JObject}s; their state is contained in an in-memory
 * {@link SnapshotJTransaction}. See {@link JObject#copyOut JObject.copyOut} and {@link JObject#copyIn JObject.copyIn}.
 * </p>
 *
 * @see JObject
 * @see JTransaction
 * @see JSimpleDBFactory
 * @see org.jsimpledb.annotation
 */
public class JSimpleDB {

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final TreeMap<Integer, JClass<?>> jclasses = new TreeMap<>();
    final HashMap<Class<?>, JClass<?>> jclassesByType = new HashMap<>();
    final TreeMap<Integer, JFieldInfo> jfieldInfos = new TreeMap<>();
    final TreeMap<Integer, JCompositeIndexInfo> jcompositeIndexInfos = new TreeMap<>();
    final ReferencePathCache referencePathCache = new ReferencePathCache(this);
    final ClassGenerator<UntypedJObject> untypedClassGenerator;
    final ArrayList<ClassGenerator<?>> classGenerators;
    final ClassLoader loader;
    final Database db;
    final int configuredVersion;
    final StorageIdGenerator storageIdGenerator;
    final JObjectCache jobjectCache = new JObjectCache(this) {
        @Override
        protected JObject instantiate(ClassGenerator<?> classGenerator, ObjId id) throws Exception {
            return (JObject)classGenerator.getConstructor().newInstance(id);
        }
    };
    final boolean hasOnCreateMethods;
    final boolean hasOnDeleteMethods;
    final boolean hasOnVersionChangeMethods;
    final boolean anyJClassRequiresValidation;

    volatile int actualVersion;

    private SchemaModel schemaModel;
    private NameIndex nameIndex;

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
     * @throws InvalidSchemaException if the schema implied by {@code classes} is invalid
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
     * @throws InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    JSimpleDB(Database database, int version, StorageIdGenerator storageIdGenerator, Iterable<? extends Class<?>> classes) {

        // Initialize
        if (database == null)
            throw new IllegalArgumentException("null database");
        if (version < 0)
            throw new IllegalArgumentException("invalid schema version: " + version);
        if (classes == null)
            throw new IllegalArgumentException("null classes");
        this.db = database;
        this.configuredVersion = version;
        this.storageIdGenerator = storageIdGenerator;

        // Inventory classes; automatically add all @JSimpleClass-annotated superclasses of @JSimpleClass-annotated classes
        final HashSet<Class<?>> jsimpleClasses = new HashSet<>();
        for (Class<?> type : classes) {

            // Sanity check
            if (type == null)
                throw new IllegalArgumentException("null class found in classes");

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

        // Set up class loader
        final ClassLoader parentLoader = jsimpleClasses.isEmpty() ?
          jsimpleClasses.iterator().next().getClassLoader() : Thread.currentThread().getContextClassLoader();
        this.loader = new ClassLoader(parentLoader) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {

                // Normalize name
                final String slashName = name.replace('.', '/');

                // Find matching genearator
                byte[] bytes = null;
                for (ClassGenerator<?> generator : JSimpleDB.this.classGenerators) {
                    if (slashName.equals(generator.getClassName())) {
                        bytes = generator.generateBytecode();
                        break;
                    }
                    if (slashName.equals(generator.getSnapshotClassName())) {
                        bytes = generator.generateSnapshotBytecode();
                        break;
                    }
                }
                if (bytes == null)
                    return super.findClass(name);

                // Define class
                JSimpleDB.this.log.debug("generating class " + name);
                final Class<?> c = this.defineClass(null, bytes, 0, bytes.length);
                JSimpleDB.this.log.debug("done generating class " + name);
                return c;
            }
        };

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
        boolean anyValidation = false;
        for (JClass<?> jclass : this.jclasses.values()) {
            if ((anyValidation |= jclass.requiresValidation))
                break;
        }
        this.anyJClassRequiresValidation = anyValidation;

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
     * If {@code jobj} is an instance of a JSimpleDB-generated subclass of a user-supplied Java model class,
     * this returns the original Java model class. Otherwise, it returns {@code obj}'s type.
     *
     * @param jobj database instance
     * @return lowest ancestor class of {@code jobj}'s class that is not a JSimpleDB-generated subclass
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public static Class<?> getModelClass(JObject jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        for (Class<?> type = jobj.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().indexOf(ClassGenerator.CLASSNAME_SUFFIX) == -1)
                return type;
        }
        return null;
    }

// Transactions

    /**
     * Create a new transaction. This does not invoke {@link JTransaction#setCurrent JTransaction.setCurrent()}: the
     * caller is responsible for doing that if necessary.
     *
     * @param allowNewSchema whether creating a new schema version is allowed
     * @param validationMode the {@link ValidationMode} to use for the new transaction
     * @throws org.jsimpledb.InvalidSchemaException if {@code schemaModel} does not match what's recorded in the
     *  database for the schema version provided to the constructor
     * @throws org.jsimpledb.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is false
     * @throws org.jsimpledb.InvalidSchemaException if the schema version provided to the constructor
     *  is not recorded in the database and {@code allowNewSchema} is true, but {@code schemaModel} is incompatible
     *  with one or more previous schemas alread recorded in the database (i.e., the same storage ID is used
     *  incompatibly between schema versions)
     * @throws org.jsimpledb.InconsistentDatabaseException if inconsistent or invalid schema information is detected
     *  in the database
     * @throws IllegalArgumentException if {@code validationMode} is null
     */
    public JTransaction createTransaction(boolean allowNewSchema, ValidationMode validationMode) {
        if (validationMode == null)
            throw new IllegalArgumentException("null validationMode");
        final Transaction tx = this.db.createTransaction(this.getSchemaModel(), this.configuredVersion, allowNewSchema);
        this.actualVersion = tx.getSchema().getSchemaVersions().lastKey();
        return new JTransaction(this, tx, validationMode);
    }

// Schema

    /**
     * Get the {@link SchemaModel} associated with this instance, derived from the annotations on the scanned classes.
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
     * Get the {@link JClass} associated with the object ID.
     *
     * @param id object ID
     * @return {@link JClass} instance
     * @throws TypeNotInSchemaVersionException if {@code id} has a type that does not exist in this instance's schema version
     * @throws IllegalArgumentException if {@code id} is null
     */
    public JClass<?> getJClass(ObjId id) {
        if (id == null)
            throw new IllegalArgumentException("null id");
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

// Object Cache

    /**
     * Get the Java model object that is has the given ID. This returns the object associated with normal
     * (i.e., non-snapshot) {@link JTransaction}s. To get the object associated with a snapshot transaction,
     * invoke {@link JTransaction#getJObject(ObjId) JTransaction.getJObject()} on the snapshot transaction.
     *
     * <p>
     * This method guarantees that for any particular {@code id}, the same Java instance will always be returned.
     * Note: while for any {@link ObjId} there is only one globally unique {@link JObject} per {@link JSimpleDB}
     * shared by all {@link JTransaction}s (the object returned by this method), each {@link SnapshotJTransaction}
     * maintains its own distinct pool of unique "snapshot" {@link JObject}s.
     * </p>
     *
     * <p>
     * A non-null object is always returned, but the corresponding object may not exist in a given transaction.
     * In that case, attempts to access its fields will throw {@link org.jsimpledb.core.DeletedObjectException}.
     * </p>
     *
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     * @see JTransaction#getJObject JTransaction.getJObject()
     */
    public JObject getJObject(ObjId id) {
        return this.jobjectCache.getJObject(id);
    }

    /**
     * Get the Java object used to represent the given object ID, cast to the given type.
     * This method just invoke {@link #getJObject(ObjId)} and then casts the result.
     *
     * @param id object ID
     * @param type expected type
     * @return Java model object
     * @see #getJObject(ObjId)
     * @throws ClassCastException if the Java model object does not have type {@code type}
     * @throws IllegalArgumentException if {@code type} is null
     */
    public <T> T getJObject(ObjId id, Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        return type.cast(this.getJObject(id));
    }

// Reference Paths

    /**
     * Parse a {@link ReferencePath} in {@link String} form.
     *
     * @param startType starting Java type for the path
     * @param path dot-separated path of zero or more reference fields, followed by a target field
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

// Misc utility

    /**
     * Utility method to get all of the objects directly referenced by a given object via any field.
     *
     * <p>
     * Note: the returned {@link Iterable} may contain duplicates; these can be eliminated using an
     * {@link org.jsimpledb.core.ObjIdSet} if necessary.
     * </p>
     *
     * @param jobj starting object
     * @return all objects directly referenced by {@code jobj}
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public Iterable<JObject> getReferencedObjects(final JObject jobj) {

        // Sanity check
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
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
        if (type == null)
            throw new IllegalArgumentException("null type");
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
}

