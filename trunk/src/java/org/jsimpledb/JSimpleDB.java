
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
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
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.schema.SchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstraction layer allowing access to a {@link Database} using normal Java objects.
 *
 * <p>
 * This class provides a natural, Java-centric view on top of the core {@link Database} class. This is done via two main
 * enhancements to the functionality provided by {@link Database}:
 * <ul>
 *  <li>{@linkplain org.jsimpledb.annotation Java annotations} are used to define all {@link Database} objects and fields,
 *      to automatically register various listeners, to access indexed fields, etc.</li>
 *  <li>{@link Database} objects and fields are represented using actual Java model objects, where the Java model classes
 *      are automatically generated subclasses of the user-supplied Java bean model classes. That is, in the view that
 *      a {@link JSimpleDB} provides, all {@link Database} object references (which are all {@link ObjId} objects)
 *      are replaced by normal Java model objects of the appropriate type, and all {@link Database} fields are accessible
 *      through the corresponding model objects' Java bean getter and setter methods, instead of directly through
 *      a {@link Transaction} object. All Java model classes implement the {@link JObject} interface.</li>
 * </ul>
 * </p>
 *
 * <p>
 * In addition, a {@link JSimpleDB} provides automatic incremental JSR 303 validation.
 * </p>
 *
 * <p>
 * Not counting "snapshot" objects (see below), this class guarantees that for each {@link ObjId}
 * it will only create a single, globally unique, {@link JObject} Java model object.
 * Therefore, the same Java model objects can be used in and out of any transaction, and can serve as
 * unique database object identifiers (if you have not overridden {@link #equals equals()} in your model clases).
 * Except for their associated {@link ObjId}s, the generated Java model objects are stateless; all database field state
 * is contained within whichever transaction is {@linkplain JTransaction#getCurrent associated with the current thread}.
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
 * @see org.jsimpledb.annotation
 */
public class JSimpleDB {

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final TreeMap<Integer, JClass<?>> jclasses = new TreeMap<>();
    final HashMap<TypeToken<?>, JClass<?>> jclassesByType = new HashMap<>();
    final HashMap<Integer, JField> jfields = new HashMap<>();
    final ReferencePathCache referencePathCache = new ReferencePathCache(this);
    final Database db;
    final int version;
    final JObjectCache jobjectCache = new JObjectCache(this) {
        @Override
        protected JObject instantiate(JClass<?> jclass, ObjId id) throws Exception {
            return (JObject)jclass.getConstructor().newInstance(id);
        }
    };

    volatile int lastVersion;

    private SchemaModel schemaModel;
    private NameIndex nameIndex;

    /**
     * Create an instance using an initially empty, in-memory {@link SimpleKVDatabase}.
     * Generates a database schema by introspecting the {@code classes}; schema version number {@code 1} is assumed.
     *
     * <p>
     * This constructor can also be used just to validate the annotations on the given classes.
     * </p>
     *
     * <p>
     * All {@link JSimpleClass &#64;JSimpleClass}-annotated super-classes of any {@link JSimpleClass &#64;JSimpleClass}-annotated
     * class in {@code classes} will be included even if not explicitly specified.
     * </p>
     *
     * @param classes classes annotated with {@link JSimpleClass &#64;JSimpleClass} annotations
     * @throws IllegalArgumentException if {@code classes} is null
     * @throws IllegalArgumentException if {@code classes} contains a null class or a class with invalid annotation(s)
     * @throws InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    public JSimpleDB(Iterable<? extends Class<?>> classes) {
        this(new Database(new SimpleKVDatabase()), 1, classes);
    }

    /**
     * Primary constructor. Generates a database schema by introspecting the provided classes.
     *
     * <p>
     * All {@link JSimpleClass &#64;JSimpleClass}-annotated super-classes of any {@link JSimpleClass &#64;JSimpleClass}-annotated
     * class in {@code classes} will be included even if not explicitly specified.
     * </p>
     *
     * @param database core database to use
     * @param version schema version number of the schema derived from {@code classes},
     *  or zero to use the highest version already recorded in the database
     * @param classes classes annotated with {@link JSimpleClass &#64;JSimpleClass} annotations; non-annotated classes are ignored
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code version} is not greater than zero
     * @throws IllegalArgumentException if {@code classes} contains a null class or a class with invalid annotation(s)
     * @throws InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    public JSimpleDB(Database database, int version, Iterable<? extends Class<?>> classes) {

        // Initialize
        if (database == null)
            throw new IllegalArgumentException("null database");
        if (version < 0)
            throw new IllegalArgumentException("invalid schema version: " + version);
        if (classes == null)
            throw new IllegalArgumentException("null classes");
        this.db = database;
        this.version = version;

        // Inventory classes; automatically add all @JSimpleClass-annotated superclasses of @JSimpleClass-annotated classes
        final HashSet<Class<?>> jsimpleClasses = new HashSet<>();
        for (Class<?> type : classes) {

            // Sanity check
            if (type == null)
                throw new IllegalArgumentException("null class found in classes");

            // Check for @JSimpleClass
            if (type.isAnnotationPresent(JSimpleClass.class)) {

                // Sanity check type
                if (type.isPrimitive() || type.isInterface() || type.isArray()) {
                    throw new IllegalArgumentException("illegal type " + type + " for @"
                      + JSimpleClass.class.getSimpleName() + " annotation: not a normal class");
                }

                // Add type and all @JSimpleClass-annotated superclasses
                jsimpleClasses.add(type);
                for (Class<?> supertype = type.getSuperclass(); type != null; type = type.getSuperclass()) {
                    if (supertype.getAnnotation(JSimpleClass.class) != null)
                        jsimpleClasses.add(supertype);
                }
            }
        }

        // Add Java model classes
        for (Class<?> type : jsimpleClasses) {

            // Create JClass
            final JSimpleClass jclassAnnotation = type.getAnnotation(JSimpleClass.class);
            final String name = jclassAnnotation.name().length() != 0 ? jclassAnnotation.name() : type.getSimpleName();
            if (this.log.isTraceEnabled()) {
                this.log.trace("found @" + JSimpleClass.class.getSimpleName() + " annotation on " + type
                  + " defining object type `" + name + "'");
            }
            JClass<?> jclass;
            try {
                jclass = this.createJClass(name, jclassAnnotation.storageId(), Util.getWildcardedType(type));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid @" + JSimpleClass.class.getSimpleName()
                  + " annotation on " + type + ": " + e, e);
            }

            // Add jclass
            this.addJClass(jclass);
            this.log.debug("added Java model class `" + jclass.name + "' with storage ID " + jclass.storageId);
        }

        // Create fields
        for (JClass<?> jclass : this.jclasses.values())
            jclass.createFields();

        // Scan for other annotations
        for (JClass<?> jclass : this.jclasses.values())
            jclass.scanAnnotations();

        // Index fields by storage ID
        for (JClass<?> jclass : this.jclasses.values()) {
            for (JField jfield : jclass.jfields.values()) {
                this.jfields.put(jfield.storageId, jfield);
                if (jfield instanceof JComplexField) {
                    final JComplexField complexField = (JComplexField)jfield;
                    for (JSimpleField subField : complexField.getSubFields())
                        this.jfields.put(subField.storageId, subField);
                }
            }
        }

        // Validate schema
        this.db.validateSchema(this.getSchemaModel());
    }

    // This method exists solely to bind the generic type parameters
    private <T> JClass<T> createJClass(String name, int storageId, TypeToken<T> typeToken) {
        return new JClass<T>(this, name, storageId, typeToken);
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
     * Get the schema version that this instance will use.
     *
     * @return the schema version that this instance will use when opening transactions via
     *  {@link Database#createTransaction Database.createTransaction()}
     */
    public int getVersion() {
        return this.version;
    }

    /**
     * Get the schema version that this instance used for the most recently created transaction.
     *
     * <p>
     * If no transations have been created yet, this returns zero. Otherwise, it returns the schema version
     * used by the most recently created transaction. If the {@code version} passed to the constructor was zero,
     * this method can be used to get the highest schema version recorded in the database;
     * otherwise, this will necessarily be the same non-zero value.
     * </p>
     *
     * @return the schema version that this instance used in the most recent transaction
     */
    public int getLastVersion() {
        return this.lastVersion;
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
        final Transaction tx = this.db.createTransaction(this.getSchemaModel(), this.version, allowNewSchema);
        this.lastVersion = tx.getSchema().getSchemaVersions().lastKey();
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
                final SchemaObject schemaObject = jclass.toSchemaItem();
                model.getSchemaObjects().put(schemaObject.getStorageId(), schemaObject);
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
    public SortedMap<Integer, JClass<?>> getJClassesByStorageId() {
        return Collections.unmodifiableSortedMap(this.jclasses);
    }

    /**
     * Get all {@link JClass}'s associated with this instance, indexed by Java model type.
     *
     * @return read-only mapping from Java model type to {@link JClass}
     */
    public Map<TypeToken<?>, JClass<?>> getJClassesByType() {
        return Collections.unmodifiableMap(this.jclassesByType);
    }

    /**
     * Get the {@link JClass} modeled by the given type.
     *
     * @param typeToken an annotated Java object model type
     * @return associated {@link JClass}
     * @throws IllegalArgumentException if {@code type} is not a known Java object model type
     */
    @SuppressWarnings("unchecked")
    public <T> JClass<T> getJClass(TypeToken<T> typeToken) {
        final JClass<?> jclass = this.jclassesByType.get(typeToken);
        if (jclass == null)
            throw new IllegalArgumentException("java model type is not recognized: " + typeToken);
        return (JClass<T>)jclass;
    }

    /**
     * Get the {@link JClass} modeled by the given type.
     *
     * @param type an annotated Java object model type
     * @return associated {@link JClass}
     * @throws IllegalArgumentException if {@code type} is not a known Java object model type
     */
    public <T> JClass<T> getJClass(Class<T> type) {
        return this.getJClass(TypeToken.of(type));
    }

    /**
     * Get the {@link JClass} associated with the given storage ID.
     *
     * @param storageId object type storage ID
     * @return {@link JClass} instance
     * @throws IllegalArgumentException if {@code storageId} does not represent an object type
     */
    public JClass<?> getJClass(int storageId) {
        final JClass<?> jclass = this.jclasses.get(storageId);
        if (jclass == null)
            throw new IllegalArgumentException("no JSimpleDB class associated with storage ID " + storageId);
        return jclass;
    }

    /**
     * Get all {@link JClass}es which sub-type the given type.
     *
     * @param typeToken type restriction, or null for no restrction
     * @return list of {@link JClass}es whose type is {@code typeToken} or a sub-type (if not null), ordered by storage ID
     */
    @SuppressWarnings("unchecked")
    public <T> List<JClass<? extends T>> getJClasses(TypeToken<T> typeToken) {
        final ArrayList<JClass<? extends T>> list = new ArrayList<>();
        for (JClass<?> jclass : this.jclasses.values()) {
            if (typeToken == null || typeToken.isAssignableFrom(jclass.typeToken))
                list.add((JClass<? extends T>)jclass);
        }
        return list;
    }

// Object Cache

    /**
     * Get the Java model object with the given object ID. The {@link JTransaction} {@linkplain JObject#getTransaction associated}
     * with the returned {@link JObject} will whatever {@link JTransaction} is
     * {@linkplain JTransaction#getCurrent associated with the current thread}.
     *
     * <p>
     * This method guarantees that for any particular {@code id}, the same Java instance will always be returned.
     * </p>
     *
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     * @see JTransaction#getJObject JTransaction.getJObject()
     * @throws UnknownTypeException if no Java model class corresponding to {@code id} exists in the schema
     *  associated with this instance
     */
    public JObject getJObject(ObjId id) {
        return this.jobjectCache.getJObject(id);
    }

    /**
     * Get the Java object used to represent the given object ID, cast to the given type.
     *
     * @param id object ID
     * @return Java model object
     * @see #getJObject(ObjId)
     * @throws UnknownTypeException if no Java model class corresponding to {@code id} exists in the schema
     *  associated with this instance
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
    public ReferencePath parseReferencePath(TypeToken<?> startType, String path) {
        return this.parseReferencePath(startType, path, null);
    }

    ReferencePath parseReferencePath(TypeToken<?> startType, String path, Boolean lastIsSubField) {
        return this.referencePathCache.get(startType, path, lastIsSubField);
    }

// Internal Stuff

    /**
     * Get the {@link JField} associated with the given storage ID.
     *
     * @param storageId field storage ID
     * @param type required type, or null
     * @return {@link JFiedl} instance
     * @throws IllegalArgumentException if {@code storageId} does not represent a field
     */
    @SuppressWarnings("unchecked")
    <T extends JField> T getJField(int storageId, Class<T> type) {
        final JField jfield = this.jfields.get(storageId);
        if (jfield == null)
            throw new IllegalArgumentException("no JSimpleDB field associated with storage ID " + storageId);
        return type != null ? type.cast(jfield) : (T)jfield;
    }

    // Add new JClass, checking for storage ID conflicts
    void addJClass(JClass<?> jclass) {

        // Check for storage ID conflict
        final JClass<?> other = this.jclasses.get(jclass.storageId);
        if (other != null) {
            throw new IllegalArgumentException("illegal duplicate use of storage ID "
              + jclass.storageId + " for both " + other + " and " + jclass);
        }
        this.jclasses.put(jclass.storageId, jclass);
        this.jclassesByType.put(jclass.typeToken, jclass);      // this can never conflict, no need to check
    }
}

