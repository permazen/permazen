
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.DatabaseException;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java model layer for accessing a JSimpleDB {@link Database} using normal Java objects.
 *
 * <p>
 * This class provides a natural, Java-centric view of a JSimpleDB {@link Database}. This is done via two main
 * enhancements to the core functionality provided by {@link Database}:
 * <ul>
 *  <li>Java annotations are used to define all {@link Database} objects and fields, and to automatically
 *      register various listeners.</li>
 *  <li>{@link Database} objects and fields are represented using actual Java model objects, where the Java model classes
 *      are automatically generated subclasses of suitably annotated user-supplied Java bean superclasses. That is,
 *      all {@link Database} object references (i.e., {@link ObjId}s) are replaced by references to these Java model objects,
 *      and all {@link Database} fields are accessible through the annotated Java bean getter and setter methods.</li>
 * </ul>
 * </p>
 *
 * <p>
 * This class guarantees that for each {@link ObjId} there is only ever one Java model object in existence,
 * independent of any transaction. Therefore, the same Java objects can be used in any transaction. The Java model
 * objects are stateless; all field state is contained within whichever transaction is associated with the current thread.
 * </p>
 */
public class JLayer {

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final TreeMap<Integer, JClass<?>> jclasses = new TreeMap<>();
    final HashMap<TypeToken<?>, JClass<?>> jclassesByType = new HashMap<>();
    final HashMap<Integer, JField> jfields = new HashMap<>();
    final ReferenceConverter referenceConverter = new ReferenceConverter(this);
    final Database db;
    final int version;

    private SchemaModel schemaModel;
    private NameIndex nameIndex;

    private final LoadingCache<ObjId, JObject> objectMap = CacheBuilder.newBuilder().weakValues().build(
      new CacheLoader<ObjId, JObject>() {
        @Override
        public JObject load(ObjId id) throws Exception {
            return (JObject)JLayer.this.getJClass(id.getStorageId()).getConstructor().newInstance(id);
        }
    });

    /**
     * Constructor.
     *
     * @param database database to use
     * @param version schema version associated with {@code classes}
     * @param classes classes annotated with {@link JSimpleClass} and/or {@link JFieldType} annotations
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code version} is not greater than zero
     * @throws IllegalArgumentException if {@code classes} contains a null, primitive, or interface class
     * @throws InvalidSchemaException if the schema implied by {@code classes} is invalid
     */
    public JLayer(Database database, int version, Iterable<Class<?>> classes) {

        // Initialize
        if (database == null)
            throw new IllegalArgumentException("null database");
        if (version <= 0)
            throw new IllegalArgumentException("invalid schema version: " + version);
        if (classes == null)
            throw new IllegalArgumentException("null classes");
        this.db = database;
        this.version = version;

        // Scan for annotations
        final HashSet<Class<?>> classesSeen = new HashSet<>();
        for (Class<?> type : classes) {

            // Ignore duplicates
            this.log.debug("checking " + type + " for JSimpleDB annotations");
            if (!classesSeen.add(type)) {
                this.log.debug("already seen " + type + ", ignoring");
                continue;
            }

            // Look for custom field types
            final JFieldType fieldTypeAnnotation = type.getAnnotation(JFieldType.class);
            if (fieldTypeAnnotation != null) {

                // Get type name
                final String name = fieldTypeAnnotation.name().length() != 0 ? fieldTypeAnnotation.name() : type.getName();
                this.log.debug("found @" + JFieldType.class.getSimpleName()
                  + " annotation on " + type + " defining field type `" + name + "'");

                // Instantiate class
                final Object obj;
                try {
                    obj = type.newInstance();
                } catch (Exception e) {
                    throw new DatabaseException("can't instantiate " + type, e);
                }
                if (!(obj instanceof FieldType)) {
                    throw new DatabaseException("invalid @" + JFieldType.class.getSimpleName()
                      + " annotation on " + type + ": not a subclass of " + FieldType.class);
                }

                // Register field type
                final FieldType<?> fieldType = (FieldType<?>)obj;
                this.db.getFieldTypeRegistry().add(fieldType);
                this.log.debug("registered new field type `" + fieldType.getName() + "' using " + type);
            }

            // Look for Java model classes
            final JSimpleClass jclassAnnotation = type.getAnnotation(JSimpleClass.class);
            if (jclassAnnotation != null) {

                // Sanity check type
                if (type == null || type.isPrimitive() || type.isInterface() || type.isArray()) {
                    throw new IllegalArgumentException("illegal type " + type + " for @"
                      + JSimpleClass.class.getSimpleName() + " annotation: not a normal class");
                }
            /* ???
                for (TypeToken<?> superType : TypeToken.of(type).getTypes()) {
                    if (superType.getRawType().getTypeParameters().length > 0) {
                        throw new IllegalArgumentException("illegal class " + type.getName() + " for @"
                          + JSimpleClass.class.getSimpleName() + " annotation: class has generic supertype " + superType);
                    }
                }
            */

                // Create JClass
                final String name = jclassAnnotation.name().length() != 0 ? jclassAnnotation.name() : type.getSimpleName();
                this.log.debug("found @" + JSimpleClass.class.getSimpleName() + " annotation on " + type
                  + " defining object type `" + name + "'");
                JClass<?> jclass;
                try {
                    jclass = this.createJClass(name, jclassAnnotation.storageId(), TypeToken.of(type));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid @" + JSimpleClass.class.getSimpleName()
                      + " annotation on " + type + ": " + e, e);
                }

                // Add jclass
                this.addJClass(jclass);
                this.log.debug("added model class `" + jclass.name + "' for object type " + jclass.storageId);
            }
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
        return new JTransaction(this, tx, validationMode);
    }

// Schema

    /**
     * Get the {@link SchemaModel} associated with this instance, derived from the annotations on the scanned classes.
     */
    public SchemaModel getSchemaModel() {
        if (this.schemaModel == null) {
            final SchemaModel model = new SchemaModel();
            for (JClass<?> jclass : this.jclasses.values())
                model.addSchemaObject(jclass.toSchemaItem());
            this.schemaModel = model;
            this.log.debug("generated schema:\n{}", this.schemaModel);
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
     * Get the {@link JClass} associated with the given storage ID.
     *
     * @param storageId object type storage ID
     * @return {@link JClass} instance
     * @throws IllegalArgumentException if {@code storageId} does not represent an object type
     */
    public JClass<?> getJClass(int storageId) {
        final JClass<?> jclass = this.jclasses.get(storageId);
        if (jclass == null)
            throw new IllegalArgumentException("no jlayer class associated with storage ID " + storageId);
        return jclass;
    }

// Object Cache

    /**
     * Get the Java object used to represent the given object ID.
     *
     * <p>
     * This method guarantees that for any particular {@code id}, the same Java instance will always be returned.
     * </p>
     *
     * @param id object ID
     * @return Java model object
     */
    public JObject getJObject(ObjId id) {
        Throwable cause;
        try {
            return this.objectMap.get(id);
        } catch (ExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        } catch (UncheckedExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        }
        if (cause instanceof Error)
            throw (Error)cause;
        throw new DatabaseException("can't instantiate object for ID " + id, cause);
    }

    /**
     * Get the Java object used to represent the given object ID, cast to the given type.
     *
     * @param id object ID
     * @return Java model object
     * @see #getJObject(ObjId)
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
        return new ReferencePath(this, startType, path, null);
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
            throw new IllegalArgumentException("no jlayer field associated with storage ID " + storageId);
        return type != null ? type.cast(jfield) : (T)jfield;
    }

    boolean isReferenceField(int storageId) {
        return this.jfields.get(storageId) instanceof JReferenceField;
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

