
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.DeleteAction;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.UnknownFieldException;
import org.jsimpledb.schema.SchemaCompositeIndex;
import org.jsimpledb.schema.SchemaField;
import org.jsimpledb.schema.SchemaObjectType;
import org.jsimpledb.util.AnnotationScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Information about a Java class that is used to represent a specific JSimpleDB object type.
 *
 * @param <T> the Java class
 */
public class JClass<T> extends JSchemaObject {

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final JSimpleDB jdb;
    final TypeToken<T> typeToken;
    final ClassGenerator<T> classGenerator;
    final TreeMap<Integer, JField> jfields = new TreeMap<>();
    final TreeMap<String, JField> jfieldsByName = new TreeMap<>();
    final TreeMap<Integer, JCompositeIndex> jcompositeIndexes = new TreeMap<>();
    final TreeMap<String, JCompositeIndex> jcompositeIndexesByName = new TreeMap<>();

    Set<OnCreateScanner<T>.MethodInfo> onCreateMethods;
    Set<OnDeleteScanner<T>.MethodInfo> onDeleteMethods;
    Set<OnChangeScanner<T>.MethodInfo> onChangeMethods;
    Set<ValidateScanner<T>.MethodInfo> validateMethods;
    ArrayList<OnVersionChangeScanner<T>.MethodInfo> onVersionChangeMethods;

    int[] subtypeStorageIds;

    /**
     * Constructor.
     *
     * @param jdb the associated {@link JSimpleDB}
     * @param name the name of the object type
     * @param storageId object type storage ID
     * @param type object type Java model class
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    JClass(JSimpleDB jdb, String name, int storageId, TypeToken<T> typeToken) {
        super(name, storageId, "object type `" + name + "' (" + typeToken + ")");
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.jdb = jdb;
        this.typeToken = typeToken;
        this.classGenerator = new ClassGenerator<T>(this);
    }

    // Get class generator
    ClassGenerator<T> getClassGenerator() {
        return this.classGenerator;
    }

// Public API

    /**
     * Get the {@link JSimpleDB} with which this instance is associated.
     */
    public JSimpleDB getJSimpleDB() {
        return this.jdb;
    }

    /**
     * Get the Java model object type associated with this instance.
     */
    public TypeToken<T> getTypeToken() {
        return this.typeToken;
    }

    /**
     * Get all {@link JField}'s associated with this instance, indexed by storage ID.
     *
     * @return read-only mapping from storage ID to {@link JClass}
     */
    public SortedMap<Integer, JField> getJFieldsByStorageId() {
        return Collections.unmodifiableSortedMap(this.jfields);
    }

    /**
     * Get all {@link JField}'s associated with this instance, indexed by name.
     *
     * @return read-only mapping from storage ID to {@link JClass}
     */
    public SortedMap<String, JField> getJFieldsByName() {
        return Collections.unmodifiableSortedMap(this.jfieldsByName);
    }

    /**
     * Get the {@link JField} in this instance associated with the specified storage ID, cast to the given type.
     *
     * @param storageId field storage ID
     * @param type required type
     * @return {@link JField} in this instance corresponding to {@code storageId}
     * @throws UnknownFieldException if {@code storageId} does not correspond to any field in this instance
     * @throws UnknownFieldException if the field is not an instance of of {@code type}
     */
    public <T extends JField> T getJField(int storageId, Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        final JField jfield = this.jfields.get(storageId);
        if (jfield == null)
            throw new UnknownFieldException(storageId, "object type `" + this.name + "' has no field with storage ID " + storageId);
        try {
            return type.cast(jfield);
        } catch (ClassCastException e) {
            throw new UnknownFieldException(storageId, "object type `" + this.name + "' has no field with storage ID "
              + storageId + " of type " + type.getName() + " (found " + jfield + " instead)");
        }
    }

// Internal methods

    void createFields(JSimpleDB jdb) {

        // Auto-generate properties?
        final boolean autogenFields = this.typeToken.getRawType().getAnnotation(JSimpleClass.class).autogenFields();

        // Scan for Simple fields
        final JFieldScanner<T> simpleFieldScanner = new JFieldScanner<T>(this, autogenFields);
        for (JFieldScanner<T>.MethodInfo info : simpleFieldScanner.findAnnotatedMethods()) {

            // Get info
            final org.jsimpledb.annotation.JField annotation = info.getAnnotation();
            final Method getter = info.getMethod();
            final String description = simpleFieldScanner.getAnnotationDescription() + " annotation on method " + getter;
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            final TypeToken<?> fieldTypeToken = this.typeToken.resolveType(getter.getGenericReturnType());
            if (this.log.isTraceEnabled())
                this.log.trace("found " + description);

            // Get storage ID
            int storageId = annotation.storageId();
            if (storageId == 0)
                storageId = jdb.getStorageIdGenerator(annotation, getter).generateFieldStorageId(getter, fieldName);

            // Handle Counter fields
            if (fieldTypeToken.equals(TypeToken.of(Counter.class))) {

                // Sanity check annotation
                if (annotation.type().length() != 0)
                    throw new IllegalArgumentException("invalid " + description + ": counter fields must not specify a type");
                if (annotation.indexed())
                    throw new IllegalArgumentException("invalid " + description + ": counter fields cannot be indexed");

                // Create counter field
                final JCounterField jfield = new JCounterField(fieldName, storageId,
                  "counter field `" + fieldName + "' of object type `" + this.name + "'", getter);
                jfield.parent = this;

                // Add field
                this.addField(jfield);
                continue;
            }

            // Find corresponding setter method
            final Method setter;
            try {
                setter = Util.findSetterMethod(this.typeToken, getter);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid " + description + ": " + e.getMessage());
            }

            // Create simple field
            final JSimpleField jfield = this.createSimpleField(description, fieldTypeToken,
              fieldName, annotation.type(), storageId, annotation.indexed(), annotation.onDelete(), annotation.cascadeDelete(),
              getter, setter, "field `" + fieldName + "' of object type `" + this.name + "'");
            jfield.parent = this;

            // Add field
            this.addField(jfield);
        }

        // Scan for Set fields
        final JSetFieldScanner<T> setFieldScanner = new JSetFieldScanner<T>(this, autogenFields);
        for (JSetFieldScanner<T>.MethodInfo info : setFieldScanner.findAnnotatedMethods()) {

            // Get info
            final org.jsimpledb.annotation.JSetField annotation = info.getAnnotation();
            final org.jsimpledb.annotation.JField elementAnnotation = annotation.element();
            final Method getter = info.getMethod();
            final String description = setFieldScanner.getAnnotationDescription() + " annotation on method " + getter;
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found " + description);

            // Get storage ID's
            int storageId = annotation.storageId();
            if (storageId == 0)
                storageId = jdb.getStorageIdGenerator(annotation, getter).generateFieldStorageId(getter, fieldName);
            int elementStorageId = elementAnnotation.storageId();
            if (elementStorageId == 0) {
                elementStorageId = jdb.getStorageIdGenerator(elementAnnotation, getter)
                  .generateSetElementStorageId(getter, fieldName);
            }

            // Get element type (the raw return type has already been validated by the annotation scanner)
            final TypeToken<?> elementType = this.typeToken.resolveType(this.getParameterType(description, getter, 0));

            // Create element sub-field
            final JSimpleField elementField = this.createSimpleField("element() property of " + description,
              elementType, SetField.ELEMENT_FIELD_NAME, elementAnnotation.type(), elementStorageId,
              elementAnnotation.indexed(), elementAnnotation.onDelete(), elementAnnotation.cascadeDelete(), null, null,
              "element field of set field `" + fieldName + "' in object type `" + this.name + "'");

            // Create set field
            final JSetField jfield = new JSetField(fieldName, storageId, elementField,
              "set field `" + fieldName + "' in object type `" + this.name + "'", getter);
            elementField.parent = jfield;

            // Add field
            this.addField(jfield);
        }

        // Scan for List fields
        final JListFieldScanner<T> listFieldScanner = new JListFieldScanner<T>(this, autogenFields);
        for (JListFieldScanner<T>.MethodInfo info : listFieldScanner.findAnnotatedMethods()) {

            // Get info
            final org.jsimpledb.annotation.JListField annotation = info.getAnnotation();
            final org.jsimpledb.annotation.JField elementAnnotation = annotation.element();
            final Method getter = info.getMethod();
            final String description = listFieldScanner.getAnnotationDescription() + " annotation on method " + getter;
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found " + description);

            // Get storage ID's
            int storageId = annotation.storageId();
            if (storageId == 0)
                storageId = jdb.getStorageIdGenerator(annotation, getter).generateFieldStorageId(getter, fieldName);
            int elementStorageId = elementAnnotation.storageId();
            if (elementStorageId == 0) {
                elementStorageId = jdb.getStorageIdGenerator(elementAnnotation, getter)
                  .generateListElementStorageId(getter, fieldName);
            }

            // Get element type (the raw return type has already been validated by the annotation scanner)
            final TypeToken<?> elementType = this.typeToken.resolveType(this.getParameterType(description, getter, 0));

            // Create element sub-field
            final JSimpleField elementField = this.createSimpleField("element() property of " + description,
              elementType, ListField.ELEMENT_FIELD_NAME, elementAnnotation.type(), elementStorageId,
              elementAnnotation.indexed(), elementAnnotation.onDelete(), elementAnnotation.cascadeDelete(), null, null,
              "element field of list field `" + fieldName + "' in object type `" + this.name + "'");

            // Create list field
            final JListField jfield = new JListField(fieldName, storageId, elementField,
              "list field `" + fieldName + "' in object type `" + this.name + "'", getter);
            elementField.parent = jfield;

            // Add field
            this.addField(jfield);
        }

        // Scan for Map fields
        final JMapFieldScanner<T> mapFieldScanner = new JMapFieldScanner<T>(this, autogenFields);
        for (JMapFieldScanner<T>.MethodInfo info : mapFieldScanner.findAnnotatedMethods()) {

            // Get info
            final org.jsimpledb.annotation.JMapField annotation = info.getAnnotation();
            final org.jsimpledb.annotation.JField keyAnnotation = annotation.key();
            final org.jsimpledb.annotation.JField valueAnnotation = annotation.value();
            final Method getter = info.getMethod();
            final String description = mapFieldScanner.getAnnotationDescription() + " annotation on method " + getter;
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found " + description);

            // Get storage ID's
            int storageId = annotation.storageId();
            if (storageId == 0)
                storageId = jdb.getStorageIdGenerator(annotation, getter).generateFieldStorageId(getter, fieldName);
            int keyStorageId = keyAnnotation.storageId();
            if (keyStorageId == 0)
                keyStorageId = jdb.getStorageIdGenerator(keyAnnotation, getter).generateMapKeyStorageId(getter, fieldName);
            int valueStorageId = valueAnnotation.storageId();
            if (valueStorageId == 0)
                valueStorageId = jdb.getStorageIdGenerator(valueAnnotation, getter).generateMapValueStorageId(getter, fieldName);

            // Get key and value types (the raw return type has already been validated by the annotation scanner)
            final TypeToken<?> keyType = this.typeToken.resolveType(this.getParameterType(description, getter, 0));
            final TypeToken<?> valueType = this.typeToken.resolveType(this.getParameterType(description, getter, 1));

            // Create key and value sub-fields
            final JSimpleField keyField = this.createSimpleField("key() property of " + description,
              keyType, MapField.KEY_FIELD_NAME, keyAnnotation.type(), keyStorageId, keyAnnotation.indexed(),
              keyAnnotation.onDelete(), keyAnnotation.cascadeDelete(), null, null,
              "key field of map field `" + fieldName + "' in object type `" + this.name + "'");
            final JSimpleField valueField = this.createSimpleField("value() property of " + description,
              valueType, MapField.VALUE_FIELD_NAME, valueAnnotation.type(), valueStorageId, valueAnnotation.indexed(),
              valueAnnotation.onDelete(), valueAnnotation.cascadeDelete(), null, null,
              "value field of map field `" + fieldName + "' in object type `" + this.name + "'");

            // Create map field
            final JMapField jfield = new JMapField(fieldName, storageId, keyField, valueField,
              "map field `" + fieldName + "' in object type `" + this.name + "'", getter);
            keyField.parent = jfield;
            valueField.parent = jfield;

            // Add field
            this.addField(jfield);
        }
    }

    void addCompositeIndex(JSimpleDB jdb, org.jsimpledb.annotation.JCompositeIndex annotation) {

        // Get info
        final String indexName = annotation.name();

        // Resolve field names
        final String[] fieldNames = annotation.fields();
        final JSimpleField[] indexFields = new JSimpleField[fieldNames.length];
        final int[] indexFieldStorageIds = new int[fieldNames.length];
        final HashSet<String> seenFieldNames = new HashSet<String>();
        for (int i = 0; i < fieldNames.length; i++) {
            final String fieldName = fieldNames[i];
            if (!seenFieldNames.add(fieldName))
                throw this.invalidIndex(annotation, "field `" + fieldName + "' appears more than once");
            final JField jfield = this.jfieldsByName.get(fieldName);
            if (!(jfield instanceof JSimpleField)) {
                throw this.invalidIndex(annotation, "field `" + fieldName + "' "
                  + (jfield != null ? "is not a simple field" : "not found"));
            }
            indexFields[i] = (JSimpleField)jfield;
            indexFieldStorageIds[i] = jfield.storageId;
        }

        // Get storage ID
        int storageId = annotation.storageId();
        if (storageId == 0) {
            final Class<?> type = this.typeToken.getRawType();
            storageId = jdb.getStorageIdGenerator(annotation, type).generateCompositeIndexStorageId(
              type, indexName, indexFieldStorageIds);
        }

        // Create and add index
        final JCompositeIndex index = new JCompositeIndex(indexName, storageId, indexFields);
        if (this.jcompositeIndexes.put(index.storageId, index) != null)
            throw this.invalidIndex(annotation, "duplicate use of storage ID " + index.storageId);
        if (this.jcompositeIndexesByName.put(index.name, index) != null)
            throw this.invalidIndex(annotation, "duplicate use of composite index name `" + index.name + "'");
    }

    void scanAnnotations() {
        this.onCreateMethods = new OnCreateScanner<T>(this).findAnnotatedMethods();
        this.onDeleteMethods = new OnDeleteScanner<T>(this).findAnnotatedMethods();
        this.onChangeMethods = new OnChangeScanner<T>(this).findAnnotatedMethods();
        this.validateMethods = new ValidateScanner<T>(this).findAnnotatedMethods();
        final OnVersionChangeScanner<T> onVersionChangeScanner = new OnVersionChangeScanner<T>(this);
        this.onVersionChangeMethods = new ArrayList<>(onVersionChangeScanner.findAnnotatedMethods());
        Collections.sort(this.onVersionChangeMethods, onVersionChangeScanner);
    }

    @Override
    SchemaObjectType toSchemaItem(JSimpleDB jdb) {
        final SchemaObjectType schemaObjectType = new SchemaObjectType();
        this.initialize(jdb, schemaObjectType);
        for (JField field : this.jfields.values()) {
            final SchemaField schemaField = field.toSchemaItem(jdb);
            schemaObjectType.getSchemaFields().put(schemaField.getStorageId(), schemaField);
        }
        for (JCompositeIndex index : this.jcompositeIndexes.values()) {
            final SchemaCompositeIndex schemaIndex = index.toSchemaItem(jdb);
            schemaObjectType.getSchemaCompositeIndexes().put(index.getStorageId(), schemaIndex);
        }
        return schemaObjectType;
    }

    private IllegalArgumentException invalidIndex(org.jsimpledb.annotation.JCompositeIndex annotation, String message) {
        return new IllegalArgumentException("invalid @JCompositeIndex annotation for index `" + annotation.name()
          + "' on " + this.typeToken.getRawType() + ": " + message);
    }

    // Add new JField (and sub-fields, if any), checking for name and storage ID conflicts
    private void addField(JField jfield) {

        // Check for storage ID conflict
        JField other = this.jfields.get(jfield.storageId);
        if (other != null) {
            throw new IllegalArgumentException("illegal duplicate use of storage ID "
              + jfield.storageId + " for both " + other + " and " + jfield);
        }
        this.jfields.put(jfield.storageId, jfield);

        // Check for name conflict
        if ((other = this.jfieldsByName.get(jfield.name)) != null)
            throw new IllegalArgumentException("illegal duplicate use of field name `" + jfield.name + "' in " + this);
        this.jfieldsByName.put(jfield.name, jfield);

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("added " + jfield + " to object type `" + this.name + "'");
    }

    // Get field name, deriving it from the getter property name if necessary
    private String getFieldName(String fieldName, AnnotationScanner<T, ?>.MethodInfo info, String description) {
        if (fieldName.length() > 0)
            return fieldName;
        try {
            return info.getMethodPropertyName();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid " + description + ": can't infer field name: " + e, e);
        }
    }

    // Get the n'th generic type parameter
    private Type getParameterType(String description, Method method, int index) {
        try {
            return Util.getTypeParameter(method.getGenericReturnType(), index);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid " + description + ": invalid method return type: " + e.getMessage(), e);
        }
    }

    // Create a simple field, either regular object field or sub-field of complex field
    @SuppressWarnings("unchecked")
    private JSimpleField createSimpleField(String description, TypeToken<?> fieldTypeToken, String fieldName,
      String typeName, int storageId, boolean indexed, DeleteAction onDelete, boolean cascadeDelete, Method getter, Method setter,
      String fieldDescription) {

        // Complex sub-field?
        final boolean isSubField = getter == null;

        // Empty type name same as null
        if (typeName.length() == 0)
            typeName = null;

        // See if field type encompasses one or more JClass types and is therefore a reference type
        boolean isReferenceType = false;
        for (JClass<?> jclass : this.jdb.jclasses.values()) {
            if (fieldTypeToken.isAssignableFrom(jclass.typeToken)) {
                isReferenceType = true;
                break;
            }
        }

        // See if field type is a simple type, known either by explicitly-given name or type
        FieldType<?> nonReferenceType = null;
        if (typeName != null) {

            // Field type is explicitly specified by name
            if ((nonReferenceType = this.jdb.db.getFieldTypeRegistry().getFieldType(typeName)) == null)
                throw new IllegalArgumentException("invalid " + description + ": unknown simple field type `" + typeName + "'");

            // Verify field type matches what we expect
            final TypeToken<?> expectedType = isSubField ? nonReferenceType.getTypeToken().wrap() : nonReferenceType.getTypeToken();
            if (!expectedType.equals(fieldTypeToken)) {
                throw new IllegalArgumentException("invalid " + description + ": field type `" + typeName
                  + "' supports values of type " + nonReferenceType.getTypeToken() + " but " + fieldTypeToken
                  + " is required (according to the getter method's return type)");
            }
        } else {

            // Try to find a field type supporting getter method return type
            final List<? extends FieldType<?>> fieldTypes = this.jdb.db.getFieldTypeRegistry().getFieldTypes(fieldTypeToken);
            switch (fieldTypes.size()) {
            case 0:
                nonReferenceType = null;
                break;
            case 1:
                nonReferenceType = fieldTypes.get(0);
                break;
            default:
                if (!isReferenceType) {
                    throw new IllegalArgumentException("invalid " + description + ": an explicit type() must be specified"
                      + " because type " + fieldTypeToken + " is supported by multiple registered simple field types: "
                      + fieldTypes);
                }
                break;
            }
        }

        // Detect enum types
        final Class<? extends Enum<?>> enumType = Enum.class.isAssignableFrom(fieldTypeToken.getRawType()) ?
          (Class<? extends Enum<?>>)fieldTypeToken.getRawType().asSubclass(Enum.class) : null;

        // If field type neither refers to a JClass type, nor is a registered field type, nor is an enum type, fail
        if (!isReferenceType && nonReferenceType == null && enumType == null) {
            throw new IllegalArgumentException("invalid " + description + ": an explicit type() must be specified"
              + " because no known type supports values of type " + fieldTypeToken);
        }

        // Handle ambiguity between reference vs. non-reference
        if (isReferenceType && nonReferenceType != null) {

            // If an explicit type name was provided, assume they want the specified non-reference type
            if (typeName != null)
                isReferenceType = false;
            else {
                throw new IllegalArgumentException("invalid " + description + ": an explicit type() must be specified"
                  + " because type " + fieldTypeToken + " is ambiguous, being both a @" + JSimpleClass.class.getSimpleName()
                  + " reference type and a simple Java type supported by type `" + nonReferenceType + "'");
            }
        }

        // Create simple, enum, or reference field
        try {
            return
              isReferenceType ?
                new JReferenceField(fieldName, storageId, fieldDescription,
                  fieldTypeToken, onDelete, cascadeDelete, getter, setter) :
              enumType != null ?
                new JEnumField(fieldName, storageId, enumType, indexed, fieldDescription, getter, setter) :
                new JSimpleField(fieldName, storageId, fieldTypeToken,
                 nonReferenceType.getName(), indexed, fieldDescription, getter, setter);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid " + description + ": " + e, e);
        }
    }
}

