
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.FollowPath;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.core.EnumValueEncoding;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.SetField;
import io.permazen.core.UnknownFieldException;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.encoding.SimpleEncodingRegistry;
import io.permazen.kv.KeyRanges;
import io.permazen.schema.SchemaCompositeIndex;
import io.permazen.schema.SchemaField;
import io.permazen.schema.SchemaObjectType;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Information about a Java class that is used to represent a specific Permazen object type.
 *
 * @param <T> the Java class
 */
public class JClass<T> extends JSchemaObject {

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final Class<T> type;
    final ClassGenerator<T> classGenerator;
    final TreeMap<Integer, JField> jfields = new TreeMap<>();                           // does not include sub-fields
    final TreeMap<String, JField> jfieldsByName = new TreeMap<>();                      // does not include sub-fields
    final TreeMap<Integer, JCompositeIndex> jcompositeIndexes = new TreeMap<>();
    final TreeMap<String, JCompositeIndex> jcompositeIndexesByName = new TreeMap<>();
    final ArrayList<JSimpleField> uniqueConstraintFields = new ArrayList<>();
    final ArrayList<JCompositeIndex> uniqueConstraintCompositeIndexes = new ArrayList<>();
    final ArrayList<JField> upgradeConversionFields = new ArrayList<>();                // contains only simple and counter fields
    final HashMap<String, List<JReferenceField>> forwardCascadeMap = new HashMap<>();
    final HashMap<String, Map<Integer, KeyRanges>> inverseCascadeMap = new HashMap<>();

    Set<FollowPathScanner<T>.MethodInfo> followPathMethods;
    Set<OnCreateScanner<T>.MethodInfo> onCreateMethods;
    Set<OnDeleteScanner<T>.MethodInfo> onDeleteMethods;
    Set<OnChangeScanner<T>.MethodInfo> onChangeMethods;
    Set<OnValidateScanner<T>.MethodInfo> onValidateMethods;
    ArrayList<OnVersionChangeScanner<T>.MethodInfo> onVersionChangeMethods;

    boolean requiresDefaultValidation;
    boolean hasSnapshotCreateOrChangeMethods;
    AnnotatedElement elementRequiringJSR303Validation;
    int[] simpleFieldStorageIds;

    /**
     * Constructor.
     *
     * @param jdb the associated {@link Permazen}
     * @param name the name of the object type
     * @param storageId object type storage ID
     * @param type object type Java model class
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    JClass(Permazen jdb, String name, int storageId, Class<T> type) {
        super(jdb, name, storageId, String.format("object type \"%s\" (%s)", name, type));
        Preconditions.checkArgument(name != null, "null name");
        if (UntypedJObject.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException(String.format(
              "invalid model type %s: model types may not subclass %s", type.getName(), UntypedJObject.class.getName()));
        }
        this.type = type;
        this.classGenerator = new ClassGenerator<>(this);
    }

    // Get class generator
    ClassGenerator<T> getClassGenerator() {
        return this.classGenerator;
    }

// Public API

    /**
     * Get the Java model object type associated with this instance.
     *
     * @return associated Java type
     */
    public Class<T> getType() {
        return this.type;
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
     * Get all {@link JCompositeIndex}'s defined on this {@link JClass}.
     *
     * @return read-only mapping from name to {@link JCompositeIndex}
     */
    public SortedMap<String, JCompositeIndex> getJCompositeIndexesByName() {
        return Collections.unmodifiableSortedMap(this.jcompositeIndexesByName);
    }

    /**
     * Get the {@link JField} in this instance associated with the specified storage ID, cast to the given type.
     *
     * @param storageId field storage ID
     * @param type required type
     * @param <T> expected encoding
     * @return {@link JField} in this instance corresponding to {@code storageId}
     * @throws UnknownFieldException if {@code storageId} does not correspond to any field in this instance
     * @throws UnknownFieldException if the field is not an instance of of {@code type}
     */
    public <T extends JField> T getJField(int storageId, Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final JField jfield = this.jfields.get(storageId);
        if (jfield == null) {
            throw new UnknownFieldException(storageId,
              String.format("object type \"%s\" has no field with storage ID %d", this.name, storageId));
        }
        try {
            return type.cast(jfield);
        } catch (ClassCastException e) {
            throw new UnknownFieldException(storageId,
              String.format("object type \"%s\" has no field with storage ID %d of type %s (found %s instead)",
                this.name, storageId, type.getName(), jfield));
        }
    }

// Internal methods

    void createFields(Permazen jdb) {

        // Auto-generate properties?
        final PermazenType permazenType = Util.getAnnotation(this.type, PermazenType.class);

        // Scan for Simple and Counter fields
        final JFieldScanner<T> simpleFieldScanner = new JFieldScanner<>(this, permazenType);
        for (JFieldScanner<T>.MethodInfo info : simpleFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.JField annotation = info.getAnnotation();
            final Method getter = Util.findJFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", simpleFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            final TypeToken<?> encodingToken = TypeToken.of(this.type).resolveType(getter.getGenericReturnType());
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

            // Get storage ID
            int storageId = annotation.storageId();
            if (storageId == 0)
                storageId = jdb.getStorageIdGenerator(annotation, getter).generateFieldStorageId(getter, fieldName);

            // Handle Counter fields
            if (encodingToken.equals(TypeToken.of(Counter.class))) {

                // Sanity check annotation
                if (annotation.encoding().length() != 0) {
                    throw new IllegalArgumentException(String.format(
                      "invalid %s: counter fields must not specify an encoding", description));
                }
                if (annotation.indexed()) {
                    throw new IllegalArgumentException(String.format(
                      "invalid %s: counter fields cannot be indexed", description));
                }

                // Create counter field
                final JCounterField jfield = new JCounterField(this.jdb, fieldName, storageId, annotation,
                  String.format("counter field \"%s\" of object type \"%s\"", fieldName, this.name), getter);

                // Remember upgrade conversion fields
                if (annotation.upgradeConversion().isConvertsValues())
                    this.upgradeConversionFields.add(jfield);

                // Add field
                this.addField(jfield);
                continue;
            }

            // Find corresponding setter method
            final Method setter;
            try {
                setter = Util.findJFieldSetterMethod(this.type, getter);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("invalid %s: %s", description, e.getMessage()));
            }

            // Create simple field
            final JSimpleField jfield = this.createSimpleField(description, encodingToken, fieldName, storageId,
              annotation, getter, setter, String.format("field \"%s\" of object type \"%s\"", fieldName, this.name));

            // Add field
            this.addField(jfield);

            // Remember unique constraint fields
            if (jfield.unique)
                this.uniqueConstraintFields.add(jfield);

            // Remember upgrade conversion fields
            if (annotation.upgradeConversion().isConvertsValues())
                this.upgradeConversionFields.add(jfield);
        }

        // Scan for Set fields
        final JSetFieldScanner<T> setFieldScanner = new JSetFieldScanner<>(this, permazenType);
        for (JSetFieldScanner<T>.MethodInfo info : setFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.JSetField annotation = info.getAnnotation();
            final io.permazen.annotation.JField elementAnnotation = annotation.element();
            final Method getter = Util.findJFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", setFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

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
            final TypeToken<?> elementType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 0));

            // Create element sub-field
            final JSimpleField elementField = this.createSimpleField("element() property of " + description, elementType,
              SetField.ELEMENT_FIELD_NAME, elementStorageId, elementAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"",
              "set", fieldName, SetField.ELEMENT_FIELD_NAME, this.name));

            // Create set field
            final JSetField jfield = new JSetField(this.jdb, fieldName, storageId, annotation, elementField,
              String.format("%s field \"%s\" in object type \"%s\"", "set", fieldName, this.name), getter);
            elementField.parent = jfield;

            // Add field
            this.addField(jfield);
        }

        // Scan for List fields
        final JListFieldScanner<T> listFieldScanner = new JListFieldScanner<>(this, permazenType);
        for (JListFieldScanner<T>.MethodInfo info : listFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.JListField annotation = info.getAnnotation();
            final io.permazen.annotation.JField elementAnnotation = annotation.element();
            final Method getter = Util.findJFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", listFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

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
            final TypeToken<?> elementType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 0));

            // Create element sub-field
            final JSimpleField elementField = this.createSimpleField("element() property of " + description, elementType,
              ListField.ELEMENT_FIELD_NAME, elementStorageId, elementAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"",
              "list", fieldName, ListField.ELEMENT_FIELD_NAME, this.name));

            // Create list field
            final JListField jfield = new JListField(this.jdb, fieldName, storageId, annotation, elementField,
              String.format("%s field \"%s\" in object type \"%s\"", "list", fieldName, this.name), getter);
            elementField.parent = jfield;

            // Add field
            this.addField(jfield);
        }

        // Scan for Map fields
        final JMapFieldScanner<T> mapFieldScanner = new JMapFieldScanner<>(this, permazenType);
        for (JMapFieldScanner<T>.MethodInfo info : mapFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.JMapField annotation = info.getAnnotation();
            final io.permazen.annotation.JField keyAnnotation = annotation.key();
            final io.permazen.annotation.JField valueAnnotation = annotation.value();
            final Method getter = Util.findJFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", mapFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

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
            final TypeToken<?> keyType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 0));
            final TypeToken<?> valueType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 1));

            // Create key and value sub-fields
            final JSimpleField keyField = this.createSimpleField("key() property of " + description, keyType,
              MapField.KEY_FIELD_NAME, keyStorageId, keyAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"", "map",
              fieldName, MapField.KEY_FIELD_NAME, this.name));
            final JSimpleField valueField = this.createSimpleField("value() property of " + description, valueType,
              MapField.VALUE_FIELD_NAME, valueStorageId, valueAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"", "map",
              fieldName, MapField.VALUE_FIELD_NAME, this.name));

            // Create map field
            final JMapField jfield = new JMapField(this.jdb, fieldName, storageId, annotation, keyField, valueField,
              String.format("%s field \"%s\" in object type \"%s\"", "map", fieldName, this.name), getter);
            keyField.parent = jfield;
            valueField.parent = jfield;

            // Add field
            this.addField(jfield);
        }

        // Verify that the generated class will not have any remaining abstract methods
        final Map<MethodKey, Method> abstractMethods = Util.findAbstractMethods(this.type);
        for (JField jfield : this.jfields.values()) {
            abstractMethods.remove(new MethodKey(jfield.getter));
            if (jfield instanceof JSimpleField)
                abstractMethods.remove(new MethodKey(((JSimpleField)jfield).setter));
        }
        for (Method method : JObject.class.getDeclaredMethods())
            abstractMethods.remove(new MethodKey(method));
        for (Iterator<Method> i = abstractMethods.values().iterator(); i.hasNext(); ) {
            if (Util.getAnnotation(i.next(), FollowPath.class) != null)
                i.remove();
        }
        if (!abstractMethods.isEmpty()) {
            throw new IllegalArgumentException(String.format(
              "the @%s-annotated type %s is invalid because %d abstract method(s) remain unimplemented: %s",
              PermazenType.class.getSimpleName(), this.type.getName(), abstractMethods.size(),
              abstractMethods.values().toString().replaceAll("^\\[(.*)\\]$", "$1")));
        }

        // Calculate which fields require default validation
        this.jfields.values()
          .forEach(JField::calculateRequiresDefaultValidation);

        // Gather simple field storage ID's
        this.simpleFieldStorageIds = Ints.toArray(this.jfields.values().stream()
          .filter(jfield -> jfield instanceof JSimpleField)
          .map(jfield -> jfield.storageId)
          .collect(Collectors.toList()));
    }

    void addCompositeIndex(Permazen jdb, Class<?> declaringType, io.permazen.annotation.JCompositeIndex annotation) {

        // Get info
        final String indexName = annotation.name();

        // Resolve field names
        final String[] fieldNames = annotation.fields();
        final JSimpleField[] indexFields = new JSimpleField[fieldNames.length];
        final int[] indexFieldStorageIds = new int[fieldNames.length];
        final HashSet<String> seenFieldNames = new HashSet<>();
        for (int i = 0; i < fieldNames.length; i++) {
            final String fieldName = fieldNames[i];
            if (!seenFieldNames.add(fieldName))
                throw this.invalidIndex(annotation, "field \"%s\" appears more than once", fieldName);
            final JField jfield = this.jfieldsByName.get(fieldName);
            if (jfield == null)
                throw this.invalidIndex(annotation, "field \"%s\" not found", fieldName);
            else if (!(jfield instanceof JSimpleField))
                throw this.invalidIndex(annotation, "field \"%s\" is not a simple field", fieldName);
            indexFields[i] = (JSimpleField)jfield;
            indexFieldStorageIds[i] = jfield.storageId;
        }

        // Get storage ID
        int storageId = annotation.storageId();
        if (storageId == 0) {
            storageId = jdb.getStorageIdGenerator(annotation, type)
              .generateCompositeIndexStorageId(this.type, indexName, indexFieldStorageIds);
        }

        // Create and add index
        final JCompositeIndex index = new JCompositeIndex(this.jdb, indexName, storageId, declaringType, annotation, indexFields);
        if (this.jcompositeIndexes.put(index.storageId, index) != null)
            throw this.invalidIndex(annotation, "duplicate use of storage ID %d", index.storageId);
        if (this.jcompositeIndexesByName.put(index.name, index) != null)
            throw this.invalidIndex(annotation, "duplicate use of composite index name \"%s\"", index.name);

        // Remember unique constraint composite indexes and trigger validation when any indexed field changes
        if (index.unique) {
            this.uniqueConstraintCompositeIndexes.add(index);
            for (JSimpleField jfield : index.jfields)
                jfield.requiresDefaultValidation = true;
        }
    }

    void scanAnnotations() {
        this.followPathMethods = new FollowPathScanner<>(this).findAnnotatedMethods();
        this.onCreateMethods = new OnCreateScanner<>(this).findAnnotatedMethods();
        this.onDeleteMethods = new OnDeleteScanner<>(this).findAnnotatedMethods();
        this.onChangeMethods = new OnChangeScanner<>(this).findAnnotatedMethods();
        this.onValidateMethods = new OnValidateScanner<>(this).findAnnotatedMethods();
        final OnVersionChangeScanner<T> onVersionChangeScanner = new OnVersionChangeScanner<>(this);
        this.onVersionChangeMethods = new ArrayList<>(onVersionChangeScanner.findAnnotatedMethods());
        Collections.sort(this.onVersionChangeMethods, onVersionChangeScanner);

        // Determine if we need to enable notifications when copying into snapshot transactions
        for (OnCreateScanner<T>.MethodInfo methodInfo : this.onCreateMethods) {
            if (methodInfo.getAnnotation().snapshotTransactions()) {
                this.hasSnapshotCreateOrChangeMethods = true;
                break;
            }
        }
        for (OnChangeScanner<T>.MethodInfo methodInfo : this.onChangeMethods) {
            if (methodInfo.getAnnotation().snapshotTransactions()) {
                this.hasSnapshotCreateOrChangeMethods = true;
                break;
            }
        }
    }

    void calculateValidationRequirement() {

        // Check for use of JSR 303 annotations
        this.elementRequiringJSR303Validation = Util.hasValidation(this.type);

        // Check for JSR 303 or @OnValidate annotations in default group
        if (Util.requiresDefaultValidation(this.type)) {
            this.requiresDefaultValidation = true;
            return;
        }

        // Check for any simple index uniqueness constraints
        if (!this.uniqueConstraintFields.isEmpty()) {
            this.requiresDefaultValidation = true;
            return;
        }

        // Check for any composite index uniqueness constraints
        if (!this.uniqueConstraintCompositeIndexes.isEmpty()) {
            this.requiresDefaultValidation = true;
            return;
        }
    }

    @Override
    SchemaObjectType toSchemaItem(Permazen jdb) {
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

    private IllegalArgumentException invalidIndex(
      io.permazen.annotation.JCompositeIndex annotation, String format, Object... args) {
        return new IllegalArgumentException(String.format(
          "invalid @%s annotation for index \"%s\" on %s: %s", io.permazen.annotation.JCompositeIndex.class.getSimpleName(),
          annotation.name(), this.type, String.format(format, args)));
    }

    // Add new JField (and sub-fields, if any), checking for name and storage ID conflicts
    private void addField(JField jfield) {

        // Check for storage ID conflict; note we can get this legitimately when a field is declared only
        // in supertypes, where two of the supertypes are mutually unassignable from each other. In that
        // case, verify that the generated field is the same.
        JField other = this.jfields.get(jfield.storageId);
        if (other != null) {

            // If the descriptions differ, no need to give any more details
            if (!other.toString().equals(jfield.toString())) {
                throw new IllegalArgumentException(String.format(
                  "illegal duplicate use of storage ID %d for both %s and %s", jfield.storageId, other, jfield));
            }

            // Check whether the fields are exactly the same; if not, there is a conflict
            if (!other.isSameAs(jfield)) {
                throw new IllegalArgumentException(String.format(
                  "two or more methods defining %s conflict: %s and %s", jfield, other.getter, jfield.getter));
            }

            // OK - they are the same thing
            return;
        }
        this.jfields.put(jfield.storageId, jfield);

        // Check for name conflict
        if ((other = this.jfieldsByName.get(jfield.name)) != null) {
            throw new IllegalArgumentException(String.format(
              "illegal duplicate use of field name \"%s\" in %s", jfield.name, this));
        }
        this.jfieldsByName.put(jfield.name, jfield);
        jfield.parent = this;

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("added {} to object type \"{}\"", jfield, this.name);
    }

    // Get field name, deriving it from the getter property name if necessary
    private String getFieldName(String fieldName, AnnotationScanner<T, ?>.MethodInfo info, String description) {
        if (fieldName.length() > 0)
            return fieldName;
        try {
            return info.getMethodPropertyName();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: can't infer field name: %s", description, e.getMessage()), e);
        }
    }

    // Get the n'th generic type parameter
    private Type getParameterType(String description, Method method, int index) {
        try {
            return Util.getTypeParameter(method.getGenericReturnType(), index);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: invalid method return type: %s", description, e.getMessage()), e);
        }
    }

    // Create a simple field, either regular object field or sub-field of complex field
    @SuppressWarnings("unchecked")
    private JSimpleField createSimpleField(String description, TypeToken<?> encodingToken, String fieldName,
      int storageId, io.permazen.annotation.JField annotation, Method getter, Method setter, String fieldDescription) {

        // Get explicit encoding, if any
        EncodingId encodingId = null;
        final String encodingName = annotation.encoding().length() > 0 ? annotation.encoding() : null;
        if (encodingName != null) {
            try {
                encodingId = this.jdb.db.getEncodingRegistry().idForAlias(encodingName);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("invalid %s: invalid encoding \"%s\"", description, encodingName));
            }
        }

        // Include containing type for annotation description; with autogenProperties it can be more than one
        description += " in " + this.type;

        // Complex sub-field?
        final boolean isSubField = setter == null;

        // Sanity check annotation
        if (isSubField && annotation.unique()) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: unique() constraint not allowed on complex sub-field", description));
        }
        if (annotation.uniqueExclude().length > 0 && !annotation.unique()) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: use of uniqueExclude() requires unique = true", description));
        }

        // See if encoding encompasses one or more JClass types and is therefore a reference type
        final Class<?> fieldRawType = encodingToken.getRawType();
        boolean isReferenceType = false;
        for (JClass<?> jclass : this.jdb.jclasses.values()) {
            if (fieldRawType.isAssignableFrom(jclass.type)) {
                isReferenceType = true;
                break;
            }
        }

        // Check for reference to UntypedJObject - not currently allowed
        if (UntypedJObject.class.isAssignableFrom(fieldRawType)) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: references to %s%s are not allowed; use %s instead", description,
              !UntypedJObject.class.equals(fieldRawType) ? "sub-types of " : "", UntypedJObject.class.getName(),
              JObject.class.getName()));
        }

        // See if encoding is a simple type, known either by explicitly-given encoding or type
        Encoding<?> nonReferenceType = null;
        if (encodingId != null) {

            // Field encoding is explicitly specified
            if ((nonReferenceType = this.jdb.db.getEncodingRegistry().getEncoding(encodingId)) == null)
                throw new IllegalArgumentException(String.format("invalid %s: unknown encoding \"%s\"", description, encodingName));

            // Verify encoding matches what we expect
            final TypeToken<?> expectedType = isSubField ? nonReferenceType.getTypeToken().wrap() : nonReferenceType.getTypeToken();
            if (!expectedType.equals(encodingToken)) {
                throw new IllegalArgumentException(String.format("invalid %s: encoding \"%s\" supports values"
                  + " of type %s but %s is required (according to the getter method's return type)",
                  description, encodingName, nonReferenceType.getTypeToken(), encodingToken));
            }
        } else {

            // Try to find an encoding supporting getter method return type
            final List<? extends Encoding<?>> encodings = this.jdb.db.getEncodingRegistry().getEncodings(encodingToken);
            switch (encodings.size()) {
            case 0:
                nonReferenceType = null;
                break;
            case 1:
                nonReferenceType = encodings.get(0);
                break;
            default:
                if (!isReferenceType) {
                    throw new IllegalArgumentException(String.format("invalid %s: an explicit encoding() must be"
                      + " specified because type %s is supported by multiple registered simple encodings: %s",
                      description, encodingToken, encodings));
                }
                break;
            }
        }

        // Handle enum and enum array types
        Class<? extends Enum<?>> enumType = null;
        Class<?> enumArrayEncoding = null;
        int enumArrayDimensions = -1;
        if (nonReferenceType == null) {

            // Handle non-array Enum type
            enumType = Enum.class.isAssignableFrom(fieldRawType) ?
              (Class<? extends Enum<?>>)fieldRawType.asSubclass(Enum.class) : null;
            if (enumType != null) {
                nonReferenceType = new EnumValueEncoding(enumType);
                enumArrayDimensions = 0;
            }

            // Handle array Enum type
            if (fieldRawType.isArray()) {

                // Get base type and count dimensions
                assert nonReferenceType == null;
                enumArrayDimensions = 0;
                Class<?> baseType = fieldRawType;
                while (baseType != null && baseType.isArray()) {
                    enumArrayDimensions++;
                    baseType = baseType.getComponentType();
                }

                // Is array base type an Enum type?
                if (Enum.class.isAssignableFrom(baseType)) {

                    // Get base Enum<?> type
                    enumType = (Class<? extends Enum<?>>)baseType.asSubclass(Enum.class);

                    // Get the corresponding EnumValue[][]... Java type and Encoding (based on the Enum's identifier list)
                    nonReferenceType = new EnumValueEncoding(enumType);
                    for (int i = 0; i < enumArrayDimensions; i++)
                        nonReferenceType = SimpleEncodingRegistry.buildArrayEncoding(nonReferenceType);

                    // Save type info
                    enumType = (Class<? extends Enum<?>>)baseType.asSubclass(Enum.class);
                    enumArrayEncoding = fieldRawType;
                }
            }
        }

        // If field's type neither refers to a JClass type, nor has a registered encoding, nor is an enum type, fail
        if (!isReferenceType && nonReferenceType == null && enumType == null && enumArrayEncoding == null) {
            throw new IllegalArgumentException(String.format("invalid %s: an explicit encoding() must be specified"
              + " because no registered encoding encodes values of type %s", description, encodingToken));
        }

        // Handle ambiguity between reference vs. non-reference
        if (isReferenceType && nonReferenceType != null) {

            // If an explicit encoding was provided, assume they want the specified non-reference type
            if (encodingId != null)
                isReferenceType = false;
            else {
                throw new IllegalArgumentException(String.format("invalid %s: an explicit encoding() must be specified because"
                  + " type %s is ambiguous, being both a @%s reference type and a simple Java type supported by type \"%s\"",
                  description, encodingToken, PermazenType.class.getSimpleName(), nonReferenceType));
            }
        }

        // Sanity check annotation some more
        if (!isReferenceType && annotation.inverseDelete() != DeleteAction.EXCEPTION) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: inverseDelete() only allowed on reference fields", description));
        }
        if (!isReferenceType && annotation.forwardDelete()) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: forwardDelete() only allowed on reference fields", description));
        }
        if (!isReferenceType && annotation.unique() && !annotation.indexed()) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: unique() constraint requires field to be indexed", description));
        }
        if (nonReferenceType != null
          && nonReferenceType.getTypeToken().isPrimitive()
          && Arrays.asList(annotation.uniqueExclude()).contains(io.permazen.annotation.JField.NULL)) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: uniqueExclude() = JField.NULL is incompatible with fields having primitive type", description));
        }
        if (!isReferenceType && annotation.forwardCascades().length != 0) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: forwardCascades() only allowed on reference fields", description));
        }
        if (!isReferenceType && annotation.inverseCascades().length != 0) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: inverseCascades() only allowed on reference fields", description));
        }

        // Create simple, enum, enum array, or reference field
        try {
            return
              isReferenceType ?
                new JReferenceField(this.jdb, fieldName, storageId, fieldDescription, encodingToken, annotation, getter, setter) :
              enumArrayEncoding != null ?
                new JEnumArrayField(this.jdb, fieldName, storageId, enumType,
                 enumArrayEncoding, enumArrayDimensions, nonReferenceType, annotation, fieldDescription, getter, setter) :
              enumType != null ?
                new JEnumField(this.jdb, fieldName, storageId, enumType, annotation, fieldDescription, getter, setter) :
                new JSimpleField(this.jdb, fieldName, storageId, encodingToken,
                  nonReferenceType, annotation.indexed(), annotation, fieldDescription, getter, setter);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("invalid %s: %s", description, e.getMessage()), e);
        }
    }
}
