
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.PermazenCompositeIndexes;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.core.EnumValueEncoding;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.SetField;
import io.permazen.core.UnknownFieldException;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.encoding.SimpleEncodingRegistry;
import io.permazen.kv.KeyRanges;
import io.permazen.schema.SchemaObjectType;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a specific Permazen object type in a {@link Permazen} database.
 *
 * @param <T> the Java type that represents instances of this database object type
 */
public class PermazenClass<T> extends PermazenSchemaItem {

    final Logger log = LoggerFactory.getLogger(this.getClass());

    final Permazen pdb;
    final Class<T> type;
    final PermazenType permazenType;
    final ClassGenerator<T> classGenerator;
    final TreeMap<String, PermazenField> fieldsByName = new TreeMap<>();                      // does not include sub-fields
    final TreeMap<Integer, PermazenField> fieldsByStorageId = new TreeMap<>();                // does not include sub-fields
    final TreeMap<String, PermazenSimpleField> simpleFieldsByName = new TreeMap<>();          // does include sub-fields
    final TreeMap<Integer, PermazenSimpleField> simpleFieldsByStorageId = new TreeMap<>();    // does include sub-fields
    final TreeMap<String, PermazenCompositeIndex> jcompositeIndexesByName = new TreeMap<>();
    final ArrayList<PermazenSimpleField> uniqueConstraintFields = new ArrayList<>();
    final ArrayList<PermazenCompositeIndex> uniqueConstraintCompositeIndexes = new ArrayList<>();
    final ArrayList<PermazenField> upgradeConversionFields = new ArrayList<>();                // only simple and counter fields
    final HashMap<String, List<PermazenReferenceField>> forwardCascadeMap = new HashMap<>();
    final HashMap<String, Map<Integer, KeyRanges>> inverseCascadeMap = new HashMap<>();

    Set<ReferencePathScanner<T>.ReferencePathMethodInfo> referencePathMethods;
    Set<OnCreateScanner<T>.MethodInfo> onCreateMethods;
    Set<OnDeleteScanner<T>.MethodInfo> onDeleteMethods;
    Set<OnChangeScanner<T>.MethodInfo> onChangeMethods;
    Set<OnValidateScanner<T>.MethodInfo> onValidateMethods;
    Set<OnSchemaChangeScanner<T>.MethodInfo> onSchemaChangeMethods;

    boolean requiresDefaultValidation;
    AnnotatedElement elementRequiringJSR303Validation;

    /**
     * Constructor.
     *
     * @param name the name of the object type
     * @param storageId object type storage ID, or zero to automatically assign
     * @param type object type Java model class
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    PermazenClass(Permazen pdb, String name, int storageId, Class<T> type) {
        super(name, storageId, String.format("object type \"%s\" (%s)", name, type));
        Preconditions.checkArgument(name != null, "null name");
        if (UntypedPermazenObject.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException(String.format(
              "invalid model type %s: model types may not subclass %s", type.getName(), UntypedPermazenObject.class.getName()));
        }
        this.pdb = pdb;
        this.type = type;
        this.classGenerator = new ClassGenerator<>(pdb, this);
        this.permazenType = Util.getAnnotation(this.type, PermazenType.class);
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
     * Get all {@link PermazenField}'s associated with this instance, indexed by name.
     *
     * <p>
     * The returned map does not include sub-fields of complex fields.
     *
     * @return read-only mapping from name to {@link PermazenField}
     */
    public NavigableMap<String, PermazenField> getFieldsByName() {
        return Collections.unmodifiableNavigableMap(this.fieldsByName);
    }

    /**
     * Get all {@link PermazenField}'s associated with this instance, indexed by storage ID.
     *
     * <p>
     * The returned map does not include sub-fields of complex fields.
     *
     * @return read-only mapping from storage ID to {@link PermazenField}
     */
    public NavigableMap<Integer, PermazenField> getFieldsByStorageId() {
        return Collections.unmodifiableNavigableMap(this.fieldsByStorageId);
    }

    /**
     * Get all {@link PermazenCompositeIndex}'s defined on this {@link PermazenClass}, indexed by name.
     *
     * @return read-only mapping from name to {@link PermazenCompositeIndex}
     */
    public NavigableMap<String, PermazenCompositeIndex> getCompositeIndexesByName() {
        return Collections.unmodifiableNavigableMap(this.jcompositeIndexesByName);
    }

    /**
     * Get the specified {@link PermazenField} in this database object type, cast to the given Java type.
     *
     * @param fieldName field name
     * @param type required type
     * @param <T> expected encoding
     * @return corresponding {@link PermazenField} in this instance
     * @throws UnknownFieldException if {@code fieldName} is not found
     * @throws UnknownFieldException if the field is not an instance of of {@code type}
     * @throws IllegalArgumentException if either parameter is null
     */
    public <T extends PermazenField> T getField(String fieldName, Class<T> type) {
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        Preconditions.checkArgument(type != null, "null type");
        final PermazenField pfield = this.fieldsByName.get(fieldName);
        if (pfield == null) {
            throw new UnknownFieldException(fieldName,
              String.format("object type \"%s\" has no field named \"%s\"", this.name, fieldName));
        }
        try {
            return type.cast(pfield);
        } catch (ClassCastException e) {
            throw new UnknownFieldException(fieldName,
              String.format("object type \"%s\" has no field named \"%s\" of type %s (found %s instead)",
                this.name, fieldName, type.getName(), pfield));
        }
    }

    @Override
    public ObjType getSchemaItem() {
        return (ObjType)super.getSchemaItem();
    }

// Package Methods

    void replaceSchemaItems(Schema schema) {
        final ObjType objType = schema.getObjType(this.name);
        this.schemaItem = objType;
        this.fieldsByName.values().forEach(field -> field.replaceSchemaItems(objType));
        this.jcompositeIndexesByName.values().forEach(index -> index.replaceSchemaItems(objType));
    }

    void createFields(EncodingRegistry encodingRegistry, Collection<PermazenClass<?>> pclasses) {

        // Scan for Simple and Counter fields
        final PermazenFieldScanner<T> simpleFieldScanner = new PermazenFieldScanner<>(this, this.permazenType);
        for (PermazenFieldScanner<T>.MethodInfo info : simpleFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.PermazenField annotation = info.getAnnotation();
            final Method getter = Util.findPermazenFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", simpleFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            final TypeToken<?> encodingToken = TypeToken.of(this.type).resolveType(getter.getGenericReturnType());
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

            // Handle Counter fields
            if (encodingToken.equals(TypeToken.of(Counter.class))) {

                // Sanity check annotation
                if (!annotation.encoding().isEmpty()) {
                    throw new IllegalArgumentException(String.format(
                      "invalid %s: counter fields must not specify an encoding", description));
                }
                if (annotation.indexed()) {
                    throw new IllegalArgumentException(String.format(
                      "invalid %s: counter fields cannot be indexed", description));
                }

                // Create counter field
                final PermazenCounterField pfield = new PermazenCounterField(fieldName, annotation.storageId(), annotation,
                  String.format("counter field \"%s\" of object type \"%s\"", fieldName, this.name), getter);

                // Remember upgrade conversion fields
                if (annotation.upgradeConversion().isConvertsValues())
                    this.upgradeConversionFields.add(pfield);

                // Add field
                this.addField(pfield);
                continue;
            }

            // Find corresponding setter method
            final Method setter;
            try {
                setter = Util.findPermazenFieldSetterMethod(this.type, getter);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("invalid %s: %s", description, e.getMessage()));
            }

            // Create simple field
            final PermazenSimpleField pfield = this.createSimpleField(encodingRegistry, pclasses,
              description, encodingToken, fieldName, annotation.storageId(), annotation, getter, setter,
              String.format("field \"%s\" of object type \"%s\"", fieldName, this.name));

            // Add field
            this.addField(pfield);

            // Remember unique constraint fields
            if (pfield.unique)
                this.uniqueConstraintFields.add(pfield);

            // Remember upgrade conversion fields
            if (annotation.upgradeConversion().isConvertsValues())
                this.upgradeConversionFields.add(pfield);
        }

        // Scan for Set fields
        final PermazenSetFieldScanner<T> setFieldScanner = new PermazenSetFieldScanner<>(this, this.permazenType);
        for (PermazenSetFieldScanner<T>.MethodInfo info : setFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.PermazenSetField annotation = info.getAnnotation();
            final io.permazen.annotation.PermazenField elementAnnotation = annotation.element();
            final Method getter = Util.findPermazenFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", setFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

            // Get element type (the raw return type has already been validated by the annotation scanner)
            final TypeToken<?> elementType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 0));

            // Create element sub-field
            final PermazenSimpleField elementField = this.createSimpleField(encodingRegistry, pclasses,
              "element() property of " + description, elementType, SetField.ELEMENT_FIELD_NAME,
              elementAnnotation.storageId(), elementAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"",
              "set", fieldName, SetField.ELEMENT_FIELD_NAME, this.name));

            // Create set field
            final PermazenSetField pfield = new PermazenSetField(fieldName, annotation.storageId(), annotation, elementField,
              String.format("%s field \"%s\" in object type \"%s\"", "set", fieldName, this.name), getter);
            elementField.parent = pfield;

            // Add field
            this.addField(pfield);
        }

        // Scan for List fields
        final PermazenListFieldScanner<T> listFieldScanner = new PermazenListFieldScanner<>(this, this.permazenType);
        for (PermazenListFieldScanner<T>.MethodInfo info : listFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.PermazenListField annotation = info.getAnnotation();
            final io.permazen.annotation.PermazenField elementAnnotation = annotation.element();
            final Method getter = Util.findPermazenFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", listFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

            // Get element type (the raw return type has already been validated by the annotation scanner)
            final TypeToken<?> elementType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 0));

            // Create element sub-field
            final PermazenSimpleField elementField = this.createSimpleField(encodingRegistry, pclasses,
              "element() property of " + description, elementType, ListField.ELEMENT_FIELD_NAME,
              elementAnnotation.storageId(), elementAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"",
              "list", fieldName, ListField.ELEMENT_FIELD_NAME, this.name));

            // Create list field
            final PermazenListField pfield = new PermazenListField(fieldName, annotation.storageId(), annotation, elementField,
              String.format("%s field \"%s\" in object type \"%s\"", "list", fieldName, this.name), getter);
            elementField.parent = pfield;

            // Add field
            this.addField(pfield);
        }

        // Scan for Map fields
        final PermazenMapFieldScanner<T> mapFieldScanner = new PermazenMapFieldScanner<>(this, this.permazenType);
        for (PermazenMapFieldScanner<T>.MethodInfo info : mapFieldScanner.findAnnotatedMethods()) {

            // Get info
            final io.permazen.annotation.PermazenMapField annotation = info.getAnnotation();
            final io.permazen.annotation.PermazenField keyAnnotation = annotation.key();
            final io.permazen.annotation.PermazenField valueAnnotation = annotation.value();
            final Method getter = Util.findPermazenFieldGetterMethod(this.type, info.getMethod());
            final String description = String.format(
               "%s annotation on method %s", mapFieldScanner.getAnnotationDescription(), getter);
            final String fieldName = this.getFieldName(annotation.name(), info, description);
            if (this.log.isTraceEnabled())
                this.log.trace("found {}", description);

            // Get key and value types (the raw return type has already been validated by the annotation scanner)
            final TypeToken<?> keyType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 0));
            final TypeToken<?> valueType = TypeToken.of(this.type).resolveType(this.getParameterType(description, getter, 1));

            // Create key and value sub-fields
            final PermazenSimpleField keyField = this.createSimpleField(encodingRegistry, pclasses,
              "key() property of " + description, keyType, MapField.KEY_FIELD_NAME,
              keyAnnotation.storageId(), keyAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"", "map", fieldName, MapField.KEY_FIELD_NAME, this.name));
            final PermazenSimpleField valueField = this.createSimpleField(encodingRegistry, pclasses,
              "value() property of " + description, valueType, MapField.VALUE_FIELD_NAME,
              valueAnnotation.storageId(), valueAnnotation, null, null,
              String.format("%s sub-field \"%s.%s\" in object type \"%s\"", "map",
              fieldName, MapField.VALUE_FIELD_NAME, this.name));

            // Create map field
            final PermazenMapField pfield = new PermazenMapField(fieldName, annotation.storageId(), annotation,
              keyField, valueField, String.format("%s field \"%s\" in object type \"%s\"", "map", fieldName, this.name), getter);
            keyField.parent = pfield;
            valueField.parent = pfield;

            // Add field
            this.addField(pfield);
        }

        // Verify that the generated class will not have any remaining abstract methods
        final Map<MethodKey, Method> abstractMethods = Util.findAbstractMethods(this.type);
        for (PermazenField pfield : this.fieldsByName.values()) {
            abstractMethods.remove(new MethodKey(pfield.getter));
            if (pfield instanceof PermazenSimpleField)
                abstractMethods.remove(new MethodKey(((PermazenSimpleField)pfield).setter));
        }
        for (Method method : PermazenObject.class.getDeclaredMethods())
            abstractMethods.remove(new MethodKey(method));
        for (Iterator<Method> i = abstractMethods.values().iterator(); i.hasNext(); ) {
            if (Util.getAnnotation(i.next(), io.permazen.annotation.ReferencePath.class) != null)
                i.remove();
        }
        if (!abstractMethods.isEmpty()) {
            throw new IllegalArgumentException(String.format(
              "the @%s-annotated type %s is invalid because %d abstract method(s) remain unimplemented: %s",
              PermazenType.class.getSimpleName(), this.type.getName(), abstractMethods.size(),
              abstractMethods.values().toString().replaceAll("^\\[(.*)\\]$", "$1")));
        }

        // Calculate which fields require default validation
        this.fieldsByName.values().forEach(PermazenField::calculateRequiresDefaultValidation);
    }

    // Like fields, composite indexes are inherited (duplicated) from supertypes
    void createCompositeIndexes() {
        for (Class<?> supertype : TypeToken.of(this.type).getTypes().rawTypes()) {

            // Decode possibly repeated annotation(s)
            final io.permazen.annotation.PermazenCompositeIndex[] annotations;
            final PermazenCompositeIndexes container = Util.getAnnotation(supertype, PermazenCompositeIndexes.class);
            if (container != null)
                annotations = container.value();
            else {
                io.permazen.annotation.PermazenCompositeIndex annotation = Util.getAnnotation(supertype,
                  io.permazen.annotation.PermazenCompositeIndex.class);
                if (annotation == null)
                    continue;
                annotations = new io.permazen.annotation.PermazenCompositeIndex[] { annotation };
            }

            // Create corresponding indexes
            for (io.permazen.annotation.PermazenCompositeIndex annotation : annotations) {
                if (annotation.uniqueExclude().length > 0 && !annotation.unique()) {
                    throw new IllegalArgumentException(String.format(
                      "invalid @%s annotation on %s: use of uniqueExclude() requires unique = true",
                      io.permazen.annotation.PermazenCompositeIndex.class.getSimpleName(), supertype));
                }
                this.addCompositeIndex(supertype, annotation);
            }
        }
    }

    private void addCompositeIndex(Class<?> declaringType, io.permazen.annotation.PermazenCompositeIndex annotation) {

        // Get info
        final String indexName = annotation.name();

        // Resolve field names
        final String[] fieldNames = annotation.fields();
        final PermazenSimpleField[] indexFields = new PermazenSimpleField[fieldNames.length];
        final HashSet<String> seenFieldNames = new HashSet<>();
        for (int i = 0; i < fieldNames.length; i++) {
            final String fieldName = fieldNames[i];
            if (!seenFieldNames.add(fieldName))
                throw this.invalidIndex(annotation, "field \"%s\" appears more than once", fieldName);
            final PermazenField pfield = this.fieldsByName.get(fieldName);
            if (pfield == null)
                throw this.invalidIndex(annotation, "field \"%s\" not found", fieldName);
            else if (!(pfield instanceof PermazenSimpleField))
                throw this.invalidIndex(annotation, "field \"%s\" is not a simple field", fieldName);
            indexFields[i] = (PermazenSimpleField)pfield;
        }

        // Create and add index
        final PermazenCompositeIndex index = new PermazenCompositeIndex(indexName,
          annotation.storageId(), declaringType, annotation, indexFields);
        if (this.jcompositeIndexesByName.put(indexName, index) != null)
            throw this.invalidIndex(annotation, "duplicate composite index name \"%s\"", indexName);

        // Remember unique constraint composite indexes and trigger validation when any indexed field changes
        if (index.unique) {
            this.uniqueConstraintCompositeIndexes.add(index);
            for (PermazenSimpleField pfield : index.pfields)
                pfield.requiresDefaultValidation = true;
        }
    }

    private IllegalArgumentException invalidIndex(
      io.permazen.annotation.PermazenCompositeIndex annotation, String format, Object... args) {
        return new IllegalArgumentException(String.format(
          "invalid @%s annotation for index \"%s\" on %s: %s", io.permazen.annotation.PermazenCompositeIndex.class.getSimpleName(),
          annotation.name(), this.type, String.format(format, args)));
    }

    void scanAnnotations() {
        this.referencePathMethods = new ReferencePathScanner<>(this).findReferencePathMethods();
        this.onCreateMethods = new OnCreateScanner<>(this).findAnnotatedMethods();
        this.onDeleteMethods = new OnDeleteScanner<>(this).findAnnotatedMethods();
        this.onChangeMethods = new OnChangeScanner<>(this).findAnnotatedMethods();
        this.onValidateMethods = new OnValidateScanner<>(this).findAnnotatedMethods();
        this.onSchemaChangeMethods = new OnSchemaChangeScanner<>(this).findAnnotatedMethods();
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
    SchemaObjectType toSchemaItem() {
        final SchemaObjectType objectType = (SchemaObjectType)super.toSchemaItem();
        objectType.setSchemaSalt(this.permazenType.schemaSalt());
        this.fieldsByName.forEach((name, field) -> objectType.getSchemaFields().put(name, field.toSchemaItem()));
        this.jcompositeIndexesByName.forEach(
          (name, index) -> objectType.getSchemaCompositeIndexes().put(name, index.toSchemaItem()));
        return objectType;
    }

    @Override
    SchemaObjectType createSchemaItem() {
        return new SchemaObjectType();
    }

    @Override
    void visitSchemaItems(Consumer<? super PermazenSchemaItem> visitor) {
        super.visitSchemaItems(visitor);
        this.fieldsByName.values().forEach(item -> item.visitSchemaItems(visitor));
        this.jcompositeIndexesByName.values().forEach(item -> item.visitSchemaItems(visitor));
    }

    // Add new PermazenField (and sub-fields, if any), checking for name conflicts
    private void addField(PermazenField pfield) {

        // Check for field name conflict; note we can get this legitimately when a field is declared
        // only in supertypes when two of the supertypes are mutually unassignable from each other.
        // In that case, if the generated field is exactlly the same then allow it.
        PermazenField other = this.fieldsByName.put(pfield.name, pfield);
        if (other != null) {

            // If the descriptions differ, no need to give any more details
            final String desc1 = other.toString();
            final String desc2 = pfield.toString();
            if (!desc2.equals(desc1)) {
                throw new IllegalArgumentException(String.format(
                  "illegal duplicate use of name \"%s\" for both %s and %s in %s", pfield.name, desc1, desc2, this));
            }

            // Check whether the fields are exactly the same; if not, there is a conflict
            if (!other.isSameAs(pfield)) {
                throw new IllegalArgumentException(String.format(
                  "two or more methods defining %s conflict: %s and %s", pfield, other.getter, pfield.getter));
            }
        }
        pfield.parent = this;

        // Update various maps
        if (pfield instanceof PermazenSimpleField) {
            final PermazenSimpleField psimpleField = (PermazenSimpleField)pfield;
            this.simpleFieldsByName.put(psimpleField.getFullName(), psimpleField);
        } else if (pfield instanceof PermazenComplexField) {
            for (PermazenSimpleField psimpleField : ((PermazenComplexField)pfield).getSubFields())
                this.simpleFieldsByName.put(psimpleField.getFullName(), psimpleField);
        }

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("added {} to object type \"{}\"", pfield, this.name);
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
    private PermazenSimpleField createSimpleField(EncodingRegistry encodingRegistry, Collection<PermazenClass<?>> pclasses,
      String description, TypeToken<?> encodingToken, String fieldName, int storageId,
      io.permazen.annotation.PermazenField annotation, Method getter, Method setter, String fieldDescription) {

        // Get explicit encoding, if any
        EncodingId encodingId = null;
        final String encodingName = annotation.encoding().length() > 0 ? annotation.encoding() : null;
        if (encodingName != null) {
            try {
                encodingId = encodingRegistry.idForAlias(encodingName);
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

        // See if encoding encompasses one or more PermazenClass types and is therefore a reference type
        final Class<?> fieldRawType = encodingToken.getRawType();
        boolean isReferenceType = false;
        for (PermazenClass<?> pclass : pclasses) {
            if (fieldRawType.isAssignableFrom(pclass.type)) {
                isReferenceType = true;
                break;
            }
        }

        // Check for reference to UntypedPermazenObject - not currently allowed
        if (UntypedPermazenObject.class.isAssignableFrom(fieldRawType)) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: references to %s%s are not allowed; use %s instead", description,
              !UntypedPermazenObject.class.equals(fieldRawType) ? "sub-types of " : "", UntypedPermazenObject.class.getName(),
              PermazenObject.class.getName()));
        }

        // See if encoding is a simple type, known either by explicitly-given encoding or type
        Encoding<?> nonReferenceType = null;
        if (encodingId != null) {

            // Field encoding is explicitly specified
            if ((nonReferenceType = encodingRegistry.getEncoding(encodingId)) == null)
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
            final List<? extends Encoding<?>> encodings = encodingRegistry.getEncodings(encodingToken);
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

        // If field's type neither refers to a PermazenClass type, nor has a registered encoding, nor is an enum type, fail
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
          && Arrays.asList(annotation.uniqueExclude()).contains(io.permazen.annotation.PermazenField.NULL)) {
            throw new IllegalArgumentException(String.format(
              "invalid %s: uniqueExclude() = PermazenField.NULL is incompatible with fields having primitive type", description));
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
                new PermazenReferenceField(fieldName, storageId,
                  fieldDescription, encodingToken, annotation, pclasses, getter, setter) :
              enumArrayEncoding != null ?
                new PermazenEnumArrayField(fieldName, storageId, enumType, enumArrayEncoding,
                  enumArrayDimensions, nonReferenceType, annotation, fieldDescription, getter, setter) :
              enumType != null ?
                new PermazenEnumField(fieldName, storageId, enumType, annotation, fieldDescription, getter, setter) :
                new PermazenSimpleField(fieldName, storageId, encodingToken, nonReferenceType,
                  annotation.indexed(), annotation, fieldDescription, getter, setter);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("invalid %s: %s", description, e.getMessage()), e);
        }
    }
}
