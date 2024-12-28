
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.CoreIndex1;
import io.permazen.core.CoreIndex2;
import io.permazen.core.CoreIndex3;
import io.permazen.core.CoreIndex4;
import io.permazen.core.ReferenceEncoding;
import io.permazen.encoding.Encoding;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.schema.SchemaId;
import io.permazen.util.ByteData;
import io.permazen.util.TypeTokens;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Information used for index queries.
 */
class IndexQuery {

    private static final KeyRange NULL_RANGE = new KeyRange(ByteData.of(0xff), null);

    final PermazenSchemaItem schemaItem;            // a REPRESENTATIVE schema item

    private final Class<?> targetType;
    private final ArrayList<KeyRanges> filters = new ArrayList<>();

// Constructors

    // Constructor for all simple index queries other than map value
    IndexQuery(Permazen pdb, Class<?> targetType, String fieldName, Class<?> valueType) {
        this(pdb, targetType, fieldName, valueType, null);
    }

    // Constructor for map value index queries
    IndexQuery(Permazen pdb, Class<?> targetType, String fieldName, Class<?> valueType, Class<?> keyType) {

        // Sanity check
        Preconditions.checkArgument(pdb != null, "null pdb");
        Preconditions.checkArgument(targetType != null, "null targetType");
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        Preconditions.checkArgument(valueType != null, "null valueType");

        // Get start type
        Preconditions.checkArgument(!targetType.isPrimitive() && !targetType.isArray(), "invalid targetType " + targetType);
        this.targetType = targetType;

        // Identify the value field(s)
        final ArrayList<PermazenSimpleField> pfields = new ArrayList<>();
        boolean foundAny = false;
        SchemaId schemaId = null;
        for (PermazenClass<?> pclass : pdb.getPermazenClasses(targetType)) {

            // Find the field
            PermazenSimpleField pfield = Util.findSimpleField(pclass, fieldName);
            if (pfield == null)
                continue;
            foundAny = true;

            // Is the field actually indexed?
            if (!pfield.indexed)
                continue;

            // Does the field conflict with another field of the same name encoding-wise?
            final SchemaId fieldSchemaId = pfield.getSchemaId();
            if (pfields.isEmpty())
                schemaId = fieldSchemaId;
            else if (!fieldSchemaId.equals(schemaId)) {
                throw new IllegalArgumentException(String.format(
                  "field name \"%s\" is ambiguous in %s, matching incompatible fields in %s and %s",
                  fieldName, targetType, pfield, pfields.get(0)));
            }

            // Add field
            pfields.add(pfield);
        }

        // Any fields found?
        if (pfields.isEmpty()) {
            throw new IllegalArgumentException(foundAny ?
              String.format("field \"%s\" in %s is not indexed", fieldName, targetType) :
              String.format("field \"%s\" not found in %s", fieldName, targetType));
        }

        // Use the first field found as representative
        final PermazenSimpleField pfield = pfields.get(0);
        this.schemaItem = pfield;

        // Identify the value type(s)
        final Set<TypeToken<?>> valueTypes = pfields.stream()
          .map(PermazenSimpleField::getTypeToken)
          .collect(Collectors.toSet());

        // Verify value type
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(3);
        valueChecks.add(new ValueCheck("value type", valueType, this.wrapRaw(valueTypes), pfield.encoding));

        // Verify target type
        valueChecks.add(new ValueCheck("target type", this.targetType));

        // Add additional check for the map key type when doing a map value index query
        if (keyType != null) {
            final PermazenMapField parent = (PermazenMapField)pfield.getParentField();
            final String keyFieldName = parent.getKeyField().getFullName();
            valueChecks.add(new ValueCheck("map key type", keyType,
              this.wrapRaw(this.getTypeTokens(pdb, this.targetType, keyFieldName)), parent.keyField.encoding));
        }

        // Check values
        this.checkValues(pdb, valueChecks, "index query on field \"%s\"", fieldName);
    }

    // Constructor for composite index queries
    IndexQuery(Permazen pdb, Class<?> targetType, String indexName, Class<?>... valueTypes) {

        // Sanity check
        Preconditions.checkArgument(pdb != null, "null pdb");
        Preconditions.checkArgument(targetType != null, "null targetType");
        Preconditions.checkArgument(indexName != null, "null indexName");
        Preconditions.checkArgument(valueTypes != null, "null valueTypes");
        Preconditions.checkArgument(valueTypes.length >= 2, "not enough value types");

        // Get start type
        Preconditions.checkArgument(!targetType.isPrimitive() && !targetType.isArray(), "invalid targetType " + targetType);
        this.targetType = targetType;

        // Find composite index(es)
        PermazenCompositeIndex index = null;
        boolean foundAny = false;
        final int numValues = valueTypes.length;
        for (PermazenClass<?> pclass : pdb.getPermazenClasses(targetType)) {
            final PermazenCompositeIndex nextIndex = pclass.jcompositeIndexesByName.get(indexName);
            if (nextIndex == null)
                continue;
            foundAny = true;
            if (nextIndex.pfields.size() != numValues)
                continue;
            if (index == null)
                index = nextIndex;
            else if (!nextIndex.getSchemaId().equals(index.getSchemaId())) {
                throw new IllegalArgumentException(String.format("ambiguous composite index name \"%s\":"
                  + " multiple incompatible composite indexes with that name exist on sub-types of %s",
                  indexName, targetType.getName()));
            }
        }
        if (index == null) {
            throw new IllegalArgumentException(String.format(
              "no composite index named \"%s\"%s exists on any sub-type of %s",
              indexName, foundAny ? " on " + numValues + " fields" : "", targetType.getName()));
        }

        // Use the first index found as representative
        this.schemaItem = index;

        // Verify value encodings
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(valueTypes.length + 1);
        for (int i = 0; i < valueTypes.length; i++) {
            final PermazenSimpleField pfield = index.pfields.get(i);
            valueChecks.add(new ValueCheck("value type #" + (i + 1), valueTypes[i],
              this.wrapRaw(this.getTypeTokens(pdb, this.targetType, pfield.name)), pfield.encoding));
        }

        // Verify target type
        valueChecks.add(new ValueCheck("target type", this.targetType));

        // Check values
        this.checkValues(pdb, valueChecks, "query on composite index \"%s\"", indexName);
    }

    private void checkValues(Permazen pdb, List<ValueCheck> valueChecks, String format, Object... args) {
        final String description = String.format(format, args);
        for (ValueCheck check : valueChecks) {
            final KeyRanges ranges = check.checkAndGetKeyRanges(pdb, this.targetType, description);
            this.filters.add(ranges);
        }
    }

    private Set<TypeToken<?>> getTypeTokens(Permazen pdb, Class<?> context, String fieldName) {
        final HashSet<TypeToken<?>> contextEncodings = new HashSet<>();
        for (PermazenClass<?> pclass : pdb.pclasses) {

            // Check if pclass is under consideration
            if (!context.isAssignableFrom(pclass.type))
                continue;

            // Find the simple field field in pclass, if it exists
            final PermazenSimpleField field = pclass.simpleFieldsByName.get(fieldName);
            if (field == null)
                continue;

            // Add field's type in pclass
            contextEncodings.add(field.typeToken);
        }
        if (contextEncodings.isEmpty()) {
            throw new IllegalArgumentException(String.format(
              "no sub-type of %s contains an indexed simple field named \"%s\"", context, fieldName));
        }
        return TypeTokens.findLowestCommonAncestors(contextEncodings.stream());
    }

    private Set<Class<?>> wrapRaw(Set<TypeToken<?>> typeTokens) {
        final HashSet<Class<?>> classes = new HashSet<>(typeTokens.size());
        for (TypeToken<?> typeToken : typeTokens)
            classes.add(typeToken.wrap().getRawType());
        return classes;
    }

// Public Methods

    public <V, T> CoreIndex1<V, T> applyFilters(CoreIndex1<V, T> index) {
        for (int i = 0; i < this.filters.size(); i++) {
            final KeyRanges filter = this.filters.get(i);
            if (filter != null && !filter.isFull())
                index = index.filter(i, filter);
        }
        return index;
    }

    public <V1, V2, T> CoreIndex2<V1, V2, T> applyFilters(CoreIndex2<V1, V2, T> index) {
        for (int i = 0; i < this.filters.size(); i++) {
            final KeyRanges filter = this.filters.get(i);
            if (filter != null && !filter.isFull())
                index = index.filter(i, filter);
        }
        return index;
    }

    public <V1, V2, V3, T> CoreIndex3<V1, V2, V3, T> applyFilters(CoreIndex3<V1, V2, V3, T> index) {
        for (int i = 0; i < this.filters.size(); i++) {
            final KeyRanges filter = this.filters.get(i);
            if (filter != null && !filter.isFull())
                index = index.filter(i, filter);
        }
        return index;
    }

    public <V1, V2, V3, V4, T> CoreIndex4<V1, V2, V3, V4, T> applyFilters(CoreIndex4<V1, V2, V3, V4, T> index) {
        for (int i = 0; i < this.filters.size(); i++) {
            final KeyRanges filter = this.filters.get(i);
            if (filter != null && !filter.isFull())
                index = index.filter(i, filter);
        }
        return index;
    }

    // COMPOSITE-INDEX

    @Override
    public String toString() {
        return "IndexQuery"
          + "[targetType=" + this.targetType
          + ",schemaItem=" + this.schemaItem
          + ",filters=" + this.filters + "]";
    }

// ValueCheck

    // Used to verify that the application of a Java type to a value or target component of an index makes sense.
    private static class ValueCheck {

        private final String description;                   // describes which type component of the index
        private final Class<?> actualType;                  // actual type found for this component
        private final Set<Class<?>> expectedTypes;          // types expected based on index definition
        private final boolean reference;                    // true if we're talking about a reference type
        private final boolean matchNull;                    // whether filter should include null values (all but target)

    // Constructors

        // Primary constructor
        ValueCheck(String description, Class<?> actualType, Set<Class<?>> expectedTypes, boolean reference, boolean matchNull) {
            this.description = description;
            this.actualType = actualType;
            this.expectedTypes = expectedTypes;
            this.reference = reference;
            this.matchNull = matchNull;
        }

        // Constructor for value types
        ValueCheck(String description, Class<?> actualType, Set<Class<?>> expectedTypes, Encoding<?> encoding) {
            this(description, actualType, expectedTypes, encoding instanceof ReferenceEncoding, true);
        }

        // Constructor for target types
        ValueCheck(String description, Class<?> targetType) {
            this(description, targetType, Collections.<Class<?>>singleton(targetType), true, false);
        }

    // Methods

        public KeyRanges checkAndGetKeyRanges(Permazen pdb, Class<?> targetType, String indexDescription) {

            // Check whether actual type matches expected type. For non-reference types, we allow any super-type; for reference
            // types, we allow any super-type or any sub-type (and in the latter case, we apply corresponding key filters).
            boolean match = this.expectedTypes.contains(this.actualType)
              || this.expectedTypes.contains(TypeToken.of(this.actualType).wrap().getRawType());
            if (!match) {
                for (Class<?> expectedType : this.expectedTypes) {
                    if (this.actualType.isAssignableFrom(expectedType)
                      || (this.reference && expectedType.isAssignableFrom(this.actualType))) {
                        match = true;
                        break;
                    }
                }
            }
            if (!match) {
                final StringBuilder expectedTypesDescription = new StringBuilder();
                if (this.expectedTypes.size() == 1)
                    expectedTypesDescription.append(this.expectedTypes.iterator().next().getName());
                else {
                    for (Class<?> expectedType : this.expectedTypes) {
                        expectedTypesDescription.append(expectedTypesDescription.length() == 0 ? "one or more of: " : ", ");
                        expectedTypesDescription.append(expectedType.getName());
                    }
                }
                throw new IllegalArgumentException(String.format("invalid %s %s for %s in %s: should be a super-type%s of %s",
                  this.description, actualType.getName(), indexDescription, targetType,
                  this.reference ? " or sub-type" : "", expectedTypesDescription));
            }

            // For non reference fields, we don't have any restrictions on 'type'
            if (!this.reference)
                return null;

            // If actual type includes all PermazenObject's no filter is necessary
            if (actualType.isAssignableFrom(PermazenObject.class))
                return null;

            // Create a filter for the actual type
            final KeyRanges filter = pdb.keyRangesFor(this.actualType);

            // For values other than the target value, we need to also accept null values in the index
            if (this.matchNull)
                filter.add(NULL_RANGE);

            // Done
            return filter;
        }

    // Object

        @Override
        public String toString() {
            return "ValueCheck"
              + "[description=\"" + this.description + "\""
              + ",actualType=" + this.actualType.getSimpleName()
              + ",expectedTypes=" + this.expectedTypes
              + ",reference=" + this.reference
              + ",matchNull=" + this.matchNull
              + "]";
        }
    }

// Key

    // Key for the IndexQuery cache
    static class Key {

        private final String name;                  // field full name or composite index name
        private final boolean composite;
        private final Class<?> targetType;
        private final Class<?>[] valueTypes;

        // Constructor for a simple index when you already know the field
        Key(PermazenSimpleField pfield) {
            this(pfield.name, false, pfield.getter.getDeclaringClass(), pfield.typeToken.wrap().getRawType());
        }

        // Constructor for a composite index when you already know the index
        Key(PermazenCompositeIndex pindex) {
            this(pindex.name, true, pindex.declaringType, pindex.getQueryInfoValueTypes());
        }

        // Primary constructor
        Key(String name, boolean composite, Class<?> targetType, Class<?>... valueTypes) {
            assert name != null;
            assert targetType != null;
            assert valueTypes != null;
            assert valueTypes.length > 0;
            this.name = name;
            this.composite = composite;
            this.targetType = targetType;
            this.valueTypes = valueTypes;
        }

        public IndexQuery getIndexQuery(Permazen pdb) {

            // Handle composite index
            if (this.composite)
                return new IndexQuery(pdb, this.targetType, this.name, this.valueTypes);

            // Handle simple index
            switch (this.valueTypes.length) {
            case 2:
                return new IndexQuery(pdb, this.targetType, this.name, this.valueTypes[0], this.valueTypes[1]); // map value index
            case 1:
                return new IndexQuery(pdb, this.targetType, this.name, this.valueTypes[0]);                     // all others
            default:
                throw new RuntimeException("internal error");
            }
        }

    // Object

        @Override
        public String toString() {
            return this.getClass().getSimpleName()
              + "[name=" + this.name
              + ",composite=" + this.composite
              + ",targetType=" + this.targetType
              + ",valueTypes=" + Arrays.asList(this.valueTypes)
              + "]";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Key that = (Key)obj;
            return this.name.equals(that.name)
              && this.composite == that.composite
              && this.targetType.equals(that.targetType)
              && Arrays.equals(this.valueTypes, that.valueTypes);
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode()
              ^ this.name.hashCode()
              ^ (this.composite ? ~0 : 0)
              ^ this.targetType.hashCode()
              ^ Arrays.hashCode(this.valueTypes);
        }
    }
}
