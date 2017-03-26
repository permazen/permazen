
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.jsimpledb.core.CoreIndex;
import org.jsimpledb.core.CoreIndex2;
import org.jsimpledb.core.CoreIndex3;
import org.jsimpledb.core.CoreIndex4;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ReferenceFieldType;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;

/**
 * Information used for index queries.
 */
class IndexQueryInfo {

    private static final KeyRange NULL_RANGE = new KeyRange(new byte[] { (byte)0xff }, null);

    final IndexInfo indexInfo;

    private final Class<?> startType;
    private final ArrayList<KeyRanges> filters = new ArrayList<>();

// Constructors

    // Constructor for regular simple index queries
    IndexQueryInfo(JSimpleDB jdb, Class<?> startType, String fieldName, Class<?> valueType) {
        this(jdb, startType, fieldName, valueType, null);
    }

    // Primary constructor (keyType is null except for map value simple index queries)
    IndexQueryInfo(JSimpleDB jdb, Class<?> startType, String fieldName, Class<?> valueType, Class<?> keyType) {

        // Sanity check
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        Preconditions.checkArgument(startType != null, "null startType");
        Preconditions.checkArgument(valueType != null, "null valueType");

        // Get start type
        Preconditions.checkArgument(!startType.isPrimitive() && !startType.isArray(), "invalid startType " + startType);
        this.startType = startType;

        // Parse reference path
        final ReferencePath path = jdb.parseReferencePath(this.startType, fieldName, true, true);
        if (path.getReferenceFields().length > 0)
            throw new IllegalArgumentException("invalid field name `" + fieldName + "': contains intermediate reference(s)");

        // Get field index info (this verifies the field is indexed); keyType != null iff the field is a map value field
        final SimpleFieldIndexInfo fieldIndexInfo = jdb.getIndexInfo(path.targetFieldStorageId,
          keyType != null ? MapValueIndexInfo.class : SimpleFieldIndexInfo.class);
        this.indexInfo = fieldIndexInfo;

        // Verify value type
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(3);
        valueChecks.add(new ValueCheck("value type", valueType,
          this.wrapRaw(path.getTargetFieldTypes()), fieldIndexInfo.getFieldType()));

        // Verify target type
        valueChecks.add(new ValueCheck("target type", startType, path.targetTypes));

        // Add additional check for the map key type when doing a map value index query
        if (keyType != null) {
            final MapValueIndexInfo mapValueIndexInfo = (MapValueIndexInfo)fieldIndexInfo;
            valueChecks.add(new ValueCheck("map key type", keyType, this.wrapRaw(this.getTypeTokens(
               jdb, this.startType, mapValueIndexInfo.getKeyFieldStorageId(), mapValueIndexInfo.getParentStorageId())),
              mapValueIndexInfo.getKeyFieldType()));
        }

        // Check values
        valueChecks.stream()
          .map(check -> check.checkAndGetKeyRanges(jdb, startType, "index query on field `" + fieldName + "'"))
          .forEach(this.filters::add);
    }

    // Constructor for composite index queries
    IndexQueryInfo(JSimpleDB jdb, Class<?> startType, String indexName, Class<?>... valueTypes) {

        // Sanity check
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(indexName != null, "null indexName");
        Preconditions.checkArgument(valueTypes != null, "null valueTypes");

        // Get start type
        Preconditions.checkArgument(!startType.isPrimitive() && !startType.isArray(), "invalid startType " + startType);
        this.startType = startType;

        // Find index
        CompositeIndexInfo compositeIndexInfo = IndexQueryInfo.findCompositeIndex(jdb, startType, indexName, valueTypes.length);
        this.indexInfo = compositeIndexInfo;

        // Verify field types
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(valueTypes.length + 1);
        for (int i = 0; i < valueTypes.length; i++) {
            final Class<?> valueType = valueTypes[i];
            valueChecks.add(new ValueCheck("value type #" + (i + 1), valueType,
              this.wrapRaw(this.getTypeTokens(jdb, this.startType, compositeIndexInfo.getStorageIds().get(i))),
              compositeIndexInfo.getFieldTypes().get(i)));
        }

        // Verify target type
        valueChecks.add(new ValueCheck("target type", startType, startType));

        // Check values
        valueChecks.stream()
          .map(check -> check.checkAndGetKeyRanges(jdb, startType, "query on composite index `" + indexName + "'"))
          .forEach(this.filters::add);
    }

    private Set<TypeToken<?>> getTypeTokens(JSimpleDB jdb, Class<?> context, int storageId) {
        return this.getTypeTokens(jdb, context, storageId, 0);
    }

    private Set<TypeToken<?>> getTypeTokens(JSimpleDB jdb, Class<?> context, int storageId, int parentStorageId) {
        final HashSet<TypeToken<?>> contextFieldTypes = new HashSet<>();
        for (JClass<?> jclass : jdb.jclasses.values()) {

            // Check if jclass is under consideration
            if (!context.isAssignableFrom(jclass.type))
                continue;

            // Find the simple field field in jclass, if it exists
            JSimpleField simpleField = null;
            if (parentStorageId != 0) {
                final JComplexField parentField = (JComplexField)jclass.jfields.get(parentStorageId);
                if (parentField != null)
                    simpleField = parentField.getSubField(storageId);
            } else
                simpleField = (JSimpleField)jclass.jfields.get(storageId);
            if (simpleField == null)
                continue;

            // Add field's type in jclass
            contextFieldTypes.add(simpleField.typeToken);
        }
        if (contextFieldTypes.isEmpty()) {
            throw new IllegalArgumentException("no sub-type of " + context
              + " contains and indexed simple field with storage ID " + storageId);
        }
        return Util.findLowestCommonAncestors(contextFieldTypes);
    }

    private Set<Class<?>> wrapRaw(Set<TypeToken<?>> typeTokens) {
        final HashSet<Class<?>> classes = new HashSet<>(typeTokens.size());
        for (TypeToken<?> typeToken : typeTokens)
            classes.add(typeToken.wrap().getRawType());
        return classes;
    }

    private static CompositeIndexInfo findCompositeIndex(JSimpleDB jdb, Class<?> startType, String indexName, int numValues) {
        CompositeIndexInfo indexInfo = null;
        for (JClass<?> jclass : jdb.getJClasses(startType)) {
            final JCompositeIndex index = jclass.jcompositeIndexesByName.get(indexName);
            if (index != null) {
                final CompositeIndexInfo candidate = jdb.getIndexInfo(index.storageId, CompositeIndexInfo.class);
                if (indexInfo != null && !candidate.equals(indexInfo)) {
                    throw new IllegalArgumentException("ambiguous composite index name `" + indexName
                      + "': multiple incompatible composite indexes with that name exist on sub-types of " + startType.getName());
                }
                indexInfo = candidate;
            }
        }
        if (indexInfo == null) {
            throw new IllegalArgumentException("no composite index named `" + indexName
              + "' exists on any sub-type of " + startType.getName());
        }
        if (numValues != indexInfo.getFieldTypes().size()) {
            throw new IllegalArgumentException("composite index `" + indexName
              + "' on " + startType.getName() + " has " + indexInfo.getFieldTypes().size() + " fields, not " + numValues);
        }
        return indexInfo;
    }

// Public Methods

    public <V, T> CoreIndex<V, T> applyFilters(CoreIndex<V, T> index) {
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
        return "IndexQueryInfo"
          + "[startType=" + this.startType
          + ",indexInfo=" + this.indexInfo
          + ",filters=" + this.filters + "]";
    }

// ValueCheck

    private static class ValueCheck {

        private final String description;
        private final Class<?> actualType;
        private final Set<Class<?>> expectedTypes;
        private final boolean reference;
        private final boolean matchNull;

        // Primary constructor
        ValueCheck(String description, Class<?> actualType, Set<Class<?>> expectedTypes, boolean reference, boolean matchNull) {
            this.description = description;
            this.actualType = actualType;
            this.expectedTypes = expectedTypes;
            this.reference = reference;
            this.matchNull = matchNull;
        }

        // Constructor for indexed fields
        ValueCheck(String description, Class<?> actualType, Set<Class<?>> expectedTypes, FieldType<?> fieldType) {
            this(description, actualType, expectedTypes, fieldType instanceof ReferenceFieldType, true);
        }

        // Constructor for target type (simple index)
        ValueCheck(String description, Class<?> actualType, Set<Class<?>> expectedTypes) {
            this(description, actualType, expectedTypes, true, false);
        }

        // Constructor for target type (composite index)
        ValueCheck(String description, Class<?> actualType, Class<?> expectedType) {
            this(description, actualType, Collections.<Class<?>>singleton(expectedType));
        }

        public KeyRanges checkAndGetKeyRanges(JSimpleDB jdb, Class<?> startType, String queryDescription) {

            // Check whether actual type matches expected type
            boolean match = this.expectedTypes.contains(this.actualType);
            if (!match && this.reference) {

                // For reference type, we allow matching any sub-type or super-type
                for (Class<?> expectedType : this.expectedTypes) {
                    if (expectedType.isAssignableFrom(this.actualType) || this.actualType.isAssignableFrom(expectedType)) {
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
                throw new IllegalArgumentException("invalid " + this.description + " " + actualType.getName()
                  + " for " + queryDescription + " in " + startType + ": should be "
                  + (this.reference ? "a super-type or sub-type of " : "") + expectedTypesDescription);
            }

            // For non reference fields, we don't have any restrictions on 'type'
            if (!this.reference)
                return null;

            // If actual type includes all JObject's no filter is necessary
            if (actualType.isAssignableFrom(JObject.class))
                return null;

            // Create a filter for the actual type
            final KeyRanges filter = jdb.keyRangesFor(this.actualType);

            // For values other than the target value, we need to also accept null values in the index
            if (this.matchNull)
                filter.add(NULL_RANGE);

            // Done
            return filter;
        }

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
}

