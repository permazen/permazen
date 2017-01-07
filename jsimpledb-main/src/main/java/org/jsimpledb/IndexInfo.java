
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
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;

/**
 * Information used for index queries.
 */
class IndexInfo {

    private static final KeyRange NULL_RANGE = new KeyRange(new byte[] { (byte)0xff }, null);

    // For simple indexes only
    final JSimpleFieldInfo fieldInfo;
    final JComplexFieldInfo superFieldInfo;

    // For composite indexes only
    final JCompositeIndexInfo indexInfo;

    private final Class<?> startType;
    private final ArrayList<KeyRanges> filters = new ArrayList<>();

// Constructors

    // Constructor for regular simple index queries
    IndexInfo(JSimpleDB jdb, Class<?> startType, String fieldName, Class<?> valueType) {
        this(jdb, startType, fieldName, valueType, null);
    }

    // Primary constructor (keyType is null except for map value simple index queries)
    IndexInfo(JSimpleDB jdb, Class<?> startType, String fieldName, Class<?> valueType, Class<?> keyType) {

        // Sanity check
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(fieldName != null, "null fieldName");
        Preconditions.checkArgument(startType != null, "null startType");
        Preconditions.checkArgument(valueType != null, "null valueType");
        this.indexInfo = null;

        // Get start type
        Preconditions.checkArgument(!startType.isPrimitive() && !startType.isArray(), "invalid startType " + startType);
        this.startType = startType;

        // Parse reference path
        final ReferencePath path = jdb.parseReferencePath(this.startType, fieldName, true);
        if (path.getReferenceFields().length > 0)
            throw new IllegalArgumentException("invalid field name `" + fieldName + "': contains intermediate reference(s)");

        // Verify target field is simple
        if (!(path.targetFieldInfo instanceof JSimpleFieldInfo)) {
            String message = path.targetFieldInfo + " does not support indexing: it is not a simple field";
            if (path.targetFieldInfo instanceof JSetFieldInfo || path.targetFieldInfo instanceof JListFieldInfo)
                message += "; perhaps you meant `" + fieldName + ".element'?";
            else if (path.targetFieldInfo instanceof JMapFieldInfo)
                message += "; perhaps you meant `" + fieldName + ".key' or `" + fieldName + ".value'?";
            throw new IllegalArgumentException(message);
        }

        // Get target object, field, and complex super-field (if any)
        this.fieldInfo = (JSimpleFieldInfo)path.targetFieldInfo;
        this.superFieldInfo = path.targetSuperFieldInfo;

        // Verify the field is actually indexed
        if (!this.fieldInfo.isIndexed())
            throw new IllegalArgumentException(this.fieldInfo + " is not an indexed field");

        // Verify value type
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(3);
        valueChecks.add(new ValueCheck("value type", valueType, this.wrapRaw(path.getTargetFieldTypes()), this.fieldInfo));

        // Verify target type
        valueChecks.add(new ValueCheck("target type", startType, path.targetTypes));

        // We should only ever see 'keyType' when field is a map value field
        if (keyType != null) {
            final JMapFieldInfo mapInfo = (JMapFieldInfo)this.superFieldInfo;
            final JSimpleFieldInfo keyInfo = mapInfo.getKeyFieldInfo();
            assert this.fieldInfo.equals(mapInfo.getValueFieldInfo());
            valueChecks.add(new ValueCheck("map key type", keyType,
              this.wrapRaw(keyInfo.getTypeTokens(this.startType)), keyInfo));
        }

        // Check values
        valueChecks.stream()
          .map(check -> check.checkAndGetKeyRanges(jdb, startType, "index query on field `" + fieldName + "'"))
          .forEach(this.filters::add);
    }

    // Constructor for composite index queries
    IndexInfo(JSimpleDB jdb, Class<?> startType, String indexName, Class<?>... valueTypes) {

        // Sanity check
        Preconditions.checkArgument(jdb != null, "null jdb");
        Preconditions.checkArgument(indexName != null, "null indexName");
        Preconditions.checkArgument(valueTypes != null, "null valueTypes");
        this.fieldInfo = null;
        this.superFieldInfo = null;

        // Get start type
        Preconditions.checkArgument(!startType.isPrimitive() && !startType.isArray(), "invalid startType " + startType);
        this.startType = startType;

        // Find index
        this.indexInfo = IndexInfo.findCompositeIndex(jdb, startType, indexName, valueTypes.length);

        // Verify field types
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(valueTypes.length + 1);
        for (int i = 0; i < valueTypes.length; i++) {
            final Class<?> valueType = valueTypes[i];
            final JSimpleFieldInfo jfieldInfo = indexInfo.jfieldInfos.get(i);
            valueChecks.add(new ValueCheck("value type #" + (i + 1), valueType,
              this.wrapRaw(jfieldInfo.getTypeTokens(this.startType)), jfieldInfo));
        }

        // Verify target type
        valueChecks.add(new ValueCheck("target type", startType, startType));

        // Check values
        valueChecks.stream()
          .map(check -> check.checkAndGetKeyRanges(jdb, startType, "query on composite index `" + indexName + "'"))
          .forEach(this.filters::add);
    }

    private Set<Class<?>> wrapRaw(Set<TypeToken<?>> typeTokens) {
        final HashSet<Class<?>> classes = new HashSet<>(typeTokens.size());
        for (TypeToken<?> typeToken : typeTokens)
            classes.add(typeToken.wrap().getRawType());
        return classes;
    }

    private static JCompositeIndexInfo findCompositeIndex(JSimpleDB jdb, Class<?> startType, String indexName, int numValues) {
        JCompositeIndexInfo indexInfo = null;
        for (JClass<?> jclass : jdb.getJClasses(startType)) {
            final JCompositeIndex index = jclass.jcompositeIndexesByName.get(indexName);
            if (index != null) {
                final JCompositeIndexInfo candidate = jdb.jcompositeIndexInfos.get(index.storageId);
                if (indexInfo != null && !candidate.equals(indexInfo)) {
                    throw new IllegalArgumentException("ambiguous composite index name `" + indexName
                      + "': multiple composite indexes with that name exist on sub-types of " + startType.getName());
                }
                indexInfo = candidate;
            }
        }
        if (indexInfo == null) {
            throw new IllegalArgumentException("no composite index named `" + indexName
              + "' exists on any sub-type of " + startType.getName());
        }
        if (numValues != indexInfo.jfieldInfos.size()) {
            throw new IllegalArgumentException("composite index `" + indexName
              + "' on " + startType.getName() + " has " + indexInfo.jfieldInfos.size() + " fields, not " + numValues);
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
        return "IndexInfo"
          + "[startType=" + this.startType
          + (this.fieldInfo != null ? ",fieldInfo=" + this.fieldInfo : "")
          + (this.superFieldInfo != null ? ",superFieldInfo=" + this.superFieldInfo : "")
          + (this.indexInfo != null ? ",indexInfo=" + this.indexInfo : "")
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
        ValueCheck(String description, Class<?> actualType, Set<Class<?>> expectedTypes, JSimpleFieldInfo fieldInfo) {
            this(description, actualType, expectedTypes, fieldInfo instanceof JReferenceFieldInfo, true);
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

