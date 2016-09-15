
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;

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
        if (!(path.targetFieldInfo instanceof JSimpleFieldInfo))
            throw new IllegalArgumentException(path.targetFieldInfo + " does not support indexing; it is not a simple field");

        // Get target object, field, and complex super-field (if any)
        this.fieldInfo = (JSimpleFieldInfo)path.targetFieldInfo;
        this.superFieldInfo = path.targetSuperFieldInfo;

        // Verify the field is actually indexed
        if (!this.fieldInfo.isIndexed())
            throw new IllegalArgumentException(this.fieldInfo + " is not indexed");

        // Verify value type
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(3);
        valueChecks.add(new ValueCheck("value type", valueType, path.getTargetFieldType(), this.fieldInfo, true));

        // Verify target type
        valueChecks.add(new ValueCheck("target type", startType, path.targetType));

        // We should only ever see 'keyType' when field is a map value field
        if (keyType != null) {
            final JMapFieldInfo mapInfo = (JMapFieldInfo)this.superFieldInfo;
            final JSimpleFieldInfo keyInfo = mapInfo.getKeyFieldInfo();
            assert this.fieldInfo.equals(mapInfo.getValueFieldInfo());
            valueChecks.add(new ValueCheck("map key type", keyType, keyInfo.getTypeToken(this.startType), keyInfo, true));
        }

        // Check values
        for (ValueCheck check : valueChecks)
            this.filters.add(check.checkAndGetKeyRanges(jdb, startType, "index query on field `" + fieldName + "'"));
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
        final ArrayList<ValueCheck> valueChecks = new ArrayList<>(3);
        for (int i = 0; i < valueTypes.length; i++) {
            final Class<?> valueType = valueTypes[i];
            final JSimpleFieldInfo jfieldInfo = indexInfo.jfieldInfos.get(i);
            valueChecks.add(new ValueCheck("value type #" + (i + 1), valueType,
              jfieldInfo.getTypeToken(this.startType), jfieldInfo instanceof JReferenceFieldInfo, true));
        }

        // Verify target type
        valueChecks.add(new ValueCheck("target type", startType, startType));

        // Check values
        for (ValueCheck check : valueChecks)
            this.filters.add(check.checkAndGetKeyRanges(jdb, startType, "query on composite index `" + indexName + "'"));
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
        private final Class<?> expectedType;
        private final boolean reference;
        private final boolean matchNull;

        ValueCheck(String description,
          Class<?> actualType, TypeToken<?> expectedType, boolean reference, boolean matchNull) {
            this.description = description;
            this.actualType = actualType;
            this.expectedType = expectedType.wrap().getRawType();
            this.reference = reference;
            this.matchNull = matchNull;
        }

        // Constructor for indexed fields
        ValueCheck(String description,
          Class<?> actualType, TypeToken<?> expectedType, JSimpleFieldInfo fieldInfo, boolean matchNull) {
            this(description, actualType, expectedType, fieldInfo instanceof JReferenceFieldInfo, matchNull);
        }

        // Constructor for target type
        ValueCheck(String description, Class<?> actualType, Class<?> expectedType) {
            this(description, actualType, TypeToken.of(expectedType), true, false);
        }

        public KeyRanges checkAndGetKeyRanges(JSimpleDB jdb, Class<?> startType, String queryDescription) {

            // Check expected type
            final boolean equal = this.expectedType.equals(this.actualType);
            final boolean comparable = equal
              || this.expectedType.isAssignableFrom(this.actualType) || this.actualType.isAssignableFrom(this.expectedType);
            if (!(this.reference ? comparable : equal)) {
                throw new IllegalArgumentException("invalid " + this.description + " " + actualType.getName()
                  + " for " + queryDescription + " in " + startType + ": should be "
                  + (this.reference ? "a super-type or sub-type of " : "") + this.expectedType.getName());
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
    }
}

