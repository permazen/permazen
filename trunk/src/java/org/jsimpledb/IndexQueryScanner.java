
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.ArrayList;

import org.jsimpledb.annotation.IndexQuery;
import org.jsimpledb.util.AnnotationScanner;

/**
 * Scans for {@link IndexQuery &#64;IndexQuery} annotations.
 */
class IndexQueryScanner<T> extends AnnotationScanner<T, IndexQuery> {

    IndexQueryScanner(JClass<T> jclass) {
        super(jclass, IndexQuery.class);
    }

    @Override
    protected boolean includeMethod(Method method, IndexQuery annotation) {
        this.checkNotStatic(method);
        this.checkParameterTypes(method);
        return true;                                    // we check return type in IndexMethodInfo
    }

    @Override
    protected IndexMethodInfo createMethodInfo(Method method, IndexQuery annotation) {
        return new IndexMethodInfo(method, annotation);
    }

// IndexMethodInfo

    class IndexMethodInfo extends MethodInfo {

        final IndexInfo indexInfo;
        final int queryType;

        @SuppressWarnings("unchecked")
        IndexMethodInfo(Method method, IndexQuery annotation) {
            super(method, annotation);

            // Get index info
            try {
                this.indexInfo = new IndexInfo(IndexQueryScanner.this.jclass.jdb,
                  annotation.type() != void.class ? annotation.type() : method.getDeclaringClass(),
                  annotation.value());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(IndexQueryScanner.this.getErrorPrefix(method) + e.getMessage(), e);
            }

            // Check method's return type
            IndexQueryScanner.this.checkReturnType(method, this.indexInfo.indexReturnTypes);

            // Determine the query type (normal object query or some kind of index entry query) from method return type
            final TypeToken<?> queryObjectType = Util.getTypeParameter(
              Util.getTypeParameter(TypeToken.of(method.getGenericReturnType()), 1), 0);
            this.queryType = this.indexInfo.targetSuperField != null ?
              this.indexInfo.targetSuperField.getIndexEntryQueryType(queryObjectType) : 0;
        }
    }

// IndexInfo

    static class IndexInfo {

        final TypeToken<?> type;
        final TypeToken<?> targetType;
        final JSimpleField targetField;
        final TypeToken<?> targetReferenceType;
        final JComplexField targetSuperField;
        final ArrayList<TypeToken<?>> indexReturnTypes = new ArrayList<TypeToken<?>>();

        IndexInfo(JSimpleDB jdb, Class<?> type, String fieldName) {

            // Sanity check
            if (jdb == null)
                throw new IllegalArgumentException("null jdb");
            if (type == null)
                throw new IllegalArgumentException("null type");
            if (fieldName == null)
                throw new IllegalArgumentException("null fieldName");

            // Get start type
            if (type.isPrimitive() || type.isArray())
                throw new IllegalArgumentException("invalid type " + type);
            this.type = Util.getWildcardedType(type);

            // Parse reference path
            final ReferencePath path = jdb.parseReferencePath(this.type, fieldName, true);
            if (path.getReferenceFields().length > 0)
                throw new IllegalArgumentException("invalid field name `" + fieldName + "': contains intermediate reference(s)");

            // Verify target field is simple
            if (!(path.targetField instanceof JSimpleField))
                throw new IllegalArgumentException(path.targetField + " does not support indexing; it is not a simple field");

            // Get target object, field, and complex super-field (if any)
            this.targetType = path.targetType;
            this.targetField = (JSimpleField)path.targetField;
            this.targetReferenceType = path.targetReferenceType;
            this.targetSuperField = path.targetSuperField;

            // Verify the field is actually indexed
            if (!this.targetField.indexed)
                throw new IllegalArgumentException(this.targetField + " is not indexed");

            // Get value type
            final TypeToken<?> valueType = this.targetField instanceof JReferenceField ?
              this.targetReferenceType : this.targetField.typeToken;

            // Get valid index return types for this field
            try {
                this.targetField.addIndexReturnTypes(this.indexReturnTypes, this.type, valueType);
            } catch (UnsupportedOperationException e) {
                throw new IllegalArgumentException("indexing is not supported for " + this.targetField, e);
            }

            // If field is a complex sub-field, determine add complex index entry type(s)
            if (this.targetSuperField != null)
                this.targetSuperField.addIndexEntryReturnTypes(this.indexReturnTypes, this.type, this.targetField, valueType);
        }
    }
}

