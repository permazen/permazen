
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

// ChangeMethodInfo

    class IndexMethodInfo extends MethodInfo {

        final TypeToken<?> targetType;
        final JSimpleField targetField;
        final JComplexField targetSuperField;
        final int queryType;

        @SuppressWarnings("unchecked")
        IndexMethodInfo(Method method, IndexQuery annotation) {
            super(method, annotation);

            // Get start type
            TypeToken<?> startType = TypeToken.of(method.getDeclaringClass());
            if (annotation.startType() != void.class) {
                if (annotation.startType().isPrimitive() || annotation.startType().isArray()) {
                    throw new IllegalArgumentException(IndexQueryScanner.this.getErrorPrefix(method)
                      + "invalid startType() " + annotation.startType());
                }
                startType = TypeToken.of(annotation.startType());
            }

            // Parse reference path
            final ReferencePath path;
            try {
                path = new ReferencePath(IndexQueryScanner.this.jclass.jdb, startType, annotation.value(), true);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(IndexQueryScanner.this.getErrorPrefix(method) + e.getMessage(), e);
            }
            if (path.getReferenceFields().length > 0) {
                throw new IllegalArgumentException(IndexQueryScanner.this.getErrorPrefix(method) + "reference path `"
                  + annotation.value() + "' contains " + path.getReferenceFields().length + " intermediate reference(s)");
            }

            // Verify target field is simple
            if (!(path.targetField instanceof JSimpleField)) {
                throw new IllegalArgumentException(IndexQueryScanner.this.getErrorPrefix(method)
                  + path.targetField + " does not support indexing; it is not a simple field");
            }

            // Get target object, field, and complex super-field (if any)
            this.targetType = path.targetType;
            this.targetField = (JSimpleField)path.targetField;
            this.targetSuperField = path.targetSuperField;

            // Verify the field is actually indexed
            if (!this.targetField.indexed) {
                throw new IllegalArgumentException(IndexQueryScanner.this.getErrorPrefix(method)
                  + this.targetField + " is not indexed");
            }

            // Validate the method's return type
            final ArrayList<TypeToken<?>> indexReturnTypes = new ArrayList<TypeToken<?>>();
            try {
                this.targetField.addIndexReturnTypes(indexReturnTypes, this.targetType);
            } catch (UnsupportedOperationException e) {
                throw new IllegalArgumentException(IndexQueryScanner.this.getErrorPrefix(method)
                  + "indexing is not supported for " + this.targetField, e);
            }
            if (this.targetSuperField != null)
                this.targetSuperField.addIndexEntryReturnTypes(indexReturnTypes, this.targetType, this.targetField);
            IndexQueryScanner.this.checkReturnType(method, indexReturnTypes);

            // Determine the query type (normal object query or some kind of index entry query) from method return type
            final TypeToken<?> queryObjectType = Util.getTypeParameter(
              Util.getTypeParameter(TypeToken.of(method.getGenericReturnType()), 1), 0);
            this.queryType = this.targetSuperField != null ? this.targetSuperField.getIndexEntryQueryType(queryObjectType) : 0;
        }
    }
}

