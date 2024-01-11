
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnSchemaChange;
import io.permazen.schema.SchemaId;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map;

/**
 * Scans for {@link OnSchemaChange &#64;OnSchemaChange} annotations.
 */
class OnSchemaChangeScanner<T> extends AnnotationScanner<T, OnSchemaChange> {

    private final TypeToken<Map<String, Object>> oldValuesParamType;

    @SuppressWarnings("serial")
    OnSchemaChangeScanner(PermazenClass<T> pclass) {
        super(pclass, OnSchemaChange.class);
        this.oldValuesParamType = new TypeToken<Map<String, Object>>() { };
    }

    @Override
    protected boolean includeMethod(Method method, OnSchemaChange annotation) {

        // Check method type
        this.checkNotStatic(method);
        this.checkReturnType(method, void.class);

        // Check parameter types
        final ArrayList<TypeToken<?>> paramTypes = new ArrayList<TypeToken<?>>(3);
        paramTypes.add(this.oldValuesParamType);
        paramTypes.add(TypeToken.of(SchemaId.class));
        paramTypes.add(TypeToken.of(SchemaId.class));
        this.checkParameterTypes(method, paramTypes.subList(0, Math.min(method.getParameterTypes().length, 3)));

        // Done
        return true;
    }

    @Override
    protected SchemaChangeMethodInfo createMethodInfo(Method method, OnSchemaChange annotation) {
        return new SchemaChangeMethodInfo(method, annotation);
    }

// SchemaChangeMethodInfo

    class SchemaChangeMethodInfo extends MethodInfo {

        @SuppressWarnings("unchecked")
        SchemaChangeMethodInfo(Method method, OnSchemaChange annotation) {
            super(method, annotation);
        }

        // Invoke method
        void invoke(PermazenObject pobj, Map<String, Object> oldValues, SchemaId oldSchemaId, SchemaId newSchemaId) {

            // Get method info
            final Method method = this.getMethod();

            // Figure out method parameters and invoke method
            switch (method.getParameterTypes().length) {
            case 1:
                Util.invoke(method, pobj, oldValues);
                break;
            case 2:
                Util.invoke(method, pobj, oldValues, oldSchemaId);
                break;
            case 3:
                Util.invoke(method, pobj, oldValues, oldSchemaId, newSchemaId);
                break;
            default:
                throw new RuntimeException("internal error");
            }
        }
    }
}
