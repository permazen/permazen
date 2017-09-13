
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.PermazenType;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

/**
 * Scans for {@link JMapField &#64;JMapField} annotations.
 */
class JMapFieldScanner<T> extends AbstractFieldScanner<T, JMapField> {

    JMapFieldScanner(JClass<T> jclass, PermazenType jsimpleClass) {
        super(jclass, JMapField.class, jsimpleClass);
    }

    @Override
    protected JMapField getDefaultAnnotation() {
        return new DefaultJMapField(this.jsimpleClass);
    }

    @Override
    protected boolean includeMethod(Method method, JMapField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, Map.class, SortedMap.class, NavigableMap.class);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        return super.isAutoPropertyCandidate(method) && Map.class.isAssignableFrom(method.getReturnType());
    }

// DefaultJMapField

    private static class DefaultJMapField implements JMapField {

        private PermazenType jsimpleClass;

        DefaultJMapField(PermazenType jsimpleClass) {
            this.jsimpleClass = jsimpleClass;
        }

        @Override
        public Class<JMapField> annotationType() {
            return JMapField.class;
        }
        @Override
        public String name() {
            return "";
        }
        @Override
        public int storageId() {
            return 0;
        }
        @Override
        public JField key() {
            return JFieldScanner.getDefaultJField(this.jsimpleClass);
        }
        @Override
        public JField value() {
            return JFieldScanner.getDefaultJField(this.jsimpleClass);
        }
    }
}
