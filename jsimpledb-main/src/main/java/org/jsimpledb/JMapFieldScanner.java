
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSimpleClass;

/**
 * Scans for {@link JMapField &#64;JMapField} annotations.
 */
class JMapFieldScanner<T> extends AbstractFieldScanner<T, JMapField> {

    JMapFieldScanner(JClass<T> jclass, JSimpleClass jsimpleClass) {
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

        private JSimpleClass jsimpleClass;

        DefaultJMapField(JSimpleClass jsimpleClass) {
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
