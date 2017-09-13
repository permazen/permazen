
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JListField;
import io.permazen.annotation.PermazenType;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Scans for {@link JListField &#64;JListField} annotations.
 */
class JListFieldScanner<T> extends AbstractFieldScanner<T, JListField> {

    JListFieldScanner(JClass<T> jclass, PermazenType jsimpleClass) {
        super(jclass, JListField.class, jsimpleClass);
    }

    @Override
    protected JListField getDefaultAnnotation() {
        return new DefaultJListField(this.jsimpleClass);
    }

    @Override
    protected boolean includeMethod(Method method, JListField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, List.class);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        return super.isAutoPropertyCandidate(method) && List.class.isAssignableFrom(method.getReturnType());
    }

// DefaultJListField

    private static class DefaultJListField implements JListField {

        private PermazenType jsimpleClass;

        DefaultJListField(PermazenType jsimpleClass) {
            this.jsimpleClass = jsimpleClass;
        }

        @Override
        public Class<JListField> annotationType() {
            return JListField.class;
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
        public JField element() {
            return JFieldScanner.getDefaultJField(this.jsimpleClass);
        }
    }
}
