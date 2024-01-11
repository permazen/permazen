
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenType;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Scans for {@link PermazenListField &#64;PermazenListField} annotations.
 */
class PermazenListFieldScanner<T> extends AbstractPermazenFieldScanner<T, PermazenListField> {

    PermazenListFieldScanner(PermazenClass<T> pclass, PermazenType permazenType) {
        super(pclass, PermazenListField.class, permazenType);
    }

    @Override
    protected PermazenListField getDefaultAnnotation() {
        return new DefaultPermazenListField(this.permazenType);
    }

    @Override
    protected boolean includeMethod(Method method, PermazenListField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, List.class);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        return super.isAutoPropertyCandidate(method) && List.class.isAssignableFrom(method.getReturnType());
    }

// DefaultPermazenListField

    private static class DefaultPermazenListField implements PermazenListField {

        private PermazenType permazenType;

        DefaultPermazenListField(PermazenType permazenType) {
            this.permazenType = permazenType;
        }

        @Override
        public Class<PermazenListField> annotationType() {
            return PermazenListField.class;
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
        public PermazenField element() {
            return PermazenFieldScanner.getDefaultPermazenField(this.permazenType);
        }
    }
}
