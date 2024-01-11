
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenType;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

/**
 * Scans for {@link PermazenMapField &#64;PermazenMapField} annotations.
 */
class PermazenMapFieldScanner<T> extends AbstractPermazenFieldScanner<T, PermazenMapField> {

    PermazenMapFieldScanner(PermazenClass<T> pclass, PermazenType permazenType) {
        super(pclass, PermazenMapField.class, permazenType);
    }

    @Override
    protected PermazenMapField getDefaultAnnotation() {
        return new DefaultPermazenMapField(this.permazenType);
    }

    @Override
    protected boolean includeMethod(Method method, PermazenMapField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, Map.class, SortedMap.class, NavigableMap.class);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        return super.isAutoPropertyCandidate(method) && Map.class.isAssignableFrom(method.getReturnType());
    }

// DefaultPermazenMapField

    private static class DefaultPermazenMapField implements PermazenMapField {

        private PermazenType permazenType;

        DefaultPermazenMapField(PermazenType permazenType) {
            this.permazenType = permazenType;
        }

        @Override
        public Class<PermazenMapField> annotationType() {
            return PermazenMapField.class;
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
        public PermazenField key() {
            return PermazenFieldScanner.getDefaultPermazenField(this.permazenType);
        }
        @Override
        public PermazenField value() {
            return PermazenFieldScanner.getDefaultPermazenField(this.permazenType);
        }
    }
}
