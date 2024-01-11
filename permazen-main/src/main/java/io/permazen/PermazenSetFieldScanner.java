
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.annotation.ReferencePath;

import java.lang.reflect.Method;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;

/**
 * Scans for {@link PermazenSetField &#64;PermazenSetField} annotations.
 */
class PermazenSetFieldScanner<T> extends AbstractPermazenFieldScanner<T, PermazenSetField> {

    PermazenSetFieldScanner(PermazenClass<T> pclass, PermazenType permazenType) {
        super(pclass, PermazenSetField.class, permazenType);
    }

    @Override
    protected PermazenSetField getDefaultAnnotation() {
        return new DefaultPermazenSetField(this.permazenType);
    }

    @Override
    protected boolean includeMethod(Method method, PermazenSetField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, Set.class, SortedSet.class, NavigableSet.class);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        return super.isAutoPropertyCandidate(method)
          && Set.class.isAssignableFrom(method.getReturnType())
          && Util.getAnnotation(method, ReferencePath.class) == null;
    }

// DefaultPermazenSetField

    private static class DefaultPermazenSetField implements PermazenSetField {

        private PermazenType permazenType;

        DefaultPermazenSetField(PermazenType permazenType) {
            this.permazenType = permazenType;
        }

        @Override
        public Class<PermazenSetField> annotationType() {
            return PermazenSetField.class;
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
