
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.FollowPath;
import io.permazen.annotation.JField;
import io.permazen.annotation.JSetField;
import io.permazen.annotation.PermazenType;

import java.lang.reflect.Method;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;

/**
 * Scans for {@link JSetField &#64;JSetField} annotations.
 */
class JSetFieldScanner<T> extends AbstractFieldScanner<T, JSetField> {

    JSetFieldScanner(JClass<T> jclass, PermazenType permazenType) {
        super(jclass, JSetField.class, permazenType);
    }

    @Override
    protected JSetField getDefaultAnnotation() {
        return new DefaultJSetField(this.permazenType);
    }

    @Override
    protected boolean includeMethod(Method method, JSetField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, Set.class, SortedSet.class, NavigableSet.class);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        return super.isAutoPropertyCandidate(method)
          && Set.class.isAssignableFrom(method.getReturnType())
          && Util.getAnnotation(method, FollowPath.class) == null;
    }

// DefaultJSetField

    private static class DefaultJSetField implements JSetField {

        private PermazenType permazenType;

        DefaultJSetField(PermazenType permazenType) {
            this.permazenType = permazenType;
        }

        @Override
        public Class<JSetField> annotationType() {
            return JSetField.class;
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
            return JFieldScanner.getDefaultJField(this.permazenType);
        }
    }
}

