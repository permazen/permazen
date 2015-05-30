
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSetField;

/**
 * Scans for {@link JSetField &#64;JSetField} annotations.
 */
class JSetFieldScanner<T> extends AbstractFieldScanner<T, JSetField> {

    JSetFieldScanner(JClass<T> jclass, boolean autogenFields) {
        super(jclass, JSetField.class, autogenFields);
    }

    @Override
    protected JSetField getDefaultAnnotation() {
        return new JSetField() {
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
                return JFieldScanner.DEFAULT_JFIELD;
            }
        };
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
        return super.isAutoPropertyCandidate(method) && Set.class.isAssignableFrom(method.getReturnType());
    }
}

