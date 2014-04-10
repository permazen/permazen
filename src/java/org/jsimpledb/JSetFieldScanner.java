
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;

import org.jsimpledb.annotation.JSetField;

/**
 * Scans for {@link JSetField &#64;JSetField} annotations.
 */
class JSetFieldScanner<T> extends AnnotationScanner<T, JSetField> {

    JSetFieldScanner(JClass<T> jclass) {
        super(jclass, JSetField.class);
    }

    @Override
    protected boolean includeMethod(Method method, JSetField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, Set.class, SortedSet.class, NavigableSet.class);
        this.checkParameterTypes(method);
        return true;
    }
}

