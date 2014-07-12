
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.util.AnnotationScanner;

/**
 * Scans for {@link JMapField &#64;JMapField} annotations.
 */
class JMapFieldScanner<T> extends AnnotationScanner<T, JMapField> {

    JMapFieldScanner(JClass<T> jclass) {
        super(jclass, JMapField.class);
    }

    @Override
    protected boolean includeMethod(Method method, JMapField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, Map.class, SortedMap.class, NavigableMap.class);
        this.checkParameterTypes(method);
        return true;
    }
}

