
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.List;

import org.jsimpledb.annotation.JListField;
import org.jsimpledb.util.AnnotationScanner;

/**
 * Scans for {@link JListField &#64;JListField} annotations.
 */
class JListFieldScanner<T> extends AnnotationScanner<T, JListField> {

    JListFieldScanner(JClass<T> jclass) {
        super(jclass, JListField.class);
    }

    @Override
    protected boolean includeMethod(Method method, JListField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, List.class);
        this.checkParameterTypes(method);
        return true;
    }
}

