
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.util.AnnotationScanner;

/**
 * Scans for {@link JField &#64;JField} annotations.
 */
class JFieldScanner<T> extends AnnotationScanner<T, JField> {

    JFieldScanner(JClass<T> jclass) {
        super(jclass, JField.class);
    }

    @Override
    protected boolean includeMethod(Method method, JField annotation) {
        this.checkNotStatic(method);
        this.checkParameterTypes(method);
        return true;
    }
}

