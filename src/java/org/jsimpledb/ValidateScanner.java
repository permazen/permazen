
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.lang.reflect.Method;

import org.jsimpledb.annotation.Validate;
import org.jsimpledb.util.AnnotationScanner;

/**
 * Scans for {@link Validate &#64;Validate} annotations.
 */
class ValidateScanner<T> extends AnnotationScanner<T, Validate> {

    ValidateScanner(JClass<T> jclass) {
        super(jclass, Validate.class);
    }

    @Override
    protected boolean includeMethod(Method method, Validate annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, void.class);
        this.checkParameterTypes(method);
        return true;
    }
}

