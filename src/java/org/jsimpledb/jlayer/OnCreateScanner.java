
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import java.lang.reflect.Method;

import org.jsimpledb.annotation.OnCreate;

/**
 * Scans for {@link OnCreate &#64;OnCreate} annotations.
 */
class OnCreateScanner<T> extends AnnotationScanner<T, OnCreate> {

    OnCreateScanner(JClass<T> jclass) {
        super(jclass, OnCreate.class);
    }

    @Override
    protected boolean includeMethod(Method method, OnCreate annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, void.class);
        this.checkParameterTypes(method);
        return true;
    }
}

