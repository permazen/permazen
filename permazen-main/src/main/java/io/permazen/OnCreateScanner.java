
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnCreate;

import java.lang.reflect.Method;

/**
 * Scans for {@link OnCreate &#64;OnCreate} annotations.
 */
class OnCreateScanner<T> extends AnnotationScanner<T, OnCreate> {

    OnCreateScanner(PermazenClass<T> pclass) {
        super(pclass, OnCreate.class);
    }

    @Override
    protected boolean includeMethod(Method method, OnCreate annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, void.class);
        this.checkParameterTypes(method);
        return true;
    }
}
