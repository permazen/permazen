
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnValidate;

import java.lang.reflect.Method;

/**
 * Scans for {@link OnValidate &#64;OnValidate} annotations.
 */
class OnValidateScanner<T> extends AnnotationScanner<T, OnValidate> {

    OnValidateScanner(PermazenClass<T> pclass) {
        super(pclass, OnValidate.class);
    }

    @Override
    protected boolean includeMethod(Method method, OnValidate annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, void.class);
        this.checkParameterTypes(method);
        return true;
    }
}
