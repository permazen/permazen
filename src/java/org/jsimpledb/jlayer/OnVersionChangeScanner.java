
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map;

import org.jsimpledb.annotation.OnVersionChange;

/**
 * Scans for {@link OnVersionChange &#64;OnVersionChange} annotations.
 */
class OnVersionChangeScanner<T> extends AnnotationScanner<T, OnVersionChange> {

    OnVersionChangeScanner(JClass<T> jclass) {
        super(jclass, OnVersionChange.class);
    }

    @Override
    @SuppressWarnings("serial")
    protected boolean includeMethod(Method method, OnVersionChange annotation) {

        // Sanity check annotation
        if (annotation.oldVersion() < 0)
            throw new IllegalArgumentException("@" + this.annotationType.getSimpleName() + " has illegal negative oldVersion");
        if (annotation.newVersion() < 0)
            throw new IllegalArgumentException("@" + this.annotationType.getSimpleName() + " has illegal negative newVersion");

        // Check method types
        this.checkNotStatic(method);
        this.checkReturnType(method, void.class);
        final ArrayList<TypeToken<?>> types = new ArrayList<TypeToken<?>>(3);
        if (annotation.oldVersion() == 0)
            types.add(TypeToken.of(int.class));
        if (annotation.newVersion() == 0)
            types.add(TypeToken.of(int.class));
        types.add(new TypeToken<Map<Integer, Object>>() { });
        this.checkParameterTypes(method, types);

        // Done
        return true;
    }
}

