
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.Component;

import java.lang.reflect.Method;

import org.jsimpledb.util.AnnotationScanner;

/**
 * Scans for {@link ProvidesReference &#64;ProvidesReference} annotations.
 */
class ProvidesReferenceScanner<T> extends AnnotationScanner<T, ProvidesReference> {

    ProvidesReferenceScanner(Class<T> type) {
        super(type, ProvidesReference.class);
    }

    @Override
    protected boolean includeMethod(Method method, ProvidesReference annotation) {
        this.checkNotStatic(method);
        final Class<?> type = method.getReturnType();
        if (!Component.class.isAssignableFrom(type) && type != String.class) {
            throw new IllegalArgumentException(this.getErrorPrefix(method)
              + "method is required to return either " + String.class.getName()
              + " or some sub-type of " + Component.class.getName());
        }
        this.checkParameterTypes(method);
        return true;
    }
}

