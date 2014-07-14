
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
 * Scans for {@link ProvidesReferenceLabel &#64;ProvidesReferenceLabel} annotations.
 */
class ProvidesReferenceLabelScanner<T> extends AnnotationScanner<T, ProvidesReferenceLabel> {

    ProvidesReferenceLabelScanner(Class<T> type) {
        super(type, ProvidesReferenceLabel.class);
    }

    @Override
    protected boolean includeMethod(Method method, ProvidesReferenceLabel annotation) {
        this.checkNotStatic(method);
        if (!Component.class.isAssignableFrom(method.getReturnType())) {
            throw new IllegalArgumentException(this.getErrorPrefix(method)
              + "method is required to return Component or some sub-type");
        }
        this.checkParameterTypes(method);
        return true;
    }
}

