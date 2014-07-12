
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.lang.reflect.Method;

import org.jsimpledb.JClass;
import org.jsimpledb.util.AnnotationScanner;

/**
 * Scans for {@link ProvidesReferenceLabel &#64;ProvidesReferenceLabel} annotations.
 */
class ProvidesReferenceLabelScanner<T> extends AnnotationScanner<T, ProvidesReferenceLabel> {

    ProvidesReferenceLabelScanner(JClass<T> jclass) {
        super(jclass, ProvidesReferenceLabel.class);
    }

    @Override
    protected boolean includeMethod(Method method, ProvidesReferenceLabel annotation) {
        this.checkNotStatic(method);
        if (method.getReturnType() == void.class)
            throw new IllegalArgumentException(this.getErrorPrefix(method) + "method is required to return non-void");
        this.checkParameterTypes(method);
        return true;
    }
}

