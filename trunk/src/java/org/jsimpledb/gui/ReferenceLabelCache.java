
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Caches {@link ProvidesReferenceLabel &#64;ProvidesReferenceLabel} methods for Java classes.
 */
@Component
public class ReferenceLabelCache {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final HashMap<Class<?>, Method> referenceLabelMethodMap = new HashMap<>();

    /**
     * Get field editor factories for the fields in the given Java model type.
     *
     * @return unmodifiable mapping from field name to editor factory
     * @throws IllegalArgumentException if {@code type} is null
     */
    public <T> Method getReferenceLabelMethod(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (!this.referenceLabelMethodMap.containsKey(type)) {
            final Set<ProvidesReferenceLabelScanner<T>.MethodInfo> methodInfos
              = new ProvidesReferenceLabelScanner<T>(type).findAnnotatedMethods();
            switch (methodInfos.size()) {
            case 0:
                this.referenceLabelMethodMap.put(type, null);
                break;
            case 1:
                this.referenceLabelMethodMap.put(type, methodInfos.iterator().next().getMethod());
                break;
            default:
                this.log.warn("found multiple @" + ProvidesReferenceLabel.class.getSimpleName() + "-annotated methods in "
                  + type + ": " + methodInfos + "; using the first one found");
                this.referenceLabelMethodMap.put(type, methodInfos.iterator().next().getMethod());
                break;
            }
        }
        return this.referenceLabelMethodMap.get(type);
    }
}

