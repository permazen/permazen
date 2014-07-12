
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Set;

import org.jsimpledb.JClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Caches {@link ProvidesReferenceLabel &#64;ProvidesReferenceLabel} methods for {@link JClass}es.
 */
@Component
public class ReferenceLabelCache {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final HashMap<JClass<?>, Method> referenceLabelMethodMap = new HashMap<>();

    /**
     * Get field editor factories for the fields in the given Java model type.
     *
     * @return unmodifiable mapping from field name to editor factory
     */
    public <T> Method getReferenceLabelMethod(JClass<T> jclass) {
        if (jclass == null)
            throw new IllegalArgumentException("null jclass");
        if (!this.referenceLabelMethodMap.containsKey(jclass)) {
            final Set<ProvidesReferenceLabelScanner<T>.MethodInfo> methodInfos
              = new ProvidesReferenceLabelScanner<T>(jclass).findAnnotatedMethods();
            switch (methodInfos.size()) {
            case 0:
                this.referenceLabelMethodMap.put(jclass, null);
                break;
            case 1:
                this.referenceLabelMethodMap.put(jclass, methodInfos.iterator().next().getMethod());
                break;
            default:
                this.log.warn("found multiple @" + ProvidesReferenceLabel.class.getSimpleName() + "-annotated methods in "
                  + jclass.getTypeToken().getRawType() + ": " + methodInfos + "; using the first one found");
                this.referenceLabelMethodMap.put(jclass, methodInfos.iterator().next().getMethod());
                break;
            }
        }
        return this.referenceLabelMethodMap.get(jclass);
    }
}

