
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.lang.reflect.Method;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Caches {@link ProvidesReferenceLabel &#64;ProvidesReferenceLabel} methods for Java classes.
 */
@Component
public final class ReferenceLabelCache {

    private static final ReferenceLabelCache INSTANCE = new ReferenceLabelCache();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final LoadingCache<Class<?>, Method> cache = CacheBuilder.newBuilder().weakKeys().build(
      new CacheLoader<Class<?>, Method>() {
        @Override
        public Method load(Class<?> type) {
            return ReferenceLabelCache.this.findReferenceLabelMethod(type);
        }
    });

    private ReferenceLabelCache() {
    }

    /**
     * Get the singleton instance.
     */
    public static ReferenceLabelCache getInstance() {
        return ReferenceLabelCache.INSTANCE;
    }

    /**
     * Get field editor factories for the fields in the given Java model type.
     *
     * @return unmodifiable mapping from field name to editor factory
     * @throws IllegalArgumentException if {@code type} is null
     */
    public <T> Method getReferenceLabelMethod(Class<T> type) {
        return this.cache.getUnchecked(type);
    }

    private <T> Method findReferenceLabelMethod(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        final Set<ProvidesReferenceLabelScanner<T>.MethodInfo> methodInfos
          = new ProvidesReferenceLabelScanner<T>(type).findAnnotatedMethods();
        switch (methodInfos.size()) {
        case 0:
            return null;
        case 1:
            return methodInfos.iterator().next().getMethod();
        default:
            this.log.warn("found multiple @" + ProvidesReferenceLabel.class.getSimpleName() + "-annotated methods in "
              + type + ": " + methodInfos + "; using the first one found");
            return methodInfos.iterator().next().getMethod();
        }
    }
}

