
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches {@link ProvidesReference &#64;ProvidesReference} methods for Java classes.
 */
public final class ReferenceMethodCache {

    private static final ReferenceMethodCache INSTANCE = new ReferenceMethodCache();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final LoadingCache<Class<?>, Method> cache = CacheBuilder.newBuilder().weakKeys().build(
      new CacheLoader<Class<?>, Method>() {
        @Override
        public Method load(Class<?> type) {
            return ReferenceMethodCache.this.findReferenceLableMethod(type);
        }
    });

    private ReferenceMethodCache() {
    }

    /**
     * Get the singleton instance.
     */
    public static ReferenceMethodCache getInstance() {
        return ReferenceMethodCache.INSTANCE;
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

    private <T> Method findReferenceLableMethod(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        Method refLabelMethod = null;
        for (ProvidesReferenceScanner<T>.MethodInfo methodInfo : new ProvidesReferenceScanner<T>(type).findAnnotatedMethods()) {
            final Method method = methodInfo.getMethod();
            final Class<?> returnType = method.getReturnType();
            if (refLabelMethod != null) {
                this.log.warn("ignoring duplicate @" + ProvidesReference.class.getSimpleName() + "-annotated method "
                  + method + "; using the first one found (" + refLabelMethod + ")");
                continue;
            }
            refLabelMethod = method;
        }
        return refLabelMethod;
    }
}

