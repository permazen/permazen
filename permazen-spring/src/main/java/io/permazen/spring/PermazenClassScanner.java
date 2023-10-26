
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import io.permazen.annotation.PermazenType;

import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.core.env.Environment;
import org.springframework.core.type.ClassMetadata;

/**
 * Scans the classpath for types annotated as {@link PermazenType &#64;PermazenType}.
 */
public class PermazenClassScanner extends AnnotatedClassScanner {

    /**
     * Constructor.
     *
     * <p>
     * The current thread's {@linkplain Thread#getContextClassLoader context class loader} will be used.
     * as the underlying {@link ClassLoader}.
     */
    public PermazenClassScanner() {
        this(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Constructor.
     *
     * @param loader the underlying {@link ClassLoader} to use
     */
    public PermazenClassScanner(ClassLoader loader) {
        super(loader, PermazenType.class);
    }

    /**
     * Constructor.
     *
     * @param loader the underlying {@link ClassLoader} to use
     * @param useDefaultFilters whether to register the default filters for {@link PermazenType &#64;PermazenType} type annotations
     * @param environment environment to use
     */
    public PermazenClassScanner(ClassLoader loader, boolean useDefaultFilters, Environment environment) {
        super(loader, useDefaultFilters, environment, PermazenType.class);
    }

    /**
     * Determine if the given bean definition is a possible search candidate.
     *
     * <p>
     * This method is overridden in {@link PermazenClassScanner} to allow abstract classes and interfaces.
     */
    @Override
    protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
        final ClassMetadata metadata = beanDefinition.getMetadata();
        return metadata.isIndependent();
    }
}
