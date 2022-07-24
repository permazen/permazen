
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import io.permazen.annotation.JFieldType;

import org.springframework.core.env.Environment;

/**
 * Scans the classpath for types annotated as {@link JFieldType &#64;JFieldType}.
 */
public class PermazenFieldTypeScanner extends AnnotatedClassScanner {

    /**
     * Constructor.
     *
     * <p>
     * The current thread's {@linkplain Thread#getContextClassLoader context class loader} will be used.
     * as the underlying {@link ClassLoader}.
     */
    public PermazenFieldTypeScanner() {
        this(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Constructor.
     */
    public PermazenFieldTypeScanner(ClassLoader loader) {
        super(loader, JFieldType.class);
    }

    /**
     * Constructor.
     *
     * @param useDefaultFilters whether to register the default filters for {@link JFieldType &#64;JFieldType} type annotations
     * @param environment environment to use
     */
    public PermazenFieldTypeScanner(ClassLoader loader, boolean useDefaultFilters, Environment environment) {
        super(loader, useDefaultFilters, environment, JFieldType.class);
    }
}

