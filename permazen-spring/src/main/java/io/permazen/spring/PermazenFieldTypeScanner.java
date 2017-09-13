
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
     */
    public PermazenFieldTypeScanner() {
        super(JFieldType.class);
    }

    /**
     * Constructor.
     *
     * @param useDefaultFilters whether to register the default filters for {@link JFieldType &#64;JFieldType} type annotations
     * @param environment environment to use
     */
    public PermazenFieldTypeScanner(boolean useDefaultFilters, Environment environment) {
        super(useDefaultFilters, environment, JFieldType.class);
    }
}

