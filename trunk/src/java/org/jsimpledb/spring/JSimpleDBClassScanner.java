
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import org.jsimpledb.annotation.JSimpleClass;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.core.env.Environment;
import org.springframework.core.type.ClassMetadata;

/**
 * Scans the classpath for types annotated as {@link JSimpleClass &#64;JSimpleClass}.
 */
public class JSimpleDBClassScanner extends AnnotatedClassScanner {

    /**
     * Constructor.
     */
    public JSimpleDBClassScanner() {
        super(JSimpleClass.class);
    }

    /**
     * Constructor.
     *
     * @param useDefaultFilters whether to register the default filters for {@link JSimpleClass &#64;JSimpleClass} type annotations
     * @param environment environment to use
     */
    public JSimpleDBClassScanner(boolean useDefaultFilters, Environment environment) {
        super(useDefaultFilters, environment, JSimpleClass.class);
    }

    /**
     * Determine if the given bean definition is a possible search candidate.
     *
     * <p>
     * This method is overridden in {@link JSimpleDBClassScanner} to allow abstract classes.
     * </p>
     */
    @Override
    protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
        final ClassMetadata metadata = beanDefinition.getMetadata();
        return !metadata.isInterface() && metadata.isIndependent();
    }
}

