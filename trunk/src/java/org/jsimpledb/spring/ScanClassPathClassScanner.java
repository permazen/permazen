
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import java.util.ArrayList;

import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.annotation.JSimpleClass;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.env.Environment;
import org.springframework.core.type.ClassMetadata;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.Assert;

class ScanClassPathClassScanner extends ClassPathScanningCandidateComponentProvider {

    ScanClassPathClassScanner(boolean useDefaultFilters, Environment environment) {
        super(useDefaultFilters, environment);
    }

    /**
     * Find annotated classes.
     */
    public ArrayList<String> scanForClasses(String... basePackages) {
        Assert.notEmpty(basePackages, "At least one base package must be specified");
        final ArrayList<String> nameList = new ArrayList<>();
        for (String basePackage : basePackages) {
            for (BeanDefinition candidate : this.findCandidateComponents(basePackage))
                nameList.add(candidate.getBeanClassName());
        }
        return nameList;
    }

    /**
     * Overridden to change the default filters. {@link JSimpleDBClassScanner} wants to match
     * {@link &#64;JSimpleClass} and {@link &#64;JFieldType} annotated classes.
     */
    @Override
    protected void registerDefaultFilters() {
        this.addIncludeFilter(new AnnotationTypeFilter(JSimpleClass.class));
        this.addIncludeFilter(new AnnotationTypeFilter(JFieldType.class));
    }

    /**
     * Overridden to allow abstract classes.
     */
    @Override
    protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
        final ClassMetadata metadata = beanDefinition.getMetadata();
        return !metadata.isInterface() && metadata.isIndependent();
    }
}

