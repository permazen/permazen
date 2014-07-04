
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;

import org.jsimpledb.cli.cmd.CliCommand;
import org.jsimpledb.cli.func.CliFunction;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.type.filter.AnnotationTypeFilter;

class ClassPathScanner extends ClassPathScanningCandidateComponentProvider {

    ClassPathScanner() {
        super(true);
        this.setResourceLoader(new DefaultResourceLoader());
    }

    /**
     * Find annotated classes.
     */
    public ArrayList<String> scanForClasses(String... basePackages) {
        final ArrayList<String> nameList = new ArrayList<>();
        for (String basePackage : basePackages) {
            for (BeanDefinition candidate : this.findCandidateComponents(basePackage))
                nameList.add(candidate.getBeanClassName());
        }
        return nameList;
    }

    /**
     * Overridden to change the default filters. {@link ClassPathScanner} wants to match
     * {@link &#64;CliCommand} and {@link &#64;CliFunction} annotated classes.
     */
    @Override
    protected void registerDefaultFilters() {
        this.addIncludeFilter(new AnnotationTypeFilter(CliCommand.class));
        this.addIncludeFilter(new AnnotationTypeFilter(CliFunction.class));
    }
}

