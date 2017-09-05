
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.maven;

import java.io.File;
import java.util.List;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Provides data appropriate for "main" tasks (as opposed to test tasks).
 */
public abstract class AbstractMainSchemaMojo extends AbstractSchemaMojo {

    @Parameter(defaultValue = "${project.build.outputDirectory}", readonly = true)
    private File outputDirectory;

    @Override
    protected File getClassOutputDirectory() {
        return this.outputDirectory;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void addDependencyClasspathElements(List<String> elements) throws DependencyResolutionRequiredException {
        elements.addAll((List<String>)this.project.getCompileClasspathElements());
        elements.addAll((List<String>)this.project.getRuntimeClasspathElements());
    }
}
