
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.maven;

import java.io.File;
import java.util.Iterator;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;

/**
 * Schema utility API.
 *
 * <p>
 * This API is used by the Maven plugin. We do things this way to avoid class loader issues.
 */
public interface SchemaUtility {

    /**
     * Configure this instance.
     *
     * @param packageNames data model packages to scan
     * @param classNames data model classes to scan
     * @return schema ID
     * @throws MojoFailureException if schema was invalid or could not be generated due to misconfiguration, etc.
     * @throws MojoExecutionException if a fatal error occurs
     * @throws IllegalArgumentException if {@code log} is null
     * @throws IllegalStateException if already configured
     */
    String configure(Log log, String[] packageNames, String[] classNames, String encodingRegistryClassName)
      throws MojoExecutionException, MojoFailureException;

    /**
     * Generate schema XML and write to the specified file.
     *
     * @param file schema destination
     * @throws MojoExecutionException if a fatal error occurs
     * @throws IllegalArgumentException if {@code file} is null
     * @throws IllegalStateException if not configured
     */
    void generateSchema(File file) throws MojoExecutionException, MojoFailureException;

    /**
     * Verify schema matches the specified file.
     *
     * @param file expected schema file
     * @return true if schema matches file, otherwise false
     * @throws MojoExecutionException if a fatal error occurs
     * @throws IllegalArgumentException if {@code file} is null
     * @throws IllegalStateException if not configured
     */
    boolean verifySchema(File file) throws MojoExecutionException;

    /**
     * Verify schema does not conflict with any of the specified files.
     *
     * @param file expected schema file
     * @return true if schema does not conflict with any file, otherwise false
     * @throws MojoExecutionException if a fatal error occurs
     * @throws IllegalArgumentException if {@code files} is null
     * @throws IllegalStateException if not configured
     */
    boolean verifySchemas(Iterator<? extends File> files) throws MojoExecutionException;
}
