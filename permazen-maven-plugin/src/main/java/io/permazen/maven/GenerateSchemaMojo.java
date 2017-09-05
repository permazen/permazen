
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.maven;

import io.permazen.JSimpleDB;

import java.io.File;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;

/**
 * Generates a schema XML file from a set of JSimpleDB model classes.
 *
 * <p>
 * Such a file can be used to detect schema changes that require a new schema version number
 * as Java model classes are modified over time.
 */
@Mojo(name = "generate",
  requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME,
  threadSafe = true)
public class GenerateSchemaMojo extends AbstractMainSchemaMojo {

    public static final String JSIMPLEDB_DIRECTORY_DEFAULT = "${basedir}/src/main/jsimpledb";
    public static final String EXPECTED_SCHEMA_DEFAULT = JSIMPLEDB_DIRECTORY_DEFAULT + "/current-schema.xml";

    /**
     * The schema XML file that you want to generate.
     */
    @Parameter(defaultValue = GenerateSchemaMojo.EXPECTED_SCHEMA_DEFAULT, property = "schemaFile")
    private File schemaFile;

    @Override
    protected void execute(JSimpleDB jdb) throws MojoExecutionException, MojoFailureException {
        this.generate(jdb.getSchemaModel(), this.schemaFile);
    }
}
