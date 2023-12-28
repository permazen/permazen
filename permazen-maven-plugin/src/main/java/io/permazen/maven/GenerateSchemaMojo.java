
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.maven;

import io.permazen.Permazen;

import java.io.File;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;

/**
 * Generates a schema XML file from a set of Permazen model classes.
 *
 * <p>
 * Such a file can be used to detect Java model class changes that result in a new schema.
 */
@Mojo(name = "generate",
  requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME,
  threadSafe = true)
public class GenerateSchemaMojo extends AbstractMainSchemaMojo {

    public static final String PERMAZEN_DIRECTORY_DEFAULT = "${basedir}/src/main/permazen";
    public static final String EXPECTED_SCHEMA_DEFAULT = PERMAZEN_DIRECTORY_DEFAULT + "/current-schema.xml";

    /**
     * The schema XML file that you want to generate.
     */
    @Parameter(defaultValue = GenerateSchemaMojo.EXPECTED_SCHEMA_DEFAULT, property = "schemaFile")
    private File schemaFile;

    @Override
    protected void execute(Permazen jdb) throws MojoExecutionException, MojoFailureException {
        this.generate(jdb.getSchemaModel(), this.schemaFile);
    }
}
