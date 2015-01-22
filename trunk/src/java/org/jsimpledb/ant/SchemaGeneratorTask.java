
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.ant;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Path;
import org.apache.tools.ant.types.Reference;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.spring.JSimpleDBClassScanner;

/**
 * Ant task for schema XML generation and/or verification.
 *
 * <p>
 * This task scans the configured classpath for {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated
 * classes and either writes the corresponding schema to an XML file or verifies the contents of an existing file.
 * </p>
 *
 * <p>
 * The following tasks are supported:
 * </p>
 *
 * <p>
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#ccffcc">
 *  <th align="left">Attribute</th>
 *  <th align="left">Required?</th>
 *  <th align="left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code mode}</td>
 *  <td>No</td>
 *  <td>Set to {@code generate} to generate a new XML file, or {@code verify} to verify an existing XML file.
 *      Default is {@code verify}.</td>
 * </tr>
 * <tr>
 *  <td>{@code file}</td>
 *  <td>Yes</td>
 *  <td>The XML file to generate or verify.</td>
 * </tr>
 * <tr>
 *  <td>{@code matchNames}</td>
 *  <td>No</td>
 *  <td>Whether to verify not only schema compatibility but also that the same names are used for
 *      object types, fields, and composite indexes. Two schemas that are equivalent except for names are considered
 *      compatible, because the core API uses storage ID's, not names. However, if names change then some
 *      JSimpleDB layer operations, such as index queries and reference path inversion, may need to be updated.
 *      Default is {@code true}. Ignored unless {@code mode} is {@code verify}.</td>
 * </tr>
 * <tr>
 *  <td>{@code classpath} or {@code classpathref}</td>
 *  <td>Yes</td>
 *  <td>Specifies the search path for classes with {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}
 *      annotations.</td>
 * </tr>
 * <tr>
 *  <td>{@code packages}</td>
 *  <td>Yes</td>
 *  <td>Specifies one or more Java package names (separated by commas and/or whitespace) under which to look
 *      for {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass} annotations.</td>
 * </tr>
 * </table>
 * </div>
 * </p>
 *
 * <p>
 * Example:
 * <pre>
 *  &lt;project xmlns:jsimpledb="urn:org.dellroad.jsimpledb" ... &gt;
 *      ...
 *      &lt;taskdef uri="urn:org.dellroad.jsimpledb" name="schema"
 *        classname="org.jsimpledb.ant.SchemaGeneratorTask" classpathref="jsimpledb.classpath"/&gt;
 *      &lt;jsimpledb:schema mode="verify" classpathref="myclasses.classpath"
 *        file="expected-schema.xml" packages="com.example.model"/&gt;
 * </pre>
 * </p>
 *
 * @see org.jsimpledb.JSimpleDB
 * @see org.jsimpledb.schema.SchemaModel
 */
public class SchemaGeneratorTask extends Task {

    public static final String MODE_VERIFY = "verify";
    public static final String MODE_GENERATE = "generate";

    private String packages;
    private String mode = MODE_VERIFY;
    private boolean matchNames = true;
    private File file;
    private Path classPath;

    public void setPackages(String packages) {
        this.packages = packages;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setMatchNames(boolean matchNames) {
        this.matchNames = matchNames;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public Path createClasspath() {
        this.classPath = new Path(this.getProject());
        return this.classPath;
    }

    public void setClasspath(Path classPath) {
        this.classPath = classPath;
    }

    public void setClasspathRef(Reference ref) {
        this.classPath = (Path)ref.getReferencedObject(getProject());
    }

    /**
     * @throws BuildException
     */
    @Override
    public void execute() {

        // Sanity check
        if (this.file == null)
            throw new BuildException("`file' attribute is required specifying output file");
        if (!this.mode.equalsIgnoreCase(MODE_VERIFY) && !this.mode.equalsIgnoreCase(MODE_GENERATE))
            throw new BuildException("`mode' attribute must be `" + MODE_VERIFY + "' or `" + MODE_GENERATE + "'");
        final boolean generate = this.mode.equalsIgnoreCase(MODE_GENERATE);
        if (this.packages == null)
            throw new BuildException("`packages' attribute is required specifying packages to scan for Java model classes");
        if (this.classPath == null)
            throw new BuildException("`classpath' attribute is required specifying search path for scanned classes");

        // Create directory containing file
        if (generate && this.file.getParent() != null && !this.file.getParentFile().exists() && !this.file.getParentFile().mkdirs())
            throw new BuildException("error creating directory `" + this.file.getParentFile() + "'");

        // Set up mysterious classloader stuff
        final AntClassLoader loader = this.getProject().createClassLoader(this.classPath);
        final ClassLoader currentLoader = this.getClass().getClassLoader();
        if (currentLoader != null)
            loader.setParent(currentLoader);
        loader.setThreadContextLoader();
        try {

            // Scan for classes
            final HashSet<Class<?>> classes = new HashSet<>();
            for (String className : new JSimpleDBClassScanner().scanForClasses(this.packages.split("[\\s,]"))) {
                this.log("adding JSimpleDB schema class " + className);
                try {
                    classes.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                } catch (ClassNotFoundException e) {
                    throw new BuildException("failed to load class `" + className + "'", e);
                }
            }

            // Build schema model
            this.log("generating JSimpleDB schema from schema classes");
            final SchemaModel schemaModel;
            try {
                schemaModel = new JSimpleDB(classes).getSchemaModel();
            } catch (Exception e) {
                throw new BuildException("schema generation failed: " + e, e);
            }

            // Verify or generate
            if (generate) {

                // Write schema model to file
                this.log("writing JSimpleDB schema to `" + this.file + "'");
                try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(this.file))) {
                    schemaModel.toXML(output, true);
                } catch (IOException e) {
                    throw new BuildException("error writing schema to `" + this.file + "': " + e, e);
                }
            } else {

                // Read file
                this.log("verifying JSimpleDB schema matches `" + this.file + "'");
                final SchemaModel verifyModel;
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(this.file))) {
                    verifyModel = SchemaModel.fromXML(input);
                } catch (IOException e) {
                    throw new BuildException("error reading schema from `" + this.file + "': " + e, e);
                }

                // Compare
                if (!(matchNames ? schemaModel.equals(verifyModel) : schemaModel.isCompatibleWith(verifyModel)))
                    throw new BuildException("schema verification failed");
            }
        } finally {
            loader.resetThreadContextLoader();
            loader.cleanup();
        }
    }
}

