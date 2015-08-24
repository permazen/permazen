
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
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
import org.jsimpledb.DefaultStorageIdGenerator;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.StorageIdGenerator;
import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.spring.JSimpleDBClassScanner;
import org.jsimpledb.spring.JSimpleDBFieldTypeScanner;

/**
 * Ant task for schema XML generation and/or verification.
 *
 * <p>
 * This task scans the configured classpath for classes with {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}
 * and {@link org.jsimpledb.annotation.JFieldType &#64;JFieldType} annotations and either writes the generated schema
 * to an XML file, or verifies the schema from an existing XML file.
 * </p>
 *
 * <p>
 * The following tasks are supported:
 * </p>
 *
 * <div style="margin-left: 20px;">
 * <table border="1" cellpadding="3" cellspacing="0" summary="Supported Tasks">
 * <tr style="bgcolor:#ccffcc">
 *  <th align="left">Attribute</th>
 *  <th align="left">Required?</th>
 *  <th align="left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code mode}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      Set to {@code generate} to generate a new XML file, or {@code verify} to verify an existing XML file.
 *      </p>
 *
 *      <p>
 *      Default is {@code verify}.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code file}</td>
 *  <td>Yes</td>
 *  <td>
 *      <p>
 *      The XML file to generate or verify.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code matchNames}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      Whether to verify not only schema compatibility but also that the two schemas are identical, i.e.,
 *      the same names are used for object types, fields, and composite indexes.
 *      </p>
 *
 *      <p>
 *      Two schemas that are equivalent except for names are considered compatible, because the core API uses
 *      storage ID's, not names. However, if names change then some JSimpleDB layer operations, such as index
 *      queries and reference path inversion, may need to be updated.
 *      </p>
 *
 *      <p>
 *      Default is {@code true}. Ignored unless {@code mode} is {@code verify}.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code failOnError}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      Whether to fail if verification fails.
 *      </p>
 *
 *      <p>
 *      Default is {@code true}. Ignored unless {@code mode} is {@code verify}.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code verifiedProperty}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      The name of an ant property to set to {@code true} or {@code false} depending on whether
 *      verification succeeded or failed. Useful when {@code failOnError} is set to {@code false}
 *      and you want to handle the failure elsewhere in the build file.
 *      </p>
 *
 *      <p>
 *      Default is to not set any property.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code classpath} or {@code classpathref}</td>
 *  <td>Yes</td>
 *  <td>
 *      <p>
 *      Specifies the search path for classes with {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}
 *      annotations.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code packages}</td>
 *  <td>Yes</td>
 *  <td>
 *      <p>
 *      Specifies one or more Java package names (separated by commas and/or whitespace) under which to look
 *      for classes with {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}
 *      and {@link org.jsimpledb.annotation.JFieldType &#64;JFieldType} annotations.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code storageIdGeneratorClass}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      Specifies the name of an optional custom {@link StorageIdGenerator} class.
 *      </p>
 *
 *      <p>
 *      By default, a {@link DefaultStorageIdGenerator} is used.
 *      </p>
 * </td>
 * </tr>
 * </table>
 * </div>
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
 *
 * @see org.jsimpledb.JSimpleDB
 * @see org.jsimpledb.schema.SchemaModel
 */
public class SchemaGeneratorTask extends Task {

    public static final String MODE_VERIFY = "verify";
    public static final String MODE_GENERATE = "generate";

    private String[] packages;
    private String mode = MODE_VERIFY;
    private boolean matchNames = true;
    private boolean failOnError = true;
    private String verifiedProperty;
    private File file;
    private Path classPath;
    private String storageIdGeneratorClassName = DefaultStorageIdGenerator.class.getName();

    public void setPackages(String packages) {
        this.packages = packages.split("[\\s,]");
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setMatchNames(boolean matchNames) {
        this.matchNames = matchNames;
    }

    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    public void setVerifiedProperty(String verifiedProperty) {
        this.verifiedProperty = verifiedProperty;
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
        this.classPath = (Path)ref.getReferencedObject(this.getProject());
    }

    public void setStorageIdGeneratorClass(String storageIdGeneratorClassName) {
        this.storageIdGeneratorClassName = storageIdGeneratorClassName;
    }

    /**
     * @throws BuildException if operation fails
     */
    @Override
    public void execute() {

        // Sanity check
        if (this.file == null)
            throw new BuildException("`file' attribute is required specifying output/verify file");
        final boolean generate;
        switch (this.mode) {
        case MODE_VERIFY:
            generate = false;
            break;
        case MODE_GENERATE:
            generate = true;
            break;
        default:
            throw new BuildException("`mode' attribute must be one of `" + MODE_VERIFY + "' or `" + MODE_GENERATE + "'");
        }
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

            // Scan for @JSimpleClass classes
            final HashSet<Class<?>> modelClasses = new HashSet<>();
            for (String className : new JSimpleDBClassScanner().scanForClasses(this.packages)) {
                this.log("adding JSimpleDB schema class " + className);
                try {
                    modelClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                } catch (ClassNotFoundException e) {
                    throw new BuildException("failed to load class `" + className + "'", e);
                }
            }

            // Scan for @JFieldType classes
            final HashSet<FieldType<?>> fieldTypes = new HashSet<>();
            for (String className : new JSimpleDBFieldTypeScanner().scanForClasses(this.packages)) {
                this.log("adding JSimpleDB field type class " + className);
                try {
                    fieldTypes.add(this.asFieldTypeClass(
                      Class.forName(className, false, Thread.currentThread().getContextClassLoader())).newInstance());
                } catch (Exception e) {
                    throw new BuildException("failed to instantiate class `" + className + "'", e);
                }
            }

            // Instantiate StorageIdGenerator
            final StorageIdGenerator storageIdGenerator;
            try {
                storageIdGenerator = Class.forName(this.storageIdGeneratorClassName,
                  false, Thread.currentThread().getContextClassLoader()).asSubclass(StorageIdGenerator.class).newInstance();
            } catch (Exception e) {
                throw new BuildException("failed to instantiate class `" + storageIdGeneratorClassName + "'", e);
            }

            // Set up database and configure field type classes
            final Database db = new Database(new SimpleKVDatabase());
            for (FieldType<?> fieldType : fieldTypes) {
                try {
                    db.getFieldTypeRegistry().add(fieldType);
                } catch (Exception e) {
                    throw new BuildException("failed to register custom field type " + fieldType.getClass().getName(), e);
                }
            }

            // Set up factory
            final JSimpleDBFactory factory = new JSimpleDBFactory();
            factory.setDatabase(db);
            factory.setSchemaVersion(1);
            factory.setStorageIdGenerator(storageIdGenerator);
            factory.setModelClasses(modelClasses);

            // Build schema model
            this.log("generating JSimpleDB schema from schema classes");
            final SchemaModel schemaModel;
            try {
                schemaModel = factory.newJSimpleDB().getSchemaModel();
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
                final boolean verified = matchNames ? schemaModel.equals(verifyModel) : schemaModel.isCompatibleWith(verifyModel);
                if (this.verifiedProperty != null)
                    this.getProject().setProperty(this.verifiedProperty, "" + verified);
                this.log("schema verification " + (verified ? "succeeded" : "failed"));
                if (!verified) {
                    this.log(schemaModel.differencesFrom(verifyModel).toString());
                    if (this.failOnError)
                        throw new BuildException("schema verification failed");
                }
            }
        } finally {
            loader.resetThreadContextLoader();
            loader.cleanup();
        }
    }

    @SuppressWarnings("unchecked")
    private Class<? extends FieldType<?>> asFieldTypeClass(Class<?> klass) {
        try {
            return (Class<? extends FieldType<?>>)klass.asSubclass(FieldType.class);
        } catch (ClassCastException e) {
            throw new BuildException("invalid @" + JFieldType.class.getSimpleName() + " annotation on "
              + klass + ": type is not a subclass of " + FieldType.class);
        }
    }
}

