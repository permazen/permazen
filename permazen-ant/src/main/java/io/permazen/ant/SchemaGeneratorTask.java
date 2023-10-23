
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.ant;

import io.permazen.DefaultStorageIdGenerator;
import io.permazen.PermazenFactory;
import io.permazen.StorageIdGenerator;
import io.permazen.annotation.JFieldType;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.FieldType;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.spring.PermazenClassScanner;
import io.permazen.spring.PermazenFieldTypeScanner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.Path;
import org.apache.tools.ant.types.Reference;
import org.apache.tools.ant.types.Resource;

/**
 * Ant task for schema XML generation and/or verification.
 *
 * <p>
 * This task scans the configured classpath for classes with {@link io.permazen.annotation.PermazenType &#64;PermazenType}
 * and {@link io.permazen.annotation.JFieldType &#64;JFieldType} annotations and either writes the generated schema
 * to an XML file, or verifies the schema matches an existing XML file.
 *
 * <p>
 * Generation of schema XML files and the use of this task is not necessary. However, it does allow certain
 * schema-related problems to be detected at build time instead of runtime. In particular, it can let you know
 * if any change to your model classes requires a new Permazen schema version.
 *
 * <p>
 * This task can also check for conflicts between the schema in question and older schema versions that may still
 * exist in production databases. These other schema versions are specified using nested {@code <oldschemas>}
 * elements, which work just like {@code <fileset>}'s.
 *
 * <p>
 * The following attributes are supported by this task:
 *
 * <div style="margin-left: 20px;">
 * <table class="striped">
 * <caption>Supported Tasks</caption>
 * <tr style="bgcolor:#ccffcc">
 *  <th style="font-weight: bold; text-align: left">Attribute</th>
 *  <th style="font-weight: bold; text-align: left">Required?</th>
 *  <th style="font-weight: bold; text-align: left">Description</th>
 * </tr>
 * <tr>
 *  <td>{@code mode}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      Set to {@code generate} to generate a new XML file, {@code verify} to verify an existing XML file,
 *      or {@code generate-and-verify} to do both (i.e., update the schema file if necessary).
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
 *      Whether to verify not only {@link io.permazen.schema.SchemaModel#isCompatibleWith "same version"
 *      schema compatibility} but also that the two schemas are actually identical, i.e.,
 *      the same names are used for object types, fields, and composite indexes, and non-structural
 *      attributes such as delete cascades have not changed.
 *      </p>
 *
 *      <p>
 *      Two schemas that are equivalent except for names are compatible, because the core API uses storage ID's,
 *      not names. However, if names change then some Permazen layer operations, such as index queries
 *      and reference path inversion, may need to be updated.
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
 *      Whether to fail if verification fails when {@code mode="verify"} or when older schema
 *      versions are specified using nested {@code <oldschemas>} elements, which work just like
 *      {@code <fileset>}s.
 *      </p>
 *
 *      <p>
 *      Default is {@code true}.
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
 *  <td>{@code schemaVersionProperty}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      The name of an ant property to set to the auto-generated schema version number. This is the schema
 *      version number that will be auto-generated when a schema version number of {@code -1} is configured.
 *      This auto-generated version number is based on
 *      {@linkplain io.permazen.schema.SchemaModel#autogenerateVersion hashing the generated schema}.
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
 *      Specifies the search path containing classes with {@link io.permazen.annotation.PermazenType &#64;PermazenType}
 *      and {@link io.permazen.annotation.JFieldType &#64;JFieldType} annotations.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code packages}</td>
 *  <td>Yes, unless {@code classes} are specified</td>
 *  <td>
 *      <p>
 *      Specifies one or more Java package names (separated by commas and/or whitespace) under which to look
 *      for classes with {@link io.permazen.annotation.PermazenType &#64;PermazenType}
 *      or {@link io.permazen.annotation.JFieldType &#64;JFieldType} annotations.
 *
 *      <p>
 *      Use of this attribute requires Spring's classpath scanning classes ({@code spring-context.jar});
 *      these must be on the {@code <taskdef>} classpath.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code classes}</td>
 *  <td>Yes, unless {@code packages} are specified</td>
 *  <td>
 *      <p>
 *      Specifies one or more Java class names (separated by commas and/or whitespace) of
 *      classes with {@link io.permazen.annotation.PermazenType &#64;PermazenType}
 *      or {@link io.permazen.annotation.JFieldType &#64;JFieldType} annotations.
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
 * Classes are found by scanning the packages listed in the {@code "packages"} attribute.
 * Alternatively, or in addition, specific classes may specified using the {@code "classes"} attribute.
 *
 * <p>
 * To install this task into ant:
 *
 * <pre>
 *  &lt;project xmlns:permazen="urn:io.permazen.ant" ... &gt;
 *      ...
 *      &lt;taskdef uri="urn:io.permazen.ant" name="schema"
 *        classname="io.permazen.ant.SchemaGeneratorTask" classpathref="permazen.classpath"/&gt;
 * </pre>
 *
 * <p>
 * Example of generating a schema XML file that corresponds to the specified Java model classes:
 *
 * <pre>
 *  &lt;permazen:schema mode="generate" classpathref="myclasses.classpath"
 *    file="schema.xml" packages="com.example.model"/&gt;
 * </pre>
 *
 * <p>
 * Example of verifying that the schema generated from the Java model classes has not
 * changed incompatibly (i.e., in a way that would require a new schema version):
 *
 * <pre>
 *  &lt;permazen:schema mode="verify" classpathref="myclasses.classpath"
 *    file="expected-schema.xml" packages="com.example.model"/&gt;
 * </pre>
 *
 * <p>
 * Example of doing the same thing, and also verifying the generated schema is compatible with prior schema versions
 * that may still be in use in production databases:
 *
 * <pre>
 *  &lt;permazen:schema mode="verify" classpathref="myclasses.classpath"
 *    file="expected-schema.xml" packages="com.example.model"&gt;
 *      &lt;permazen:oldschemas dir="obsolete-schemas" includes="*.xml"/&gt;
 *  &lt;/permazen:schema&gt;
 * </pre>
 *
 * @see io.permazen.Permazen
 * @see io.permazen.schema.SchemaModel
 */
public class SchemaGeneratorTask extends Task {

    public static final String MODE_VERIFY = "verify";
    public static final String MODE_GENERATE = "generate";
    public static final String MODE_GENERATE_AND_VERIFY = "generate-and-verify";

    private String mode = MODE_VERIFY;
    private boolean matchNames = true;
    private boolean failOnError = true;
    private String verifiedProperty;
    private String schemaVersionProperty;
    private File file;
    private Path classPath;
    private String storageIdGeneratorClassName = DefaultStorageIdGenerator.class.getName();
    private final ArrayList<FileSet> oldSchemasList = new ArrayList<>();
    private final LinkedHashSet<String> classes = new LinkedHashSet<>();
    private final LinkedHashSet<String> packages = new LinkedHashSet<>();

    public void setClasses(String classes) {
        this.classes.addAll(Arrays.asList(classes.split("[\\s,]+")));
    }

    public void setPackages(String packages) {
        this.packages.addAll(Arrays.asList(packages.split("[\\s,]+")));
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

    public void setSchemaVersionProperty(String schemaVersionProperty) {
        this.schemaVersionProperty = schemaVersionProperty;
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

    public void addOldSchemas(FileSet oldSchemas) {
        this.oldSchemasList.add(oldSchemas);
    }

    /**
     * @throws BuildException if operation fails
     */
    @Override
    public void execute() {

        // Sanity check
        if (this.file == null)
            throw new BuildException("`file' attribute is required specifying output/verify file");
        boolean generate = false;
        boolean verify = false;
        switch (this.mode) {
        case MODE_VERIFY:
            verify = true;
            break;
        case MODE_GENERATE:
            generate = true;
            break;
        case MODE_GENERATE_AND_VERIFY:
            generate = true;
            verify = true;
            break;
        default:
            throw new BuildException("`mode' attribute must be one of \""
              + MODE_VERIFY + "\", or \"" + MODE_GENERATE + "\", or \"" + MODE_GENERATE_AND_VERIFY + "\"");
        }
        if (this.classPath == null)
            throw new BuildException("`classpath' attribute is required specifying search path for scanned classes");

        // Create directory containing file
        if (generate && this.file.getParent() != null && !this.file.getParentFile().exists() && !this.file.getParentFile().mkdirs())
            throw new BuildException("error creating directory \"" + this.file.getParentFile() + "\"");

        // Set up mysterious classloader stuff
        final AntClassLoader loader = this.getProject().createClassLoader(this.classPath);
        final ClassLoader currentLoader = this.getClass().getClassLoader();
        if (currentLoader != null)
            loader.setParent(currentLoader);
        loader.setThreadContextLoader();
        try {

            // Model and field type classes
            final HashSet<Class<?>> modelClasses = new HashSet<>();
            final HashSet<Class<?>> fieldTypeClasses = new HashSet<>();

            // Do package scanning
            if (!this.packages.isEmpty()) {

                // Join list
                final StringBuilder buf = new StringBuilder();
                for (String packageName : this.packages) {
                    if (buf.length() > 0)
                        buf.append(' ');
                    buf.append(packageName);
                }
                final String packageNames = buf.toString();

                // Scan for @PermazenType classes
                this.log("scanning for @PermazenType annotations in packages: " + packageNames);
                for (String className : new PermazenClassScanner().scanForClasses(packageNames)) {
                    this.log("adding Permazen model class " + className);
                    try {
                        modelClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                    } catch (ClassNotFoundException e) {
                        throw new BuildException("failed to load class \"" + className + "\"", e);
                    }
                }

                // Scan for @JFieldType classes
                this.log("scanning for @JFieldType annotations in packages: " + packageNames);
                for (String className : new PermazenFieldTypeScanner().scanForClasses(packageNames)) {
                    this.log("adding Permazen field type class \"" + className + "\"");
                    try {
                        fieldTypeClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                    } catch (Exception e) {
                        throw new BuildException("failed to instantiate " + className, e);
                    }
                }
            }

            // Do specific class scanning
            for (String className : this.classes) {

                // Load class
                final Class<?> cl;
                try {
                    cl = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                } catch (ClassNotFoundException e) {
                    throw new BuildException("failed to load class \"" + className + "\"", e);
                }

                // Add model classes
                if (cl.isAnnotationPresent(PermazenType.class)) {
                    this.log("adding Permazen model " + cl);
                    modelClasses.add(cl);
                }

                // Add field types
                if (cl.isAnnotationPresent(JFieldType.class)) {
                    this.log("adding Permazen field type " + cl);
                    fieldTypeClasses.add(cl);
                }
            }

            // Instantiate StorageIdGenerator
            final StorageIdGenerator storageIdGenerator;
            try {
                storageIdGenerator = Class.forName(this.storageIdGeneratorClassName,
                   false, Thread.currentThread().getContextClassLoader())
                  .asSubclass(StorageIdGenerator.class).getConstructor().newInstance();
            } catch (Exception e) {
                throw new BuildException("failed to instantiate class \"" + storageIdGeneratorClassName + "\"", e);
            }

            // Set up database
            final Database db = new Database(new SimpleKVDatabase());

            // Instantiate and configure field type classes
            for (Class<?> cl : fieldTypeClasses) {

                // Instantiate field types
                this.log("instantiating " + cl + " as field type instance");
                final FieldType<?> fieldType;
                try {
                    fieldType = this.asFieldTypeClass(cl).getConstructor().newInstance();
                } catch (Exception e) {
                    throw new BuildException("failed to instantiate " + cl.getName(), e);
                }

                // Add field type
                try {
                    db.getFieldTypeRegistry().add(fieldType);
                } catch (Exception e) {
                    throw new BuildException("failed to register custom field type " + cl.getName(), e);
                }
            }

            // Set up factory
            final PermazenFactory factory = new PermazenFactory();
            factory.setDatabase(db);
            factory.setSchemaVersion(1);
            factory.setStorageIdGenerator(storageIdGenerator);
            factory.setModelClasses(modelClasses);

            // Build schema model
            this.log("generating Permazen schema from schema classes");
            final SchemaModel schemaModel;
            try {
                schemaModel = factory.newPermazen().getSchemaModel();
            } catch (Exception e) {
                throw new BuildException("schema generation failed: " + e, e);
            }
            this.log("auto-generate schema version is " + schemaModel.autogenerateVersion());

            // Record schema model in database
            db.createTransaction(schemaModel, 1, true).commit();

            // Parse verification file
            SchemaModel verifyModel = null;
            if (verify)  {

                // Read file
                this.log("reading Permazen verification file \"" + this.file + "\"");
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(this.file))) {
                    verifyModel = SchemaModel.fromXML(input);
                } catch (IOException e) {
                    throw new BuildException("error reading schema from \"" + this.file + "\": " + e, e);
                }
            }

            // Generate new file
            if (generate) {
                this.log("writing generated Permazen schema to \"" + this.file + "\"");
                try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(this.file))) {
                    schemaModel.toXML(output, true);
                } catch (IOException e) {
                    throw new BuildException("error writing schema to \"" + this.file + "\": " + e, e);
                }
            }

            // Compare
            boolean verified = true;
            if (verify) {
                final boolean matched = matchNames ? schemaModel.equals(verifyModel) : schemaModel.isCompatibleWith(verifyModel);
                if (!matched)
                    verified = false;
                this.log("schema verification " + (matched ? "succeeded" : "failed"));
                if (!matched)
                    this.log(schemaModel.differencesFrom(verifyModel).toString());
            }

            // Check for conflicts with other schema versions
            if (verify && verified) {
                int schemaVersion = 2;
                for (FileSet oldSchemas : this.oldSchemasList) {
                    for (Iterator<?> i = oldSchemas.iterator(); i.hasNext(); ) {
                        final Resource resource = (Resource)i.next();
                        this.log("checking schema for conflicts with " + resource);
                        final SchemaModel otherSchema;
                        try (BufferedInputStream input = new BufferedInputStream(resource.getInputStream())) {
                            otherSchema = SchemaModel.fromXML(input);
                        } catch (IOException e) {
                            throw new BuildException("error reading schema from \"" + resource + "\": " + e, e);
                        }
                        try {
                            db.createTransaction(otherSchema, schemaVersion++, true).commit();
                        } catch (Exception e) {
                            this.log("schema conflicts with " + resource + ": " + e);
                            verified = false;
                        }
                    }
                }
            }

            // Set verified property
            if (verify && this.verifiedProperty != null)
                this.getProject().setProperty(this.verifiedProperty, "" + verified);

            // Set auto-generated schema version property
            if (this.schemaVersionProperty != null)
                this.getProject().setProperty(this.schemaVersionProperty, "" + schemaModel.autogenerateVersion());

            // Check verification results
            if (verify && !verified && this.failOnError)
                throw new BuildException("schema verification failed");
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

