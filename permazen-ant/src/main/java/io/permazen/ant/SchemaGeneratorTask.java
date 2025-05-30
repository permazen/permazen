
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.ant;

import io.permazen.Permazen;
import io.permazen.PermazenConfig;
import io.permazen.annotation.OnSchemaChange;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.TransactionConfig;
import io.permazen.encoding.DefaultEncodingRegistry;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.spring.PermazenClassScanner;

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
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-xml-doc.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
 *
 * <p>
 * This task scans the configured classpath for classes with {@link PermazenType &#64;PermazenType}
 * annotations and either writes the generated schema to an XML file, or verifies the schema matches an existing XML file.
 *
 * <p>
 * Generation of schema XML files and the use of this task is not necessary. However, it does allow certain
 * schema-related problems to be detected at build time instead of runtime. In particular, it can let you know
 * if any change to your model classes requires a new Permazen schema version.
 *
 * <p>
 * This task can also check for conflicts between the schema in question and older schema versions that may still
 * exist in production databases. These other schema versions are specified using nested {@code <oldschemas>}
 * elements, which work just like {@code <fileset>}'s. Conflicts are only possible when explicit storage ID's are used.
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
 *  <td>{@code schemaIdProperty}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      The name of an ant property to set to the {@link SchemaId} associated with the generated schema.
 *      This is a string value that looks like {@code "Schema_d462f3e631781b00ef812561115c48f6"}.
 *      These values are provided to {@link OnSchemaChange &#64;OnSchemaChange} annotated methods.
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
 *      Specifies the search path containing classes with {@link PermazenType &#64;PermazenType}
 *      annotations.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code packages}</td>
 *  <td>Yes, unless {@code classes} are specified</td>
 *  <td>
 *      <p>
 *      Specifies one or more Java package names (separated by commas and/or whitespace) under which to look
 *      for classes with {@link PermazenType &#64;PermazenType} annotations.
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
 *      classes with {@link PermazenType &#64;PermazenType} annotations.
 *      </p>
 * </td>
 * </tr>
 * <tr>
 *  <td>{@code encodingRegistry}</td>
 *  <td>No</td>
 *  <td>
 *      <p>
 *      Specifies the name of an optional custom {@link EncodingRegistry} class.
 *      </p>
 *
 *      <p>
 *      By default, a {@link DefaultEncodingRegistry} is used.
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
 * <pre><code class="language-xml">
 *  &lt;project xmlns:permazen="urn:io.permazen.ant" ... &gt;
 *      ...
 *      &lt;taskdef uri="urn:io.permazen.ant" name="schema"
 *        classname="io.permazen.ant.SchemaGeneratorTask" classpathref="permazen.classpath"/&gt;
 * </code></pre>
 *
 * <p>
 * Example of generating a schema XML file that corresponds to the specified Java model classes:
 *
 * <pre><code class="language-xml">
 *  &lt;permazen:schema mode="generate" classpathref="myclasses.classpath"
 *    file="schema.xml" packages="com.example.model"/&gt;
 * </code></pre>
 *
 * <p>
 * Example of verifying that the schema generated from the Java model classes has not
 * changed incompatibly (i.e., in a way that would require a new schema version):
 *
 * <pre><code class="language-xml">
 *  &lt;permazen:schema mode="verify" classpathref="myclasses.classpath"
 *    file="expected-schema.xml" packages="com.example.model"/&gt;
 * </code></pre>
 *
 * <p>
 * Example of doing the same thing, and also verifying the generated schema is compatible with prior schema versions
 * that may still be in use in production databases:
 *
 * <pre><code class="language-xml">
 *  &lt;permazen:schema mode="verify" classpathref="myclasses.classpath"
 *    file="expected-schema.xml" packages="com.example.model"&gt;
 *      &lt;permazen:oldschemas dir="obsolete-schemas" includes="*.xml"/&gt;
 *  &lt;/permazen:schema&gt;
 * </code></pre>
 *
 * @see Permazen
 * @see SchemaModel
 */
public class SchemaGeneratorTask extends Task {

    public static final String MODE_VERIFY = "verify";
    public static final String MODE_GENERATE = "generate";
    public static final String MODE_GENERATE_AND_VERIFY = "generate-and-verify";

    private String mode = MODE_VERIFY;
    private boolean failOnError = true;
    private String verifiedProperty;
    private String schemaIdProperty;
    private File file;
    private Path classPath;
    private String encodingRegistryClassName;
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

    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    public void setVerifiedProperty(String verifiedProperty) {
        this.verifiedProperty = verifiedProperty;
    }

    public void setSchemaIdProperty(String schemaIdProperty) {
        this.schemaIdProperty = schemaIdProperty;
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

    public void setEncodingRegistryClass(String encodingRegistryClassName) {
        this.encodingRegistryClassName = encodingRegistryClassName;
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
            throw new BuildException("\"file\" attribute is required specifying output/verify file");
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
            throw new BuildException(String.format(
              "\"mode\" attribute must be one of \"%s\", or \"%s\", or \"%s\"",
              MODE_VERIFY, MODE_GENERATE, MODE_GENERATE_AND_VERIFY));
        }
        if (this.classPath == null)
            throw new BuildException("\"classpath\" attribute is required specifying search path for scanned classes");

        // Create directory containing file
        if (generate && this.file.getParent() != null && !this.file.getParentFile().exists() && !this.file.getParentFile().mkdirs())
            throw new BuildException(String.format("error creating directory \"%s\"", this.file.getParentFile()));

        // Set up mysterious classloader stuff
        final AntClassLoader loader = this.getProject().createClassLoader(this.classPath);
        final ClassLoader currentLoader = this.getClass().getClassLoader();
        if (currentLoader != null)
            loader.setParent(currentLoader);
        loader.setThreadContextLoader();
        try {

            // Model classes
            final HashSet<Class<?>> modelClasses = new HashSet<>();

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
                this.flog("scanning for @PermazenType annotations in packages: %s", packageNames);
                for (String className : new PermazenClassScanner().scanForClasses(packageNames)) {
                    this.flog("adding Permazen model class %s", className);
                    try {
                        modelClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                    } catch (ClassNotFoundException e) {
                        throw new BuildException(String.format("failed to load class \"%s\"", className), e);
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
                    throw new BuildException(String.format("failed to load class \"%s\"", className), e);
                }

                // Add model classes
                if (cl.isAnnotationPresent(PermazenType.class)) {
                    this.flog("adding Permazen model %s", cl);
                    modelClasses.add(cl);
                }
            }

            // Instantiate EncodingRegistry
            EncodingRegistry encodingRegistry = null;
            if (this.encodingRegistryClassName != null) {
                try {
                    encodingRegistry = Class.forName(this.encodingRegistryClassName,
                       false, Thread.currentThread().getContextClassLoader())
                      .asSubclass(EncodingRegistry.class).getConstructor().newInstance();
                } catch (Exception e) {
                    throw new BuildException(String.format(
                      "failed to instantiate class \"%s\"", this.encodingRegistryClassName), e);
                }
            }

            // Set up database
            final Database db = new Database(new MemoryKVDatabase());
            db.setEncodingRegistry(encodingRegistry);

            // Set up config
            final PermazenConfig config = PermazenConfig.builder()
              .database(db)
              .modelClasses(modelClasses)
              .build();

            // Build schema model
            this.log("generating Permazen schema from schema classes");
            final SchemaModel schemaModel;
            try {
                schemaModel = config.newPermazen().getSchemaModel(false);
            } catch (Exception e) {
                throw new BuildException(String.format("schema generation failed: %s", e), e);
            }
            final SchemaId schemaId = schemaModel.getSchemaId();
            this.flog("schema ID is \"%s\"", schemaId);

            // Parse verification file
            SchemaModel verifyModel = null;
            if (verify)  {

                // Read file
                this.flog("reading Permazen verification file \"%s\"", this.file);
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(this.file))) {
                    verifyModel = SchemaModel.fromXML(input);
                } catch (IOException e) {
                    throw new BuildException(String.format("error reading schema from \"%s\": %s", this.file, e), e);
                }
            }

            // Generate new file
            if (generate) {
                this.flog("writing generated Permazen schema to \"%s\"", this.file);
                try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(this.file))) {
                    schemaModel.toXML(output, false, true);
                } catch (IOException e) {
                    throw new BuildException(String.format("error writing schema to \"%s\": %s", this.file, e), e);
                }
            }

            // Compare
            boolean verified = true;
            if (verify) {
                final boolean matched = schemaModel.equals(verifyModel);
                if (!matched)
                    verified = false;
                this.flog("schema verification %s", matched ? "succeeded" : "failed");
                if (!matched)
                    this.log(schemaModel.differencesFrom(verifyModel).toString());
            }

            // Check for conflicts with other schema versions
            if (verify && verified) {
                for (FileSet oldSchemas : this.oldSchemasList) {
                    for (Iterator<?> i = oldSchemas.iterator(); i.hasNext(); ) {
                        final Resource resource = (Resource)i.next();
                        this.flog("checking schema for conflicts with %s", resource);
                        final SchemaModel otherSchema;
                        try (BufferedInputStream input = new BufferedInputStream(resource.getInputStream())) {
                            otherSchema = SchemaModel.fromXML(input);
                        } catch (IOException e) {
                            throw new BuildException(String.format("error reading schema from \"%s\": %s", resource, e), e);
                        }
                        final TransactionConfig txConfig = TransactionConfig.builder()
                          .schemaModel(otherSchema)
                          .build();
                        try {
                            db.createTransaction(txConfig).commit();
                        } catch (Exception e) {
                            this.flog("schema conflicts with %s: %s", resource, e);
                            verified = false;
                        }
                    }
                }
            }

            // Set verified property
            if (verify && this.verifiedProperty != null)
                this.getProject().setProperty(this.verifiedProperty, "" + verified);

            // Set auto-generated schema version property
            if (this.schemaIdProperty != null)
                this.getProject().setProperty(this.schemaIdProperty, "" + schemaId);

            // Check verification results
            if (verify && !verified && this.failOnError)
                throw new BuildException("schema verification failed");
        } finally {
            loader.resetThreadContextLoader();
            loader.cleanup();
        }
    }

    private void flog(String format, Object... args) {
        this.log(String.format(format, args));
    }
}
