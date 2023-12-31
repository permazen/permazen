
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.maven;

import io.permazen.Permazen;
import io.permazen.PermazenConfig;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.InvalidSchemaException;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * Generates a schema XML file from a set of Permazen model classes.
 */
public abstract class AbstractSchemaMojo extends AbstractMojo {

    /**
     * Specifies Java package names under which to search for classes with @{@code PermazenType} annotations.
     *
     * <p>
     * If no {@code <classes>} or {@code <packages>} are configured, then by default this plugin
     * searches all classes in the output directory.
     */
    @Parameter
    protected String[] packages;

    /**
     * Specifies the names of specific Java classes to search for @{@code PermazenType} annotations.
     *
     * <p>
     * If no {@code <classes>} or {@code <packages>} are configured, then by default this plugin
     * searches all classes in the output directory.
     */
    @Parameter
    protected String[] classes;

    /**
     * <p>
     * The {@link EncodingRegistry} to use for looking up field encodings.
     * By default, a {@link DefaultEncodingRegistry} is used.
     *
     * <p>
     * To configure a custom {@link EncodingRegistry}, specify its class name like this:
     * <blockquote><pre>
     * &lt;configuration&gt;
     *     &lt;encodingRegistryClass&gt;com.example.MyEncodingRegistry&lt;/encodingRegistryClass&gt;
     *     ...
     * &lt;/configuration&gt;
     * </pre></blockquote>
     * The specified class ({@code com.example.MyEncodingRegistry} in this example) must be available either
     * as part of the project being built or in one of the project's dependencies.
     */
    @Parameter
    protected String encodingRegistryClass;

    /**
     * The name of a Maven property to set to the auto-generated schema ID. This is a string that is
     * a unique hash of the schema and which looks like {@code "Schema_12e983a72e72ed56741ddc45e47d3377"}.
     */
    @Parameter(defaultValue = "")
    protected String schemaIdProperty;

    @Parameter(defaultValue = "${project}", readonly = true)
    protected MavenProject project;

/*
    @Parameter(defaultValue = "${session}", readonly = true)
    protected MavenSession session;

    @Parameter(defaultValue = "${mojoExecution}", readonly = true)
    protected MojoExecution mojo;

    @Parameter(defaultValue = "${plugin}", readonly = true)
    protected PluginDescriptor plugin;

    @Parameter(defaultValue = "${settings}", readonly = true)
    protected Settings settings;

    @Parameter(defaultValue = "${project.build.directory}", readonly = true)
    protected File target;
*/

    protected abstract File getClassOutputDirectory();

    protected abstract void addDependencyClasspathElements(List<String> elements) throws DependencyResolutionRequiredException;

    protected abstract void execute(Permazen jdb) throws MojoExecutionException, MojoFailureException;

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException {

        // Set up class loader that includes project classes and dependencies
        final TreeSet<URL> urls = new TreeSet<>(new Comparator<URL>() {                 // sort URLs to aid in debugging
            @Override
            public int compare(URL url1, URL url2) {
                return url1.toString().compareTo(url2.toString());
            }
        });
        try {
            urls.add(this.getClassOutputDirectory().toURI().toURL());
        } catch (MalformedURLException e) {
            throw new MojoExecutionException("error creating URL from directory \"" + this.getClassOutputDirectory() + "\"", e);
        }
        final ArrayList<String> dependencies = new ArrayList<>();
        try {
            this.addDependencyClasspathElements(dependencies);
        } catch (DependencyResolutionRequiredException e) {
            throw new MojoExecutionException("error gathering dependency classpath elements", e);
        }
        for (String dependency : dependencies) {
            try {
                urls.add(new File(dependency).toURI().toURL());
            } catch (MalformedURLException e) {
                throw new MojoExecutionException("error creating URL from classpath element \"" + dependency + "\"", e);
            }
        }

        // Apply default class names if necessary - which is every class in the output directory
        if (this.classes == null && this.packages == null) {
            final ArrayList<String> classNames = new ArrayList<>();
            final Path dir = this.getClassOutputDirectory().toPath();
            try {
                Files.walkFileTree(this.getClassOutputDirectory().toPath(), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                        path = dir.relativize(path);
                        if (path.getNameCount() == 0 || !path.getName(path.getNameCount() - 1).toString().endsWith(".class"))
                            return FileVisitResult.CONTINUE;
                        final StringBuilder buf = new StringBuilder();
                        for (Path component : path) {
                            if (buf.length() > 0)
                                buf.append('.');
                            buf.append(component.toString());
                        }
                        String name = buf.toString();
                        name = name.substring(0, name.length() - ".class".length());
                        classNames.add(name);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new MojoExecutionException("error walking output directory hierarchy", e);
            }
            this.classes = classNames.toArray(new String[classNames.size()]);
            this.getLog().debug(this.getClass().getSimpleName() + " auto-generated class list: "
              + classNames.toString().replaceAll("(^\\[|, )", "\n  "));
        }

        // Gather model and encoding classes
        this.getLog().debug(this.getClass().getSimpleName() + " classloader setup: "
          + urls.toString().replaceAll("(^\\[|, )", "\n  "));
        final ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
        final ClassLoader loader = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]), parentLoader);
        Thread.currentThread().setContextClassLoader(loader);
        try {
            final HashSet<Class<?>> modelClasses = new HashSet<>();

            // Do package scanning
            if (this.packages != null && this.packages.length > 0) {

                // Join list
                final StringBuilder buf = new StringBuilder();
                for (String packageName : this.packages) {
                    if (buf.length() > 0)
                        buf.append(' ');
                    buf.append(packageName);
                }
                final String packageNames = buf.toString();

                // Scan for @PermazenType classes
                this.getLog().info("scanning for @PermazenType annotations in packages: " + packageNames);
                for (String className : new PermazenClassScanner().scanForClasses(packageNames)) {
                    this.getLog().info("adding Permazen model class " + className);
                    try {
                        modelClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                    } catch (ClassNotFoundException e) {
                        throw new MojoExecutionException("failed to load model class \"" + className + "\"", e);
                    }
                }
            }

            // Do specific class scanning
            if (this.classes != null && this.classes.length > 0) {
                for (String className : this.classes) {

                    // Load class
                    final Class<?> cl;
                    try {
                        cl = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new MojoExecutionException("failed to load class \"" + className + "\"", e);
                    }

                    // Add model classes
                    if (cl.isAnnotationPresent(PermazenType.class)) {
                        this.getLog().info("adding Permazen model " + cl);
                        modelClasses.add(cl);
                    }
                }
            }

            // Instantiate EncodingRegistry
            EncodingRegistry encodingRegistry = null;
            if (encodingRegistryClass != null) {
                try {
                    encodingRegistry = Class.forName(this.encodingRegistryClass, false,
                       Thread.currentThread().getContextClassLoader())
                      .asSubclass(EncodingRegistry.class).getConstructor().newInstance();
                } catch (Exception e) {
                    throw new MojoExecutionException("error instantiatiating the configured <encodingRegistryClass> \""
                      + encodingRegistryClass + "\"", e);
                }
            }

            // Construct database and schema model
            this.getLog().info("generating Permazen schema from schema classes");
            final Permazen jdb;
            final SchemaModel schema;
            try {
                final Database db = new Database(new MemoryKVDatabase());
                if (encodingRegistry != null)
                    db.setEncodingRegistry(encodingRegistry);
                jdb = PermazenConfig.builder()
                  .database(db)
                  .modelClasses(modelClasses)
                  .build()
                  .newPermazen();
                schema = jdb.getSchemaModel();
            } catch (Exception e) {
                throw new MojoFailureException("schema generation failed: " + e, e);
            }
            final SchemaId schemaId = schema.getSchemaId();
            this.getLog().info("schema ID is \"" + schemaId + "\"");

            // Set auto-generated schema ID property
            if (this.schemaIdProperty != null && this.schemaIdProperty.length() > 0)
                this.project.getProperties().setProperty(this.schemaIdProperty, "" + schemaId);

            // Proceed
            this.execute(jdb);
        } finally {
            Thread.currentThread().setContextClassLoader(parentLoader);
        }
    }

    /**
     * Generate schema XML file, overwriting any previous file.
     *
     * @param schema database schema
     * @param file schema XML output file
     * @throws MojoExecutionException if an unexpected error occurs
     */
    protected void generate(SchemaModel schema, File file) throws MojoExecutionException {

        // Create directory
        final File dir = file.getParentFile();
        if (!dir.exists()) {
            this.getLog().info("creating directory \"" + dir + "\"");
            try {
                Files.createDirectories(dir.toPath());
            } catch (IOException e) {
                throw new MojoExecutionException("error creating directory \"" + dir + "\"", e);
            }
        }

        // Write schema model to file
        this.getLog().info("writing Permazen schema to \"" + file + "\"");
        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file))) {
            schema.toXML(output, true);
        } catch (IOException e) {
            throw new MojoExecutionException("error writing schema to \"" + file + "\": " + e, e);
        }
    }

    /**
     * Verify that the provided schema is the sam as the schema defined by the specified XML file.
     *
     * @param schema actual schema
     * @param file expected schema XML file
     * @return true if verification succeeded, otherwise false
     * @throws MojoExecutionException if an unexpected error occurs
     */
    protected boolean verify(SchemaModel schema, File file) throws MojoExecutionException {

        // Read file
        this.getLog().info("verifying Permazen schema matches \"" + file + "\"");
        final SchemaModel verifyModel;
        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
            verifyModel = SchemaModel.fromXML(input);
        } catch (IOException | InvalidSchemaException e) {
            throw new MojoExecutionException("error reading schema from \"" + file + "\": " + e, e);
        }

        // Compare
        final boolean matched = schema.getSchemaId().equals(verifyModel.getSchemaId());
        if (!matched) {
            this.getLog().error("schema verification failed:");
            this.getLog().error(schema.differencesFrom(verifyModel).toString());
        } else
            this.getLog().info("schema verification succeeded");

        // Done
        return matched;
    }

    /**
     * Check schema for structural conflicts with other schemas defined by an iteration of XML files.
     *
     * @param jdb database instance
     * @param otherSchemaFiles iteration of other schema XML files
     * @return true if verification succeeded, otherwise false
     * @throws MojoExecutionException if an unexpected error occurs
     */
    protected boolean verify(Permazen jdb, Iterator<? extends File> otherSchemaFiles) throws MojoExecutionException {
        boolean success = true;
        while (otherSchemaFiles.hasNext()) {

            // Read next file
            final File file = otherSchemaFiles.next();
            this.getLog().info("checking schema for conflicts with " + file);
            final SchemaModel otherSchema;
            try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
                otherSchema = SchemaModel.fromXML(input);
            } catch (IOException | InvalidSchemaException e) {
                throw new MojoExecutionException("error reading schema from \"" + file + "\": " + e, e);
            }

            // Check compatible
            try {
                TransactionConfig.builder()
                  .schemaModel(otherSchema)
                  .build()
                  .newTransaction(jdb.getDatabase())
                  .rollback();
            } catch (Exception e) {
                this.getLog().error("schema conflicts with " + file + ": " + e);
                success = false;
            }
        }
        return success;
    }
}
