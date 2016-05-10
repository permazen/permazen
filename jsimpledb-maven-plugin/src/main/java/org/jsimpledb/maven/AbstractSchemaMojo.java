
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.maven;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.jsimpledb.DefaultStorageIdGenerator;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.StorageIdGenerator;
import org.jsimpledb.annotation.JFieldType;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.spring.JSimpleDBClassScanner;
import org.jsimpledb.spring.JSimpleDBFieldTypeScanner;

/**
 * Generates a schema XML file from a set of JSimpleDB model classes.
 */
public abstract class AbstractSchemaMojo extends AbstractMojo {

    /**
     * Specifies Java package names under which to search for classes with
     * {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass} or
     * {@link org.jsimpledb.annotation.JFieldType &#64;JFieldType} annotations.
     */
    @Parameter
    protected String[] packages;

    /**
     * Specifies the names of specific Java classes to search for {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}
     * or {@link org.jsimpledb.annotation.JFieldType &#64;JFieldType} annotations.
     *
     * <p>
     * If no {@code &lt;classes&gt;} or {@code &lt;packages&gt;} are configured, then by default this plugin
     * searches all classes in the output directory.
     */
    @Parameter
    protected String[] classes;

    /**
     * <p>
     * The {@link StorageIdGenerator} to use for generating JSimpleDB storage ID's.
     * By default, a {@link DefaultStorageIdGenerator} is used.
     *
     * <p>
     * To configure a custom {@link StorageIdGenerator}, specify its class name like this:
     * <blockquote><pre>
     * &lt;configuration&gt;
     *     &lt;storageIdGeneratorClass&gt;com.example.MyStorageIdGenerator&lt;/storageIdGeneratorClass&gt;
     *     ...
     * &lt;/configuration&gt;
     * </pre></blockquote>
     * The specified class ({@code com.example.MyStorageIdGenerator} in this example) must be available either
     * as part of the project being built or in one of the project's dependencies.
     */
    @Parameter
    protected String storageIdGeneratorClass;

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

    protected abstract void execute(JSimpleDB jdb) throws MojoExecutionException, MojoFailureException;

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException {

        // Set up class loader that includes project classes and dependencies
        final HashSet<URL> urls = new HashSet<>();
        try {
            urls.add(this.getClassOutputDirectory().toURI().toURL());
        } catch (MalformedURLException e) {
            throw new MojoExecutionException("error creating URL from directory `" + this.getClassOutputDirectory() + "'", e);
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
                throw new MojoExecutionException("error creating URL from classpath element `" + dependency + "'", e);
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
                        classNames.add(buf.toString());
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new MojoExecutionException("error walking output directory hierarchy", e);
            }
            this.classes = classNames.toArray(new String[classNames.size()]);
        }

        // Gather model and field type classes
        final ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
        final ClassLoader loader = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]), parentLoader);
        Thread.currentThread().setContextClassLoader(loader);
        try {
            final HashSet<Class<?>> modelClasses = new HashSet<>();
            final HashSet<Class<?>> fieldTypeClasses = new HashSet<>();

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

                // Scan for @JSimpleClass classes
                this.getLog().info("scanning for @JSimpleClass annotations in packages: " + packageNames);
                for (String className : new JSimpleDBClassScanner().scanForClasses(packageNames)) {
                    this.getLog().info("adding JSimpleDB model class " + className);
                    try {
                        modelClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                    } catch (ClassNotFoundException e) {
                        throw new MojoExecutionException("failed to load model class `" + className + "'", e);
                    }
                }

                // Scan for @JFieldType classes
                this.getLog().info("scanning for @JFieldType annotations in packages: " + packageNames);
                for (String className : new JSimpleDBFieldTypeScanner().scanForClasses(packageNames)) {
                    this.getLog().info("adding JSimpleDB field type class `" + className + "'");
                    try {
                        fieldTypeClasses.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                    } catch (Exception e) {
                        throw new MojoExecutionException("failed to load field type class `" + className + "'", e);
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
                        throw new MojoExecutionException("failed to load class `" + className + "'", e);
                    }

                    // Add model classes
                    if (cl.isAnnotationPresent(JSimpleClass.class)) {
                        this.getLog().info("adding JSimpleDB model " + cl);
                        modelClasses.add(cl);
                    }

                    // Add field types
                    if (cl.isAnnotationPresent(JFieldType.class)) {
                        this.getLog().info("adding JSimpleDB field type " + cl);
                        fieldTypeClasses.add(cl);
                    }
                }
            }

            // Instantiate StorageIdGenerator
            final StorageIdGenerator storageIdGenerator;
            if (storageIdGeneratorClass != null) {
                try {
                    storageIdGenerator = Class.forName(this.storageIdGeneratorClass,
                      false, Thread.currentThread().getContextClassLoader()).asSubclass(StorageIdGenerator.class).newInstance();
                } catch (Exception e) {
                    throw new MojoExecutionException("error instantiatiating the configured <storageIdGeneratorClass> `"
                      + storageIdGeneratorClass + "'", e);
                }
            } else
                storageIdGenerator = new DefaultStorageIdGenerator();

            // Set up database
            final Database db = new Database(new SimpleKVDatabase());

            // Instantiate and configure field type classes
            for (Class<?> cl : fieldTypeClasses) {

                // Instantiate field types
                this.getLog().info("instantiating " + cl + " as field type instance");
                final FieldType<?> fieldType;
                try {
                    fieldType = this.asFieldTypeClass(cl).newInstance();
                } catch (Exception e) {
                    throw new MojoExecutionException("failed to instantiate class `" + cl.getName() + "'", e);
                }

                // Add field type
                try {
                    db.getFieldTypeRegistry().add(fieldType);
                } catch (Exception e) {
                    throw new MojoExecutionException("failed to register custom field type class `" + cl.getName() + "'", e);
                }
            }

            // Set up factory
            final JSimpleDBFactory factory = new JSimpleDBFactory();
            factory.setDatabase(db);
            factory.setSchemaVersion(1);
            factory.setStorageIdGenerator(storageIdGenerator);
            factory.setModelClasses(modelClasses);

            // Construct database and schema model
            this.getLog().info("generating JSimpleDB schema from schema classes");
            final JSimpleDB jdb;
            final SchemaModel schema;
            try {
                jdb = factory.newJSimpleDB();
                schema = jdb.getSchemaModel();
            } catch (Exception e) {
                throw new MojoFailureException("schema generation failed: " + e, e);
            }

            // Record schema model in database as version 1
            db.createTransaction(schema, 1, true).commit();

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
            this.getLog().info("creating directory `" + dir + "'");
            try {
                Files.createDirectories(dir.toPath());
            } catch (IOException e) {
                throw new MojoExecutionException("error creating directory `" + dir + "'", e);
            }
        }

        // Write schema model to file
        this.getLog().info("writing JSimpleDB schema to `" + file + "'");
        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file))) {
            schema.toXML(output, true);
        } catch (IOException e) {
            throw new MojoExecutionException("error writing schema to `" + file + "': " + e, e);
        }
    }

    /**
     * Verify that the provided schema matches the schema defined by the specified XML file.
     *
     * @param schema actual schema
     * @param file expected schema XML file
     * @param matchNames whether names must match, or only storage ID's
     * @return true if verification succeeded, otherwise false
     * @throws MojoExecutionException if an unexpected error occurs
     */
    protected boolean verify(SchemaModel schema, File file, boolean matchNames) throws MojoExecutionException {

        // Read file
        this.getLog().info("verifying JSimpleDB schema matches `" + file + "'");
        final SchemaModel verifyModel;
        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
            verifyModel = SchemaModel.fromXML(input);
        } catch (IOException | InvalidSchemaException e) {
            throw new MojoExecutionException("error reading schema from `" + file + "': " + e, e);
        }

        // Compare
        final boolean matched = matchNames ? schema.equals(verifyModel) : schema.isCompatibleWith(verifyModel);
        if (!matched) {
            this.getLog().error("schema verification failed:");
            this.getLog().error(schema.differencesFrom(verifyModel).toString());
        } else
            this.getLog().info("schema verification succeeded");

        // Done
        return matched;
    }

    /**
     * Check schema for hard conflicts with other schema versions defined by an interation of XML files.
     *
     * @param jdb database instance
     * @param otherVersionFiles iteration of other schema version XML files
     * @return true if verification succeeded, otherwise false
     * @throws MojoExecutionException if an unexpected error occurs
     */
    protected boolean verify(JSimpleDB jdb, Iterator<? extends File> otherVersionFiles) throws MojoExecutionException {
        boolean success = true;
        while (otherVersionFiles.hasNext()) {
            final File file = otherVersionFiles.next();
            this.getLog().info("checking schema for conflicts with " + file);
            final SchemaModel otherSchema;
            try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
                otherSchema = SchemaModel.fromXML(input);
            } catch (IOException | InvalidSchemaException e) {
                throw new MojoExecutionException("error reading schema from `" + file + "': " + e, e);
            }
            try {
                jdb.getDatabase().createTransaction(otherSchema, 2, true).rollback();
            } catch (Exception e) {
                this.getLog().error("schema conflicts with " + file + ": " + e);
                success = false;
            }
        }
        return success;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends FieldType<?>> asFieldTypeClass(Class<?> klass) throws MojoExecutionException {
        try {
            return (Class<? extends FieldType<?>>)klass.asSubclass(FieldType.class);
        } catch (ClassCastException e) {
            throw new MojoExecutionException("invalid @" + JFieldType.class.getSimpleName() + " annotation on "
              + klass + ": type is not a subclass of " + FieldType.class);
        }
    }
}

/* %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

        // Get specified files
        final ArrayList<String> files = new ArrayList<>();
        final FileSetManager fileSetManager = new FileSetManager(this.getLog());
        if (this.fileSets == null) {

            // Apply default: all classes in output directory
            final FileSet fileSet = new FileSet();
            fileSet.setDirectory(this.getClassOutputDirectory());

        for (FileSet fileSet : this.fileSets
        String[] includedFiles = fileSetManager.getIncludedFiles( fileset );
*/

