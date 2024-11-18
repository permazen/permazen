
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.maven;

import java.io.File;
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
import java.util.List;
import java.util.ServiceLoader;
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
     * The {@link io.permazen.encoding.EncodingRegistry} to use for looking up field encodings.
     * By default, a {@link io.permazen.encoding.DefaultEncodingRegistry} is used.
     *
     * <p>
     * To configure a custom {@link io.permazen.encoding.EncodingRegistry}, specify its class name like this:
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

    protected abstract void execute(SchemaUtility schemaUtility) throws MojoExecutionException, MojoFailureException;

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
            throw new MojoExecutionException(String.format(
              "Error creating URL from directory \"%s\"", this.getClassOutputDirectory()), e);
        }
        final ArrayList<String> dependencies = new ArrayList<>();
        try {
            this.addDependencyClasspathElements(dependencies);
        } catch (DependencyResolutionRequiredException e) {
            throw new MojoExecutionException("Error gathering dependency classpath elements", e);
        }
        for (String dependency : dependencies) {
            try {
                urls.add(new File(dependency).toURI().toURL());
            } catch (MalformedURLException e) {
                throw new MojoExecutionException(String.format(
                  "Error creating URL from classpath element \"%s\"", dependency), e);
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
                throw new MojoExecutionException("Error walking output directory hierarchy", e);
            }
            this.classes = classNames.toArray(new String[classNames.size()]);
            if (this.getLog().isDebugEnabled()) {
                this.getLog().debug(String.format("%s auto-generated class list:%n%s",
                  this.getClass().getSimpleName(), this.listOfStrings(classNames)));
            }
        }

        // Gather model and encoding classes
        if (this.getLog().isDebugEnabled()) {
            this.getLog().debug(String.format("%s classloader setup:%n%s",
              this.getClass().getSimpleName(), this.listOfStrings(urls)));
        }
        final ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
        final ClassLoader loader = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]), parentLoader);
        Thread.currentThread().setContextClassLoader(loader);
        try {

            // Find and instantiate SchemaUtility
            final SchemaUtility schemaUtility = ServiceLoader.load(SchemaUtility.class)
              .findFirst()
              .orElseThrow(() -> new MojoExecutionException(String.format(
                "no %s implementation found on classpath - is permazen-main.jar a dependency?", SchemaUtility.class.getName())));

            // Configure SchemaUtility
            final String schemaId = schemaUtility.configure(this.getLog(), this.packages, this.classes, this.encodingRegistryClass);

            // Set auto-generated schema ID property
            if (this.schemaIdProperty != null && this.schemaIdProperty.length() > 0)
                this.project.getProperties().setProperty(this.schemaIdProperty, schemaId);

            // Perform task
            this.execute(schemaUtility);
        } finally {
            Thread.currentThread().setContextClassLoader(parentLoader);
        }
    }

    private String listOfStrings(Iterable<?> items) {
        final StringBuilder buf = new StringBuilder();
        for (Object item : items) {
            if (buf.length() > 0)
                buf.append("\n");
            buf.append("  ").append(item);
        }
        return buf.toString();
    }
}
