
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.InvalidSchemaException;
import io.permazen.core.TransactionConfig;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.maven.SchemaUtility;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;

/**
 * Schema utility API.
 *
 * <p>
 * This API is used by the Maven plugin. We do things this way to avoid plugin class loader issues
 * and to ensure that the schema logic applied at build time matches what will be applied at run time.
 */
public class SchemaUtilityImpl implements SchemaUtility {

    private static final String PERMAZEN_CLASS_SCANNER_CLASS_NAME = "io.permazen.spring.PermazenClassScanner";

    private Log log;
    private Permazen pdb;
    private SchemaModel schema;

    @Override
    public String configure(Log log, String[] packageNames, String[] classNames, String encodingRegistryClass)
      throws MojoExecutionException, MojoFailureException {

        // Sanity check
        Preconditions.checkState(this.pdb == null, "already configured");
        Preconditions.checkArgument(log != null, "null log");
        Preconditions.checkArgument(packageNames != null || classNames != null, "no package or class names");

        // Initialize
        this.log = log;

        // Gather model and encoding classes
        final HashSet<Class<?>> modelClasses = new HashSet<>();

        // Do package scanning - requires permazen-spring.jar
        if (packageNames != null && packageNames.length > 0) {

            // Scan for @PermazenType classes using PermazenClassScanner - if available on the classpath
            this.log.info("scanning for @PermazenType annotations in package(s): "
              + Stream.of(packageNames).collect(Collectors.joining(", ")));
            final Class<?> scannerClass = this.loadClass(PERMAZEN_CLASS_SCANNER_CLASS_NAME, name -> String.format(
              "failed to load class \"%s\" required to support <packages> - is permazen-spring.jar a dependency?", name));
            final List<?> scannedClasses;
            try {
                scannedClasses = (List<?>)scannerClass.getMethod("scanForClasses", String[].class)
                  .invoke(scannerClass.getConstructor().newInstance(), (Object)packageNames);
            } catch (ReflectiveOperationException e) {
                throw new MojoExecutionException(String.format("error scanning for classes using %s", scannerClass), e);
            }
            for (Object obj : scannedClasses) {
                final String className = (String)obj;
                this.log.info("adding Permazen model class " + className);
                final Class<?> cl = this.loadClass(className, name -> String.format("failed to load model class \"%s\"", name));
                modelClasses.add(cl);
            }
        }

        // Do specific class scanning
        if (classNames != null && classNames.length > 0) {
            for (String className : classNames) {

                // Load class
                final Class<?> cl = this.loadClass(className, name -> String.format("failed to load class \"%s\"", name));

                // Add model classes
                if (cl.isAnnotationPresent(PermazenType.class)) {
                    this.log.info("adding Permazen model " + cl);
                    modelClasses.add(cl);
                }
            }
        }

        // Instantiate the EncodingRegistry, if any
        EncodingRegistry encodingRegistry = null;
        if (encodingRegistryClass != null) {
            this.log.info("loading encoding registry " + encodingRegistryClass);
            final Class<? extends EncodingRegistry> cl = this.loadClass(encodingRegistryClass, EncodingRegistry.class,
              name -> String.format("failed to load the configured <encodingRegistryClass> \"%s\"", encodingRegistryClass));
            try {
                encodingRegistry = cl.getConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new MojoExecutionException(String.format(
                  "failed to instantiate the configured <encodingRegistryClass> \"%s\"", encodingRegistryClass), e);
            }
        }

        // Construct database and schema model
        this.log.info("generating Permazen schema from schema classes using " + encodingRegistry.getClass().getName());
        try {
            final Database db = new Database(new MemoryKVDatabase());
            if (encodingRegistry != null)
                db.setEncodingRegistry(encodingRegistry);
            this.pdb = PermazenConfig.builder()
              .database(db)
              .modelClasses(modelClasses)
              .build()
              .newPermazen();
            this.pdb.initialize();
            this.schema = this.pdb.getSchemaModel(false);
            this.schema.lockDown(true);
        } catch (Exception e) {
            throw new MojoFailureException(String.format("schema generation failed: %s", e.getMessage()), e);
        }

        // Return schema ID
        final SchemaId schemaId = this.schema.getSchemaId();
        this.log.info("schema ID is \"" + schemaId + "\"");
        return schemaId.toString();
    }

    @Override
    public void generateSchema(File file) throws MojoExecutionException, MojoFailureException {
        Preconditions.checkState(this.pdb != null, "not configured");
        Preconditions.checkArgument(file != null, "null file");

        // Create directory
        final File dir = file.getParentFile();
        if (!dir.exists()) {
            this.log.info("creating directory \"" + dir + "\"");
            try {
                Files.createDirectories(dir.toPath());
            } catch (IOException e) {
                throw new MojoExecutionException(String.format("error creating directory \"%s\": %s", dir, e.getMessage()), e);
            }
        }

        // Write schema model to file
        this.log.info("writing Permazen schema to \"" + file + "\"");
        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file))) {
            this.schema.toXML(output, true);
        } catch (IOException e) {
            throw new MojoExecutionException(String.format("error writing schema to \"%s\": %s", file, e.getMessage()), e);
        }
    }

    @Override
    public boolean verifySchema(File file) throws MojoExecutionException {
        Preconditions.checkState(this.pdb != null, "not configured");
        Preconditions.checkArgument(file != null, "null file");

        // Read file
        this.log.info("verifying Permazen schema matches \"" + file + "\"");
        final SchemaModel verifyModel;
        try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
            verifyModel = SchemaModel.fromXML(input);
        } catch (IOException | InvalidSchemaException e) {
            throw new MojoExecutionException(String.format("error reading schema from \"%s\": %s", file, e), e);
        }
        verifyModel.lockDown(true);

        // Compare
        final boolean matched = this.schema.getSchemaId().equals(verifyModel.getSchemaId());
        if (!matched)
            this.log.error("schema verification failed:\n" + this.schema.differencesFrom(verifyModel));
        else
            this.log.info("schema verification succeeded");

        // Done
        return matched;
    }

    @Override
    public boolean verifySchemas(Iterator<? extends File> files) throws MojoExecutionException {
        Preconditions.checkState(this.pdb != null, "not configured");
        Preconditions.checkArgument(files != null, "null files");

        // Check fiels
        boolean success = true;
        while (files.hasNext()) {
            final File file = files.next();

            // Read next file
            this.log.info("checking schema for conflicts with " + file);
            final SchemaModel otherSchema;
            try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
                otherSchema = SchemaModel.fromXML(input);
            } catch (IOException | InvalidSchemaException e) {
                throw new MojoExecutionException(String.format("error reading schema from \"%s\": %s", file, e), e);
            }

            // Check compatible
            try {
                TransactionConfig.builder()
                  .schemaModel(otherSchema)
                  .build()
                  .newTransaction(this.pdb.getDatabase())
                  .rollback();
            } catch (Exception e) {
                this.log.error("schema conflicts with " + file + ": " + e);
                success = false;
            }
        }

        // Done
        return success;
    }

    private <T> Class<? extends T> loadClass(String className, Class<T> type, UnaryOperator<String> errorGenerator)
      throws MojoExecutionException {
        Preconditions.checkArgument(type != null, "null type");
        final Class<?> cl = this.loadClass(className, errorGenerator);
        try {
            return cl.asSubclass(type);
        } catch (ClassCastException e) {
            throw new MojoExecutionException(String.format(
              "error loading class \"%s\" - expected a subtype of \"%s\"", className, type.getName()), e);
        }
    }

    private Class<?> loadClass(String className, UnaryOperator<String> errorGenerator) throws MojoExecutionException {
        Preconditions.checkArgument(className != null, "null className");
        Preconditions.checkArgument(errorGenerator != null, "null errorGenerator");
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new MojoExecutionException(errorGenerator.apply(className), e);
        }
    }
}
