
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.config;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.PermazenConfig;
import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.schema.SchemaModel;
import io.permazen.spring.PermazenClassScanner;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Function;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Configuration for a CLI application supporting a Permazen database.
 */
public class PermazenCliConfig extends CoreApiCliConfig {

    // Options
    protected OptionSpec<File> classPathOption;
    protected OptionSpec<String> modelClassOption;
    protected OptionSpec<String> modelPackageOption;

    // Database
    protected Permazen pdb;

    // Internal state
    protected final HashSet<Class<?>> modelClasses = new HashSet<>();

// Options

    @Override
    public void addOptions(OptionParser parser) {
        super.addOptions(parser);
        this.addPermazenOptions(parser);
    }

    /**
     * Add {@link Permazen} layer command line flags to the given option parser.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     * @throws IllegalStateException if an option being added has already been added
     */
    protected void addPermazenOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.classPathOption == null, "duplicate option");
        Preconditions.checkState(this.modelPackageOption == null, "duplicate option");
        Preconditions.checkState(this.modelClassOption == null, "duplicate option");
        this.classPathOption = parser.accepts("class-path", "Add to the class path searched for model classes")
          .withRequiredArg()
          .describedAs("file-or-dir")
          .ofType(File.class)
          .withValuesSeparatedBy(File.pathSeparatorChar);
        this.modelPackageOption = parser.acceptsAll(Arrays.asList("p", "model-package"),
            "Specify @PermazenType model Java package(s) to scan")
          .withRequiredArg()
          .withValuesConvertedBy(new JavaNameConverter("package"))
          .withValuesSeparatedBy(',');
        this.modelClassOption = parser.acceptsAll(Arrays.asList("c", "model-class"),
            "Specify @PermazenType model class(es)")
          .withRequiredArg()
          .withValuesConvertedBy(new JavaNameConverter("class"))
          .withValuesSeparatedBy(',');
    }

    @Override
    protected void processOptions(OptionSet options) {

        // Apply "--class-path" option as early as possible
        if (this.classPathOption != null)
            options.valuesOf(this.classPathOption).forEach(this.loader::addFile);

        // Delegate to superclass
        super.processOptions(options);

        // Scan model packages
        if (this.modelPackageOption != null) {
            for (String packageName : options.valuesOf(this.modelPackageOption)) {
                if (!this.scanModelPackage(packageName))
                    this.log.warn("no Java model classes found under package \"{}\"", packageName);
            }
        }

        // Add model classes
        if (this.modelClassOption != null) {
            for (String className : options.valuesOf(this.modelClassOption))
                this.modelClasses.add(this.loadClass(Object.class, className));
        }
    }

    @Override
    protected SessionMode getDefaultSessionMode() {
        return SessionMode.PERMAZEN;
    }

    private boolean scanModelPackage(String packageName) {
        boolean foundAny = false;
        for (String className : new PermazenClassScanner(this.loader).scanForClasses(packageName.split("[\\s,]"))) {
            this.log.debug("loading Java model class {}", className);
            this.modelClasses.add(this.loadClass(Object.class, className));
            foundAny = true;
        }
        return foundAny;
    }

// Database

    @Override
    public void startupDatabase(OptionSet options) {
        Preconditions.checkState(this.pdb == null, "already started");
        super.startupDatabase(options);

        // Build Permazen instance
        this.pdb = PermazenConfig.builder()
          .database(this.db)
          .modelClasses(this.modelClasses)
          .build()
          .newPermazen();

        // Configure schema or verify consistency
        final SchemaModel explicitSchemaModel = this.schemaModel;
        final SchemaModel permazenSchemaModel = this.pdb.getSchemaModel(false);
        if (explicitSchemaModel == null)
            this.schemaModel = permazenSchemaModel;
        else if (!explicitSchemaModel.getSchemaId().equals(permazenSchemaModel.getSchemaId())) {
            final Function<SchemaModel, String> describer = s -> s.isEmpty() ?
              "empty schema" : String.format("schema \"%s\"", s.getSchemaId());
            throw new IllegalArgumentException(String.format(
              "%s read from \"--schema-file\" flag conflicts with %s generated from scanned classes",
              describer.apply(explicitSchemaModel), describer.apply(permazenSchemaModel)));
        }

        // Initialize database (PERMAZEN mode only)
        if (this.sessionMode.equals(SessionMode.PERMAZEN))
            this.pdb.initialize();
    }

    @Override
    public void shutdownDatabase() {
        super.shutdownDatabase();
        this.pdb = null;
    }

    @Override
    public Permazen getPermazen() {
        return this.pdb;
    }

// Session

    @Override
    protected void configureSession(Session session) {
        super.configureSession(session);                // nothing else to do here
    }
}
