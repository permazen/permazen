
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.config;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.core.Database;
import io.permazen.core.InvalidSchemaException;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.schema.SchemaModel;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Configuration for a CLI application supporting Core API database interaction.
 */
public class CoreApiCliConfig extends KeyValueCliConfig {

    // Options
    protected OptionSpec<File> schemaFileOption;
    protected OptionSpec<SessionMode> sessionModeOption;
    protected OptionSpec<Void> noNewSchemaOption;
    protected OptionSpec<Void> gcSchemasOption;
    protected OptionSpec<String> encodingRegistryOption;

    // Database
    protected Database db;

    // Internal State
    protected SchemaModel schemaModel;
    protected EncodingRegistry encodingRegistry;
    protected SessionMode sessionMode;
    protected boolean disableNewSchema;
    protected boolean enableGcScemas;

// Options

    /**
     * Configure an {@link OptionParser} with the comand line flags supported by this instance.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     */
    public void addOptions(OptionParser parser) {
        super.addOptions(parser);
        this.addSessionModeOption(parser);
        this.addCoreApiOptions(parser);
    }

    /**
     * Add the {@code --session-mode} command line option.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     * @throws IllegalStateException if option has already been added
     */
    protected void addSessionModeOption(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.sessionModeOption == null, "duplicate option");
        this.sessionModeOption = parser.accepts("session-mode",
            String.format("Session mode, one of: %s (default \"%s\")",
              Stream.of(SessionMode.values())
                .map(SessionModeConverter::toString)
                .map(s -> String.format("\"%s\"", s))
                .collect(Collectors.joining(", ")),
              SessionModeConverter.toString(this.getDefaultSessionMode())))
          .withRequiredArg()
          .describedAs("mode")
          .withValuesConvertedBy(new SessionModeConverter());
    }

    /**
     * Get the default {@link SessionMode}.
     */
    protected SessionMode getDefaultSessionMode() {
        return SessionMode.CORE_API;
    }

    /**
     * Add Core API layer command line flags to the given option parser.
     *
     * @param parser command line flag parser
     * @throws IllegalArgumentException if {@code parser} is null
     * @throws IllegalStateException if an option being added has already been added
     */
    protected void addCoreApiOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.schemaFileOption == null, "duplicate option");
        Preconditions.checkState(this.gcSchemasOption == null, "duplicate option");
        Preconditions.checkState(this.noNewSchemaOption == null, "duplicate option");
        Preconditions.checkState(this.encodingRegistryOption == null, "duplicate option");
        this.schemaFileOption = parser.accepts("schema-file", "Specify core API database schema from XML file")
          .withRequiredArg()
          .describedAs("file")
          .ofType(File.class);
        this.gcSchemasOption = parser.accepts("gc-schemas", "Enable garbage collection of obsolete schemas");
        this.noNewSchemaOption = parser.accepts("no-new-schema", "Disallow recording of new database schemas");
        this.encodingRegistryOption = parser.accepts("encoding-registry", "Specify an EncodingRegistry for custom field encodings")
          .withRequiredArg()
          .describedAs("class-name")
          .withValuesConvertedBy(new JavaNameConverter("class"));
    }

    @Override
    protected void processOptions(OptionSet options) {
        super.processOptions(options);
        Optional.ofNullable(this.encodingRegistryOption)
          .map(options::valueOf)
          .map(className -> this.instantiateClass(EncodingRegistry.class, className))
          .ifPresent(registry -> this.encodingRegistry = registry);
        Optional.ofNullable(this.schemaFileOption)
          .map(options::valueOf)
          .ifPresent(file -> {
            try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
                this.schemaModel = SchemaModel.fromXML(input);
                this.schemaModel.lockDown(true);
            } catch (IOException | InvalidSchemaException e) {
                throw new IllegalArgumentException(String.format("can't load schema from \"%s\": %s", file, e.getMessage()), e);
            }
          });
        this.sessionMode = Optional.ofNullable(this.sessionModeOption)
          .map(options::valueOf)
          .orElseGet(this::getDefaultSessionMode);
        this.disableNewSchema = this.noNewSchemaOption != null && options.has(this.noNewSchemaOption);
        this.enableGcScemas = this.gcSchemasOption != null && options.has(this.gcSchemasOption);
    }

// Database

    @Override
    public void startupDatabase(OptionSet options) {
        Preconditions.checkState(this.db == null, "already started");
        super.startupDatabase(options);

        // Construct core API Database
        this.db = new Database(this.kvdb);
        if (this.encodingRegistry != null)
            this.db.setEncodingRegistry(this.encodingRegistry);
    }

    @Override
    public void shutdownDatabase() {
        super.shutdownDatabase();
        this.db = null;
    }

    @Override
    public Database getDatabase() {
        return this.db;
    }

// Session

    @Override
    protected void configureSession(Session session) {
        super.configureSession(session);
        if (this.schemaModel != null)
            session.setSchemaModel(this.schemaModel);
        session.setMode(this.sessionMode);
        if (this.disableNewSchema)
            session.setAllowNewSchema(false);
        if (this.enableGcScemas)
            session.setGarbageCollectSchemas(true);
    }
}
