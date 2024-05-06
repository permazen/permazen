
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.app;

import com.google.common.base.Preconditions;

import io.permazen.cli.config.PermazenCliConfig;
import io.permazen.cli.main.BasicCliMain;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

public class CliMain extends BasicCliMain {

    public static final Level DEFAULT_LOG_LEVEL = Level.INFO;       // Note: keep this consistent with log4j2.xml

// BasicCliMain

    @Override
    protected CliMainConfig buildCliConfig() {
        return new CliMainConfig();
    }

// Main Entry Point

    public static void main(String[] args) {
        BasicCliMain.main(new CliMain(), args);
    }

// Log Level

    public static void setLogLevel(String levelName) {
        final Level level;
        try {
            level = Level.valueOf(levelName.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid logging level \"" + levelName + "\"", e);
        }
        Configurator.setAllLevels("", level);
    }

// CliMainConfig

    private class CliMainConfig extends PermazenCliConfig {

        protected OptionSpec<Level> logLevelOption;
        protected Level logLevel;

        @Override
        public void addOptions(OptionParser parser) {
            super.addOptions(parser);
            Preconditions.checkState(this.logLevelOption == null, "duplicate option");
            this.logLevelOption = parser.accepts("log-level",
                String.format("Log level, one of: %s (default \"%s\")",
                  Stream.of(Level.values())
                    .sorted()
                    .map(Level::name)
                    .map(s -> String.format("\"%s\"", s))
                    .collect(Collectors.joining(", ")),
                  DEFAULT_LOG_LEVEL))
              .withRequiredArg()
              .ofType(Level.class)
              .describedAs("level");
        }

        @Override
        protected void processOptions(OptionSet options) {

            // Process log level as early as possible
            this.logLevel = Optional.ofNullable(this.logLevelOption)
              .map(options::valueOf)
              .orElse(DEFAULT_LOG_LEVEL);
            CliMain.setLogLevel(this.logLevel.name());
            if (this.logLevel.isLessSpecificThan(Level.DEBUG))
                CliMain.this.showErrorStackTraces = true;

            // Proceed
            super.processOptions(options);
            if (!options.nonOptionArguments().isEmpty())
                throw new IllegalArgumentException("this command does not take any parameters");
        }
    }
}
