
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.app;

import com.google.common.base.Preconditions;

import io.permazen.cli.PermazenShell;
import io.permazen.cli.PermazenShellSession;
import io.permazen.cli.config.PermazenCliConfig;
import io.permazen.cli.jshell.PermazenJShellCommand;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.CommandBundle;
import org.dellroad.jct.core.simple.SimpleShellRequest;
import org.dellroad.jct.core.util.ConsoleUtil;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class CliMain {

    public static final Level DEFAULT_LOG_LEVEL = Level.INFO;       // Note: keep this consistent with log4j2.xml

    private static CliMain instance;

    private boolean showErrorStackTraces;

// Constructor

    public CliMain() {

        // Disallow more than one instance
        synchronized (CliMain.class) {
            Preconditions.checkState(CliMain.instance == null, "duplicate singleton");
            CliMain.instance = this;
        }
    }

// Singleton Accessor

    public static CliMain getInstance() {
        return CliMain.instance;
    }

// Public Methods

    public String getErrorPrefix() {
        return "Error";
    }

    public int run(String[] args) {

        // Try to detect "--verbose" flag early
        this.showErrorStackTraces |= Stream.of(args).anyMatch(s -> s.matches("-v|--verbose"));

        // Build CLI config
        final CliMainConfig config = new CliMainConfig();
        try {
            if (!config.startup(System.out, System.err, -1, args))
                return 0;
        } catch (IllegalArgumentException e) {
            final String description = Optional.ofNullable(e.getMessage()).orElseGet(e::toString);
            System.err.println(String.format("%s: %s", this.getErrorPrefix(), description));
            if (this.showErrorStackTraces)
                e.printStackTrace(System.err);
            return 1;
        }

        // Create Permazen shell
        final PermazenShell shell = config.newPermazenShell();

        // Find CLI command bundles on the classpath
        final List<CommandBundle> commandBundles = CommandBundle.scanAndGenerate().collect(Collectors.toList());

        // Replace standard "jshell" command (if present) with our custom version
        commandBundles.forEach(bundle -> bundle.computeIfPresent("jshell", (name, value) -> new PermazenJShellCommand()));

        // Register CLI command bundles with the shell
        shell.getCommandBundles().addAll(commandBundles);

        // Create system terminal
        final AtomicReference<ShellSession> shellSessionRef = new AtomicReference<>();
        final Terminal terminal;
        try {
            terminal = TerminalBuilder.builder()
              .name("JCT")
              .system(true)
              .nativeSignals(true)
              .signalHandler(ConsoleUtil.interrruptHandler(shellSessionRef::get, Terminal.SignalHandler.SIG_DFL))
              .build();
        } catch (IOException e) {
            System.err.println(String.format("%s: error creating system terminal: %s", this.getErrorPrefix(), e));
            if (this.showErrorStackTraces)
                e.printStackTrace(System.err);
            return 1;
        }

        // Create shell session
        final PermazenShellSession shellSession;
        try {
            shellSession = shell.newShellSession(new SimpleShellRequest(terminal, Collections.emptyList(), System.getenv()));
        } catch (IOException e) {
            System.err.println(String.format("%s: error creating %s session: %s", this.getErrorPrefix(), "shell", e));
            if (this.showErrorStackTraces)
                e.printStackTrace(System.err);
            return 1;
        }
        shellSessionRef.set(shellSession);

        // Execute shell session
        try {
            return shellSession.execute();
        } catch (InterruptedException e) {
            System.err.println(String.format("%s: ", this.getErrorPrefix(), "interrupted"));
            return 1;
        }
    }

// Main Entry Point

    public static void main(String[] args) {

        // Create and execute program
        int exitValue;
        try {
            exitValue = new CliMain().run(args);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exitValue = 1;
        }

        // Exit with exit value
        System.exit(exitValue);
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
