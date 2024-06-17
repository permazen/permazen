
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.main;

import com.google.common.base.Preconditions;

import io.permazen.cli.PermazenShell;
import io.permazen.cli.PermazenShellSession;
import io.permazen.cli.config.CliConfig;
import io.permazen.cli.config.PermazenCliConfig;
import io.permazen.cli.jshell.PermazenJShellCommand;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.CommandBundle;
import org.dellroad.jct.core.simple.SimpleShellRequest;
import org.dellroad.jct.core.util.ConsoleUtil;
import org.dellroad.jct.jshell.command.JShellCommand;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * A "main" class for Permazen CLI applications.
 */
public class BasicCliMain {

    /**
     * Determines whether full stack traces should be displayed when exceptions occur.
     */
    protected boolean showErrorStackTraces;

// Public Methods

    /**
     * Get the prefix to use when displaying errors.
     *
     * <p>
     * The implementation in {@link BasicCliMain} returns {@code "Error"}.
     */
    public String getErrorPrefix() {
        return "Error";
    }

    /**
     * Execute the CLI application.
     *
     * @param args command line arguments
     * @return application exit value (0 = success, non-zero = error)
     */
    public int run(String[] args) {

        // Try to detect "--verbose" flag early
        this.showErrorStackTraces |= Stream.of(args).anyMatch(s -> s.matches("-v|--verbose"));

        // Build CLI config
        final CliConfig config = this.buildCliConfig();
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
        commandBundles.forEach(bundle -> bundle.computeIfPresent("jshell", (name, value) -> this.buildJShellCommand()));

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

// Subclass Hooks

    /**
     * Build a {@link CliConfig} to use.
     *
     * <p>
     * The implementation in {@link BasicCliMain} returns a new {@link PermazenCliConfig}.
     * Subclasses can override this method to restrict to lower layers (e.g., Core API),
     * add additional command-line flags, etc.
     *
     * @return CLI configuration
     */
    protected CliConfig buildCliConfig() {
        return new PermazenCliConfig();
    }

    /**
     * Build a {@link JShellCommand} to use as the "jshell" command implementation.
     *
     * <p>
     * The implementation in {@link BasicCliMain} returns a new {@link PermazenJShellCommand}.
     * Subclasses can override this method to add additional configuration, startup scripts, etc.
     *
     * @return "jshell" command, or null for the default
     */
    protected JShellCommand buildJShellCommand() {
        return new PermazenJShellCommand();
    }

// Main Entry Point

    /**
     * Main entry point for a CLI application using the given {@link BasicCliMain} instance.
     *
     * <p>
     * This method never returns; instead, it invokes {@link System#exit} with an appropriate exit code.
     *
     * @param main CLI application
     */
    public static void main(BasicCliMain main, String[] args) {

        // Sanity check
        Preconditions.checkState(main != null, "null main");
        Preconditions.checkState(args != null, "null args");

        // Create and execute program
        int exitValue;
        try {
            exitValue = main.run(args);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            exitValue = 1;
        }

        // Exit with exit value
        System.exit(exitValue);
    }

    /**
     * Main entry point for a {@link BasicCliMain} CLI application.
     */
    public static void main(String[] args) {
        BasicCliMain.main(new BasicCliMain(), args);
    }
}
