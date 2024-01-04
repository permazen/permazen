
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.app;

import com.google.common.base.Preconditions;

import io.permazen.cli.PermazenShell;
import io.permazen.cli.PermazenShellSession;
import io.permazen.cli.Session;
import io.permazen.cli.config.PermazenCliConfig;
import io.permazen.cli.jshell.PermazenJShellCommand;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import joptsimple.OptionSet;

import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.CommandBundle;
import org.dellroad.jct.core.simple.SimpleShellRequest;
import org.dellroad.jct.core.util.ConsoleUtil;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class CliMain {

    private static CliMain instance;

    private Session cliSession;

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

        // Build CLI config
        final PermazenCliConfig config = new PermazenCliConfig() {
            @Override
            protected void processOptions(OptionSet options) {
                super.processOptions(options);
                if (!options.nonOptionArguments().isEmpty())
                    throw new IllegalArgumentException("this command does not take any parameters");
            }
        };
        try {
            if (!config.startup(System.out, System.err, -1, args))
                return 0;
        } catch (IllegalArgumentException e) {
            System.err.println(String.format("%s: %s", this.getErrorPrefix(), e.getMessage()));
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
            return 1;
        }

        // Create shell session
        final PermazenShellSession shellSession;
        try {
            shellSession = shell.newShellSession(new SimpleShellRequest(terminal, Collections.emptyList(), System.getenv()));
        } catch (IOException e) {
            System.err.println(String.format("%s: error creating %s session: %s", this.getErrorPrefix(), "shell", e));
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
}
