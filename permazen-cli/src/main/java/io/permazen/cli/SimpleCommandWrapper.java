
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import io.permazen.cli.cmd.Command;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.dellroad.jct.core.ConsoleSession;
import org.dellroad.jct.core.simple.SimpleCommand;

/**
 * Wraps a Permazen {@link Command}, making it into a Java Console Toolkit {@link SimpleCommand}.
 */
public class SimpleCommandWrapper implements SimpleCommand {

    private final Command command;

    /**
     * Constructor.
     *
     * @param command wrapped command
     * @throws IllegalArgumentException if {@code command} is null
     */
    public SimpleCommandWrapper(Command command) {
        Preconditions.checkArgument(command != null, "null command");
        this.command = command;
    }

    /**
     * Get the underlying {@link Command}.
     *
     * @return wrapped command
     */
    public Command getCommand() {
        return this.command;
    }

// SimpleCommand

    @Override
    public String getUsage(String name) {
        return this.command.getUsage();
    }

    @Override
    public String getHelpSummary(String name) {
        return this.command.getHelpSummary();
    }

    @Override
    public String getHelpDetail(String name) {
        final StringWriter buf = new StringWriter();
        try (PrintWriter writer = new PrintWriter(buf)) {
            writer.println(this.command.getHelpDetail());
            writer.println("Supported session modes: "
              + this.command.getSessionModes().toString().replaceAll("\\[(.*)\\]", "$1"));
        }
        return buf.toString();
    }

    @Override
    public int execute(ConsoleSession<?, ?> consoleSession, String name, List<String> args) throws InterruptedException {
        final Session session = ((HasPermazenSession)consoleSession).getPermazenSession();
        return this.command.execute(session, name, args);
    }
}
