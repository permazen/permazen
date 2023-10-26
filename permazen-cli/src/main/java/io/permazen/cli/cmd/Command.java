
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;

import java.util.EnumSet;
import java.util.List;

/**
 * {@link Session} command.
 *
 * @see AbstractCommand
 */
public interface Command {

    /**
     * Get the name of this command.
     *
     * @return command name
     */
    String getName();

    /**
     * Get command usage string.
     *
     * @return command usage string
     */
    String getUsage();

    /**
     * Get summarized help (typically a single line).
     *
     * @return one line command summary
     */
    String getHelpSummary();

    /**
     * Get expanded help (typically multiple lines).
     *
     * @return detailed command description
     */
    String getHelpDetail();

    /**
     * Get the {@link SessionMode}(s) supported by this command.
     *
     * @return set of supported {@link SessionMode}s
     */
    EnumSet<SessionMode> getSessionModes();

    /**
     * Execute this command.
     *
     * @param session CLI session
     * @param name command name
     * @param args command line arguments
     * @return command return value
     * @throws InterruptedException if the current thread is interrupted
     * @throws IllegalArgumentException if any parameter is null
     */
    int execute(Session session, String name, List<String> args) throws InterruptedException;
}
