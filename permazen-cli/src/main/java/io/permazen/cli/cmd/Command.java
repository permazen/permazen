
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.Parser;

import java.util.EnumSet;

/**
 * {@link io.permazen.cli.CliSession} command.
 *
 * @see AbstractCommand
 */
public interface Command extends Parser<CliSession.Action> {

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
}
