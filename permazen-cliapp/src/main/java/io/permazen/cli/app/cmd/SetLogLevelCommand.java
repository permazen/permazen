
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.app.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.app.CliMain;
import io.permazen.cli.cmd.AbstractCommand;

import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;

public class SetLogLevelCommand extends AbstractCommand {

    public SetLogLevelCommand() {
        super("set-log-level level");
    }

    @Override
    public String getHelpSummary() {
        return String.format("Sets the logging threshold, one of: %s",
          Stream.of(Level.values())
            .sorted()
            .map(Level::name)
            .map(s -> String.format("\"%s\"", s))
            .collect(Collectors.joining(", ")));
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public Session.Action getAction(Session session0, Map<String, Object> params) {
        final String levelName = (String)params.get("level");
        return session -> {
            CliMain.setLogLevel(levelName);
            session.getOutput().println(String.format("Set logging level to %s", levelName));
        };
    }
}
