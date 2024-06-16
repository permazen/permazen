
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.parse.EnumNameParser;
import io.permazen.cli.parse.Parser;

import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SetSessionModeCommand extends AbstractCommand {

    public SetSessionModeCommand() {
        super("set-session-mode mode:mode");
    }

    @Override
    public String getHelpSummary() {
        return "Sets the CLI session mode";
    }

    @Override
    public String getHelpDetail() {
        return "Changes the current CLI session mode. One of: "
          + Stream.of(SessionMode.values())
              .map(m -> String.format("\"%s\"", m))
              .collect(Collectors.joining(", "));
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "mode".equals(typeName) ? new EnumNameParser<>(SessionMode.class, false) : super.getParser(typeName);
    }

    @Override
    public Session.Action getAction(Session session0, Map<String, Object> params) {
        final SessionMode mode = (SessionMode)params.get("mode");
        return session -> {
            session.setMode(mode);
            session.getOutput().println("Set session mode to " + mode);
        };
    }
}
