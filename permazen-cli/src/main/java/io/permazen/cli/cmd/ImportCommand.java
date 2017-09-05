
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.util.EnumSet;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.ParseException;
import io.permazen.util.ParseContext;

public class ImportCommand extends AbstractCommand {

    public ImportCommand() {
        super("import name");
    }

    @Override
    public String getHelpSummary() {
        return "Adds a package name to the search path for unqualified Java class names";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String name = (String)params.get("name");
        if (!name.matches("(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)+"
          + "(\\*|\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)"))
            throw new ParseException(ctx, "invalid Java import `" + name + "'");
        return session -> session.getImports().add(name);
    }
}

