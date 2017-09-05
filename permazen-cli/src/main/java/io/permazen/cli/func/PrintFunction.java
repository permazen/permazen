
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.func;

import java.util.EnumSet;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.expr.Value;

public class PrintFunction extends SimpleCliFunction {

    public PrintFunction() {
        super("print", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "Prints a value followed by newline";
    }

    @Override
    public String getUsage() {
        return "print(expr)";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected Value apply(CliSession session, Value[] params) {
        session.getWriter().println(params[0].get(session));
        return Value.NO_VALUE;
    }
}

