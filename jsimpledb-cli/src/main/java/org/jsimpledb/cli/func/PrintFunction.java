
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.func;

import java.util.EnumSet;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.expr.Value;

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

