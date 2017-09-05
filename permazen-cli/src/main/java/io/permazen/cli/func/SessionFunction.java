
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.func;

import java.util.EnumSet;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.expr.ConstValue;
import io.permazen.parse.expr.Value;

public class SessionFunction extends SimpleCliFunction {

    public SessionFunction() {
        super("session", 0, 0);
    }

    @Override
    public String getHelpSummary() {
        return "Returns the current CLI session Java instance (a " + CliSession.class.getName() + ").";
    }

    @Override
    public String getUsage() {
        return "session()";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected ConstValue apply(CliSession session, Value[] params) {
        return new ConstValue(session);
    }
}

