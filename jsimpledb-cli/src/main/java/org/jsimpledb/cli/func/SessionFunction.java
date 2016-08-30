
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.func;

import java.util.EnumSet;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.expr.ConstValue;
import org.jsimpledb.parse.expr.Value;

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

