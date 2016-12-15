
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.EnumSet;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.func.Function;
import org.jsimpledb.util.ParseContext;

public class RegisterFunctionCommand extends AbstractCommand {

    public RegisterFunctionCommand() {
        super("register-function class:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Instantiates a user-supplied class implementing the Function interface and registers it as an available function.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Node expr = (Node)params.get("class");
        return session -> {
            final Object result = expr.evaluate(session).get(session);
            if (!(result instanceof Class))
                throw new Exception("invalid parameter: not a " + Class.class.getName() + " instance");
            final Class<?> cl = (Class<?>)result;
            if (!Function.class.isAssignableFrom(cl))
                throw new Exception("invalid parameter: " + cl + " does not implement " + Function.class);
            final Function function = cl.asSubclass(Function.class).getConstructor().newInstance();
            session.registerFunction(function);
            session.getWriter().println("Registered function `" + function.getName() + "'");
        };
    }
}

