
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.util.EnumSet;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.expr.Node;
import io.permazen.parse.func.Function;
import io.permazen.util.ParseContext;

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

            // Evaluate class expression
            final Class<? extends Function> functionClass = RegisterFunctionCommand.this.getExprParam(session, expr, "class",
              obj -> {
                if (!(obj instanceof Class))
                    throw new IllegalArgumentException("not a " + Class.class.getName() + " instance");
                final Class<?> cl = (Class<?>)obj;
                if (!Function.class.isAssignableFrom(cl))
                    throw new IllegalArgumentException(cl + " does not implement " + Function.class);
                return cl.asSubclass(Function.class);
            });

            // Instantiate class to create new function and register function
            final Function function = functionClass.getConstructor().newInstance();
            session.registerFunction(function);
            session.getWriter().println("Registered function `" + function.getName() + "'");
        };
    }
}

