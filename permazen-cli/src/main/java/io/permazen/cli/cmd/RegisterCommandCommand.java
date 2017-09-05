
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.expr.Node;
import io.permazen.util.ParseContext;

import java.util.EnumSet;
import java.util.Map;

public class RegisterCommandCommand extends AbstractCommand {

    public RegisterCommandCommand() {
        super("register-command class:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Instantiates a user-supplied class implementing the Command interface and registers it as an available command.";
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
            final Class<? extends Command> commandClass = RegisterCommandCommand.this.getExprParam(session, expr, "class", obj -> {
                if (!(obj instanceof Class))
                    throw new IllegalArgumentException("not a " + Class.class.getName() + " instance");
                final Class<?> cl = (Class<?>)obj;
                if (!Command.class.isAssignableFrom(cl))
                    throw new IllegalArgumentException(cl + " does not implement " + Command.class);
                return cl.asSubclass(Command.class);
            });

            // Instantiate class to create new command and register command
            final Command command = commandClass.getConstructor().newInstance();
            session.registerCommand(command);
            session.getWriter().println("Registered command `" + command.getName() + "'");
        };
    }
}

