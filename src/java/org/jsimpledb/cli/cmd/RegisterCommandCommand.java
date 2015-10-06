
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.expr.Node;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class RegisterCommandCommand extends AbstractCommand {

    public RegisterCommandCommand() {
        super("register-command class:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Instantiates a user-supplied class subclassing AbstractCommand and registers it as an available CLI command.";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Node expr = (Node)params.get("expr");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final Object result = expr.evaluate(session).get(session);
                if (!(result instanceof Class))
                    throw new Exception("invalid parameter: not a " + Class.class.getName() + " instance");
                final AbstractCommand command = session.registerCommand((Class<?>)result);
                session.getWriter().println("Registered command `" + command.getName() + "'");
            }
        };
    }
}

