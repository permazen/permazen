
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.Map;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Node;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.util.ParseContext;

@CliCommand
public class EvalCommand extends Command {

    public EvalCommand() {
        super("eval expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "evaluates the specified expression and displays the result if not null";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Node expr = (Node)params.get("expr");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final PrintWriter writer = session.getWriter();
                final Value value;
                final Object result;
                try {
                    result = (value = expr.evaluate(session)).get(session);
                } catch (EvalException e) {
                    writer.println("Error: " + e.getMessage());
                    for (Throwable t = e.getCause(); t != null; t = t.getCause())
                        writer.println("Caused by: " + t);
                    return;
                }
                if (value != Value.NO_VALUE)
                    writer.println(result);
            }
        };
    }
}

