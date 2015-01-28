
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

@Command
public class EvalCommand extends AbstractCommand {

    public EvalCommand() {
        super("eval expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "evaluates the specified Java expression";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Node expr = (Node)params.get("expr");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final PrintWriter writer = session.getWriter();
                final Value value;
                final Object result;
                try {
                    result = (value = expr.evaluate(session)).get(session);
                } catch (EvalException e) {
                    writer.println("Error: " + e.getMessage());
                    if (session.isVerbose())
                        e.printStackTrace(writer);
                    return;
                }
                if (value != Value.NO_VALUE)
                    writer.println(result);
            }
        };
    }
}

