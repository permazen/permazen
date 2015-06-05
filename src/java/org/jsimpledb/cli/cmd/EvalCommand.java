
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class EvalCommand extends AbstractCommand {

    public EvalCommand() {
        super("eval -f:force expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Evaluates the specified Java expression";
    }

    @Override
    public String getHelpDetail() {
        return "The expression is evaluated within a transaction. If an exception occurs, the transaction is rolled back"
          + " unless the `-f' flag is given, in which case it will be committed anyway.";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Node expr = (Node)params.get("expr");
        final boolean force = params.containsKey("force");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final PrintWriter writer = session.getWriter();
                final Value value;
                final Object result;
                try {
                    result = (value = expr.evaluate(session)).get(session);
                } catch (EvalException e) {
                    if (!force && session.getMode().hasCoreAPI())
                        session.getTransaction().setRollbackOnly();
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

