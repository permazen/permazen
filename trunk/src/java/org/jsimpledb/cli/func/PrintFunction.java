
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import java.io.PrintWriter;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;

@CliFunction
public class PrintFunction extends SimpleFunction {

    public PrintFunction() {
        super("print", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "prints a value followed by newline";
    }

    @Override
    public String getUsage() {
        return "print(expr)";
    }

    @Override
    protected Value apply(Session session, Value[] params) {
        final PrintWriter writer = session.getWriter();
        try {
            writer.println(params[0].get(session));
        } catch (EvalException e) {
            writer.println("Error: " + e.getMessage());
            for (Throwable t = e.getCause(); t != null; t = t.getCause())
                writer.println("Caused by: " + t);
        }
        return new Value(null);
    }
}

