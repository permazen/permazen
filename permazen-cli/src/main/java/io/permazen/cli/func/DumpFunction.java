
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.func;

import java.io.PrintWriter;

import io.permazen.JObject;
import io.permazen.cli.CliSession;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.util.ObjDumper;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Value;

public class DumpFunction extends SimpleCliFunction {

    public DumpFunction() {
        super("dump", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "Prints all fields of the given database object to the console";
    }

    @Override
    public String getUsage() {
        return "dump(expr)";
    }

    @Override
    protected Value apply(CliSession session, Value[] params) {

        // Get object
        Object obj = params[0].checkNotNull(session, "dump()");
        if (obj instanceof JObject)
            obj = ((JObject)obj).getObjId();
        else if (!(obj instanceof ObjId))
            throw new EvalException("invalid dump() operation on non-database object of type " + obj.getClass().getName());
        final ObjId id = (ObjId)obj;

        // Dump object
        this.dump(session, id);

        // Done
        return Value.NO_VALUE;
    }

    private void dump(final CliSession session, final ObjId id) {

        // Get transaction and console writer
        final Transaction tx = session.getTransaction();
        final PrintWriter writer = session.getWriter();

        // Verify object exists
        if (!tx.exists(id)) {
            writer.println("object " + id + " (does not exist)");
            return;
        }

        // Dump info
        ObjDumper.print(writer, tx, id, session.getLineLimit());
    }
}
