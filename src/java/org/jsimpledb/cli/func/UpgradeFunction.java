
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.core.ObjId;

@CliFunction
public class UpgradeFunction extends SimpleFunction {

    public UpgradeFunction() {
        super("upgrade", 1, 1);
    }

    @Override
    public String getUsage() {
        return "upgrade(object)";
    }

    @Override
    public String getHelpSummary() {
        return "updates an object's schema version";
    }

    @Override
    protected Value apply(Session session, Value[] params) {

        // Get object
        final Object obj = params[0].checkNotNull(session, "upgrade()");
        if (!(obj instanceof ObjId))
            throw new EvalException("invalid upgrade() operation on non-database object of type " + obj.getClass().getName());
        final ObjId id = (ObjId)obj;

        // Upgrade object
        session.getTransaction().updateSchemaVersion(id);

        // Done
        return new Value(id);
    }
}

