
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;

@CliFunction
public class VersionFunction extends SimpleFunction {

    public VersionFunction() {
        super("version", 1, 1);
    }

    @Override
    public String getUsage() {
        return "version(object)";
    }

    @Override
    public String getHelpSummary() {
        return "returns the schema version of an object";
    }

    @Override
    protected Value apply(Session session, Value[] params) {

        // Get object
        final Object obj = params[0].checkNotNull(session, "version()");
        if (!(obj instanceof ObjId))
            throw new EvalException("invalid version() operation on non-database object of type " + obj.getClass().getName());
        final ObjId id = (ObjId)obj;

        // Return version
        try {
            return new Value(session.getTransaction().getSchemaVersion(id));
        } catch (DeletedObjectException e) {
            throw new EvalException("invalid version() operation on non-existent object " + id);
        }
    }
}

