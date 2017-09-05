
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import io.permazen.JObject;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.ConstValue;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Value;

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
        return "Returns the schema version of a database object";
    }

    @Override
    protected Value apply(ParseSession session, Value[] params) {

        // Get object
        Object obj = params[0].checkNotNull(session, "version()");
        if (obj instanceof JObject)
            obj = ((JObject)obj).getObjId();
        else if (!(obj instanceof ObjId))
            throw new EvalException("invalid version() operation on non-database object of type " + obj.getClass().getName());
        final ObjId id = (ObjId)obj;

        // Return version
        try {
            return new ConstValue(session.getTransaction().getSchemaVersion(id));
        } catch (DeletedObjectException e) {
            throw new EvalException("invalid version() operation on non-existent object " + id);
        }
    }
}

