
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JObject;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.ConstValue;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.Value;

@Function
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

