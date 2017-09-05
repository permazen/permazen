
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.ConstValue;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Value;

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
        return "Updates a database object's schema version if necessary, returning true if an update occurred";
    }

    @Override
    protected Value apply(ParseSession session, Value[] params) {

        // Get object
        Object obj = params[0].checkNotNull(session, "upgrade()");
        if (obj instanceof JObject)
            obj = ((JObject)obj).getObjId();
        else if (!(obj instanceof ObjId))
            throw new EvalException("invalid upgrade() operation on non-database object of type " + obj.getClass().getName());
        final ObjId id = (ObjId)obj;

        // Upgrade object
        try {
            return new ConstValue(session.getMode().hasJSimpleDB() ?
              JTransaction.getCurrent().get(id).upgrade() : session.getTransaction().updateSchemaVersion(id));
        } catch (DeletedObjectException e) {
            throw new EvalException("invalid upgrade() operation on non-existent object " + id);
        }
    }
}

