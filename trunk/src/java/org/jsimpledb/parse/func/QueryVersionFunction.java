
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.Value;

@Function
public class QueryVersionFunction extends SimpleFunction {

    public QueryVersionFunction() {
        super("queryVersion", 0, 0);
    }

    @Override
    public String getHelpSummary() {
        return "queries the object version index";
    }

    @Override
    public String getUsage() {
        return "queryVersion()";
    }

    @Override
    public String getHelpDetail() {
        return "Queries the index of object versions, returning a map from object version to the set of objects with that version.";
    }

    @Override
    public Value apply(ParseSession session, Value[] params) {
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                return session.hasJSimpleDB() ?
                  JTransaction.getCurrent().queryVersion(JObject.class) :
                  session.getTransaction().queryVersion().asMap();
            }
        };
    }
}

