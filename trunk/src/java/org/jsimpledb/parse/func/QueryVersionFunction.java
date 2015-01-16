
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.CoreIndex;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.parse.ObjTypeParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

@Function
public class QueryVersionFunction extends AbstractFunction {

    public QueryVersionFunction() {
        super("queryVersion");
    }

    @Override
    public String getHelpSummary() {
        return "queries the object version index";
    }

    @Override
    public String getUsage() {
        return "queryVersion([object-type])";
    }

    @Override
    public String getHelpDetail() {
        return "Queries the index of object versions, returning a map from object version to the set of objects with that version."
          + " An optional object type restricts returned objects.";
    }

    // Returns either null, ObjType, or Node
    @Override
    public Object parseParams(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Check existence of parameter
        if (ctx.tryLiteral(")"))
            return null;

        // Attempt to parse either type name or Java expression (hopefully evaluating to Class<?>)
        Object result;
        final int typeStart = ctx.getIndex();
        try {
            result = new ObjTypeParser().parse(session, ctx, complete);
            ctx.skipWhitespace();
            if (!ctx.tryLiteral(")"))
                throw new ParseException(ctx);
        } catch (ParseException e) {
            ctx.setIndex(typeStart);
            result = this.parseExpressionParams(session, ctx, complete, 0, 1, 1)[0];
        }

        // Done
        return result;
    }

    @Override
    public Value apply(ParseSession session, final Object param) {
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                if (session.hasJSimpleDB()) {
                    final Class<?> type =
                      param instanceof ObjType ?
                        session.getJSimpleDB().getJClass(((ObjType)param).getStorageId()).getType() :
                      param instanceof Node ?
                        ((Node)param).evaluate(session).checkType(session, QueryVersionFunction.this.getName(), Class.class) :
                        Object.class;
                    return JTransaction.getCurrent().queryVersion(type);
                } else {
                    CoreIndex<Integer, ObjId> index = session.getTransaction().queryVersion();
                    final int storageId =
                      param instanceof Node ?
                        ((Node)param).evaluate(session).checkType(session, QueryVersionFunction.this.getName(), Integer.class) :
                      param instanceof ObjType ?
                        ((ObjType)param).getStorageId() :
                        -1;
                    if (storageId != -1)
                        index = index.filter(1, new KeyRanges(ObjId.getKeyRange(storageId)));
                    return index.asMap();
                }
            }
        };
    }
}

