
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JTransaction;
import org.jsimpledb.parse.CompositeIndexParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.Value;

@Function
public class QueryCompositeIndexFunction extends AbstractQueryFunction {

    private final SpaceParser spaceParser = new SpaceParser();

    public QueryCompositeIndexFunction() {
        super("queryCompositeIndex", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "queries a composite index";
    }

    @Override
    public String getUsage() {
        return this.getName() + "(object-type, index-name, value-type, ...) (JSimpleDB mode only)\n"
          + "       " + this.getName() + "(type-name.index-name)"
          + "       " + this.getName() + "(storage-id)";
    }

    @Override
    public String getHelpDetail() {
        return "Queries a composite index. The object-type is the type of object to be queried, i.e.,"
          + " the object type on which the composite index is defined, or any super-type or sub-type; a strict"
          + " sub-type will cause the returned index to be restricted to that sub-type. The index-name"
          + " is the name of the compositeindex. The value-type(s) are the indexed fields' value type(s);"
          + " in the case of reference fields, a super-type or more restrictive sub-type may also be specified,"
          + " otherwise the field type must exactly match the field."
          + "\n\nThe first form is only valid in JSimpleDB mode; the second and third forms may be used in either JSimpleDB"
          + " mode or Core API mode.";
    }

    @Override
    protected int parseName(ParseSession session, ParseContext ctx, boolean complete) {
        return new CompositeIndexParser().parse(session, ctx, complete).getStorageId();
    }

    @Override
    protected Value apply(ParseSession session, final Class<?> objectType, final String indexName, final Class<?>[] valueTypes) {
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                switch (valueTypes.length) {
                case 2:
                    return JTransaction.getCurrent().queryCompositeIndex(objectType, indexName, valueTypes[0], valueTypes[1]);
                case 3:
                    return JTransaction.getCurrent().queryCompositeIndex(objectType,
                      indexName, valueTypes[0], valueTypes[1], valueTypes[2]);
                // COMPOSITE-INDEX
                default:
                    throw new IllegalArgumentException("wrong number of value types (" + valueTypes.length
                      + ") provided for composite index `" + indexName + "'");
                }
            }
        };
    }
}

