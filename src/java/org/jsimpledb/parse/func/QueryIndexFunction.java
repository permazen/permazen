
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JTransaction;
import org.jsimpledb.parse.IndexedFieldParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.Value;

@Function
public class QueryIndexFunction extends AbstractQueryFunction {

    public QueryIndexFunction() {
        super("queryIndex", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "Queries the index associated with an indexed field";
    }

    @Override
    public String getUsage() {
        return this.getName() + "(object-type, field-name, value-type) (JSimpleDB mode only)\n"
          + "       " + this.getName() + "(type-name.field-name)\n"
          + "       " + this.getName() + "(storage-id)";
    }

    @Override
    public String getHelpDetail() {
        return "Queries the simple index associated with an indexed field. The object-type is the type of object to be"
          + " queried, i.e., the object type that contains the indexed field, or any super-type or sub-type; a strict"
          + " sub-type will cause the returned index to be restricted to that sub-type. The field-name"
          + " is the name of the field to query; for collection fields, this must include the sub-field name, e.g.,"
          + " `mylist.element' or `mymap.value'. The value-type is the field's value type; in the case of reference fields,"
          + " a super-type or more restrictive sub-type may also be specified, otherwise the field type must exactly"
          + " match the field."
          + "\n\nThe first form is only valid in JSimpleDB mode; the second and third forms may be used in either JSimpleDB"
          + " mode or Core API mode.";
    }

    @Override
    protected int parseName(ParseSession session, ParseContext ctx, boolean complete) {
        return new IndexedFieldParser().parse(session, ctx, complete).getField().getStorageId();
    }

    @Override
    protected Value apply(ParseSession session, final Class<?> objectType, final String fieldName, final Class<?>[] valueTypes) {
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                return JTransaction.getCurrent().queryIndex(objectType, fieldName, valueTypes[0]);
            }
        };
    }
}

