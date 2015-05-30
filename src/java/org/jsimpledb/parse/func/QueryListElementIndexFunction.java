
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import com.google.common.collect.Iterables;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.ListField;
import org.jsimpledb.parse.IndexedFieldParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.util.InstancePredicate;

@Function
public class QueryListElementIndexFunction extends AbstractQueryFunction {

    public QueryListElementIndexFunction() {
        super("queryListElementIndex", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "queries the composite index on a list element field that includes the list indicies";
    }

    @Override
    public String getUsage() {
        return this.getName() + "(object-type, field-name, value-type) (JSimpleDB mode only)\n"
          + "       " + this.getName() + "(type-name.field-name)"
          + "       " + this.getName() + "(storage-id)";
    }

    @Override
    public String getHelpDetail() {
        return "Queries the composite index associated with an indexed list element field. The object-type is the type of object"
          + " to be queried, i.e., the object type that contains the list field, or any super-type or sub-type; a strict"
          + " sub-type will cause the returned index to be restricted to that sub-type. The field-name"
          + " is the name of the list field; it must include the `element' sub-field name, e.g., `mylist.element'."
          + " The value-type is the list element value type; in the case of reference fields,"
          + " a super-type or more restrictive sub-type may also be specified, otherwise the type must exactly"
          + " match the list element type."
          + "\n\nThe first form is only valid in JSimpleDB mode; the second and third forms may be used in either JSimpleDB"
          + " mode or Core API mode.";
    }

    @Override
    protected int parseName(ParseSession session, ParseContext ctx, boolean complete) {
        return new IndexedFieldParser() {
            @Override
            protected Iterable<? extends Field<?>> filterFields(Iterable<? extends Field<?>> fields) {
                return Iterables.filter(fields, new InstancePredicate(ListField.class));
            }
        }.parse(session, ctx, complete).getField().getStorageId();
    }

    @Override
    protected Value apply(ParseSession session, final Class<?> objectType, final String fieldName, final Class<?>[] valueTypes) {
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                return JTransaction.getCurrent().queryListElementIndex(objectType, fieldName, valueTypes[0]);
            }
        };
    }
}

