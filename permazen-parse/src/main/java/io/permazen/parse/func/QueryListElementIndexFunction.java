
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import io.permazen.JTransaction;
import io.permazen.core.Field;
import io.permazen.core.ListField;
import io.permazen.parse.IndexedFieldParser;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.AbstractValue;
import io.permazen.parse.expr.Value;
import io.permazen.util.ParseContext;

import java.util.function.Predicate;

public class QueryListElementIndexFunction extends AbstractQueryFunction {

    public QueryListElementIndexFunction() {
        super("queryListElementIndex", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "Queries the composite index on a list element field that includes the list indices";
    }

    @Override
    public String getUsage() {
        return this.getName() + "(object-type, field-name, value-type) (Permazen mode only)\n"
          + "       " + this.getName() + "(type-name.field-name)\n"
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
          + "\n\nThe first form is only valid in Permazen mode; the second and third forms may be used in either Permazen"
          + " mode or Core API mode.";
    }

    @Override
    protected int parseName(ParseSession session, ParseContext ctx, boolean complete) {
        return new IndexedFieldParser() {
            @Override
            protected Predicate<Field<?>> getFieldFilter() {
                return field -> field instanceof ListField;
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

