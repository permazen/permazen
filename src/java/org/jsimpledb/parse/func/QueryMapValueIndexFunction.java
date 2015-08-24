
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import com.google.common.collect.Iterables;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.parse.IndexedFieldParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.expr.AbstractValue;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.util.InstancePredicate;

@Function
public class QueryMapValueIndexFunction extends AbstractQueryFunction {

    public QueryMapValueIndexFunction() {
        super("queryMapValueIndex", 2, 2);
    }

    @Override
    public String getHelpSummary() {
        return "Queries the composite index on a map value field that includes the map keys";
    }

    @Override
    public String getUsage() {
        return this.getName() + "(object-type, field-name, value-type, key-type) (JSimpleDB mode only)\n"
          + "       " + this.getName() + "(type-name.field-name)\n"
          + "       " + this.getName() + "(storage-id)";
    }

    @Override
    public String getHelpDetail() {
        return "Queries the composite index associated with an indexed map value field. The object-type is the type of object"
          + " to be queried, i.e., the object type that contains the map field, or any super-type or sub-type; a strict"
          + " sub-type will cause the returned index to be restricted to that sub-type. The field-name"
          + " is the name of the map value field; it must include the `value' sub-field name, e.g., `mymap.value'."
          + " The key-type and value-type are the map key and value types; in the case of reference fields,"
          + " a super-type or more restrictive sub-type may also be specified, otherwise the type must exactly match."
          + "\n\nThe first form is only valid in JSimpleDB mode; the second and third forms may be used in either JSimpleDB"
          + " mode or Core API mode.";
    }

    @Override
    protected int parseName(ParseSession session, ParseContext ctx, boolean complete) {
        return new IndexedFieldParser() {
            @Override
            protected Iterable<? extends Field<?>> filterFields(Iterable<? extends Field<?>> fields) {
                return Iterables.filter(fields, new InstancePredicate(MapField.class));
            }
            @Override
            protected Iterable<? extends SimpleField<?>> filterSubFields(Iterable<? extends SimpleField<?>> subFields) {
                return Iterables.filter(subFields, new ParseUtil.HasNamePredicate(MapField.VALUE_FIELD_NAME));
            }
        }.parse(session, ctx, complete).getField().getStorageId();
    }

    @Override
    protected Value apply(ParseSession session, final Class<?> objectType, final String fieldName, final Class<?>[] valueTypes) {
        return new AbstractValue() {
            @Override
            public Object get(ParseSession session) {
                return JTransaction.getCurrent().queryMapValueIndex(objectType, fieldName, valueTypes[0], valueTypes[1]);
            }
        };
    }
}

