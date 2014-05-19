
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.jsimpledb.core.Field;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.util.ParseContext;

/**
 * Parses a field.
 *
 * <p>
 * A field is a type name plus dot and field name, e.g., {@code Person.lastName}.
 * </p>
 */
public class FieldParser implements Parser<Field<?>> {

    @Override
    public Field<?> parse(Session session, ParseContext ctx, boolean complete) {

        // Start with type
        final ObjType objType = new ObjTypeParser().parse(session, ctx, complete);
        if (!ctx.tryLiteral(".")) {
            throw new ParseException(ctx, "invalid field starting with `" + Util.truncate(ctx.getInput(), 16) + "'")
              .addCompletion(".");
        }

        // Find the field
        final String fieldName = ctx.matchPrefix("[^\\s.;]*").group();
        final Field<?> field = Iterables.find(objType.getFields().values(), new Predicate<Field<?>>() {
            @Override
            public boolean apply(Field<?> field) {
                return fieldName.equals(field.getName()) || fieldName.equals("" + field.getStorageId());
            }
          }, null);
        if (field == null) {
            throw new ParseException(ctx, "no such field `" + fieldName + "' in " + objType).addCompletions(
              Util.complete(Iterables.transform(objType.getFields().values(), new Function<Field<?>, String>() {
                  @Override
                  public String apply(Field<?> field) {
                      return field.getName();
                  }
              }), fieldName));
        }

        // Done
        return field;
    }
}

