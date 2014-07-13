
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.NoSuchElementException;
import java.util.regex.Matcher;

import org.jsimpledb.JTransaction;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ObjTypeParser;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.ParseUtil;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.core.ComplexField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldSwitchAdapter;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.util.ParseContext;

@CliFunction
public class QueryFunction extends Function {

    private final SpaceParser spaceParser = new SpaceParser();

    public QueryFunction() {
        super("query");
    }

    @Override
    public String getHelpSummary() {
        return "queries an indexed field";
    }

    @Override
    public String getUsage() {
        return "query(type.field)";
    }

    @Override
    public String getHelpDetail() {
        return "Queries a field index and returns a mapping from field value to set of objects containing that value in the field."
          + " For collection fields, specify the sub-field, e.g., `Person.friends.element' or `Person.grades.key'.";
    }

    @Override
    public Integer parseParams(final Session session, final ParseContext ctx, final boolean complete) {

        // Get object type
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "indexed field parameter required");
        final int typeStart = ctx.getIndex();
        final ObjType objType = new ObjTypeParser().parse(session, ctx, complete);
        final String typeName = ctx.getOriginalInput().substring(typeStart, ctx.getIndex()).trim();

        // Get field
        ctx.skipWhitespace();
        if (!ctx.tryLiteral("."))
            throw new ParseException(ctx, "expected field name after type name `" + typeName + "'").addCompletion(".");
        ctx.skipWhitespace();
        final Matcher fieldMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
        if (fieldMatcher == null) {
            throw new ParseException(ctx, "expected field name after type name `" + typeName + "'").addCompletions(
              Iterables.transform(Iterables.filter(
               objType.getFields().values(), new HasIndexedPredicate()), new FieldNameFunction()));
        }
        final String name = fieldMatcher.group();
        final Field<?> field;
        try {
            field = ParseUtil.resolveField(session, objType, name);
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, e.getMessage()).addCompletions(ParseUtil.complete(Iterables.transform(Iterables.filter(
              objType.getFields().values(), new HasIndexedPredicate()), new FieldNameFunction()), name));
        }

        // Get sub-field if field is a complex field
        final int storageId = field.visit(new FieldSwitchAdapter<Integer>() {

            @Override
            protected <T> Integer caseComplexField(ComplexField<T> field) {
                ctx.skipWhitespace();
                if (!ctx.tryLiteral(".")) {
                    throw new ParseException(ctx, "expected sub-field name after complex field name `"
                      + name + "'").addCompletion(".");
                }
                final Matcher subfieldMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
                if (subfieldMatcher == null) {
                    throw new ParseException(ctx, "expected sub-field name after complex field name `" + name + "'")
                      .addCompletions(Iterables.transform(Iterables.filter(
                       field.getSubFields(), new IsIndexedPredicate()), new FieldNameFunction()));
                }
                final String subName = subfieldMatcher.group();
                final SimpleField<?> subField;
                try {
                    return Iterables.find(field.getSubFields(), new HasNamePredicate(subName)).getStorageId();
                } catch (NoSuchElementException e) {
                    throw new ParseException(ctx, "unknown sub-field `" + subName + "' of complex field `" + name + "'")
                      .addCompletions(ParseUtil.complete(Iterables.transform(Iterables.filter(
                        field.getSubFields(), new IsIndexedPredicate()), new FieldNameFunction()), name));
                }
            }

            @Override
            protected <T> Integer caseField(Field<T> field) {
                return field.getStorageId();
            }
        });

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return storageId;
    }

    @Override
    public Value apply(Session session, Object params) {
        final int storageId = (Integer)params;
        return new Value(session.hasJSimpleDB() ?
          JTransaction.getCurrent().querySimpleField(storageId) : session.getTransaction().querySimpleField(storageId));
    }

// Functions and Predicates

    private static class FieldNameFunction implements com.google.common.base.Function<Field<?>, String> {
        @Override
        public String apply(Field<?> field) {
            return field.getName();
        }
    }

    private static class HasNamePredicate implements Predicate<Field<?>> {

        private final String name;

        HasNamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Field<?> field) {
            return field.getName().equals(this.name);
        }
    }

    private static class IsIndexedPredicate implements Predicate<SimpleField<?>> {
        @Override
        public boolean apply(SimpleField<?> field) {
            return field.isIndexed();
        }
    }

    private static class HasIndexedPredicate implements Predicate<Field<?>> {
        @Override
        public boolean apply(Field<?> field) {
            return field instanceof SimpleField ? ((SimpleField<?>)field).isIndexed() :
              field instanceof ComplexField ? Iterables.any(((ComplexField<?>)field).getSubFields(), new IsIndexedPredicate()) :
              false;
        }
    }
}

