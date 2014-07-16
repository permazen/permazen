
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.NoSuchElementException;
import java.util.regex.Matcher;

import org.jsimpledb.cli.Session;
import org.jsimpledb.core.ComplexField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldSwitchAdapter;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.util.ParseContext;

/**
 * Parses an indexed field.
 *
 * <p>
 * Syntax examples:
 * <ul>
 *  <li><code>Person.lastName</code></li>
 *  <li><code>Person.friends.element</code></li>
 *  <li><code>Person.grades.key</code></li>
 * </p>
 */
public class IndexedFieldParser implements Parser<IndexedFieldParser.Result> {

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Result parse(final Session session, final ParseContext ctx, final boolean complete) {

        // Get object type
        final int typeStart = ctx.getIndex();
        final ObjType objType = new ObjTypeParser().parse(session, ctx, complete);
        final String typeName = ctx.getOriginalInput().substring(typeStart, ctx.getIndex()).trim();

        // Get indexed field
        ctx.skipWhitespace();
        if (!ctx.tryLiteral("."))
            throw new ParseException(ctx, "expected field name").addCompletion(".");
        ctx.skipWhitespace();
        final Matcher fieldMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
        if (fieldMatcher == null) {
            throw new ParseException(ctx, "expected field name").addCompletions(Iterables.transform(Iterables.filter(
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
        return field.visit(new FieldSwitchAdapter<Result>() {

            @Override
            protected <T> Result caseComplexField(ComplexField<T> field) {
                ctx.skipWhitespace();
                if (!ctx.tryLiteral(".")) {
                    throw new ParseException(ctx, "expected sub-field name").addCompletion(".");
                }
                final Matcher subfieldMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
                if (subfieldMatcher == null) {
                    throw new ParseException(ctx, "expected sub-field name").addCompletions(Iterables.transform(Iterables.filter(
                       field.getSubFields(), new IsIndexedPredicate()), new FieldNameFunction()));
                }
                final String subName = subfieldMatcher.group();
                final SimpleField<?> subField;
                try {
                    subField = Iterables.find(field.getSubFields(), new HasNamePredicate(subName));
                } catch (NoSuchElementException e) {
                    throw new ParseException(ctx, "unknown sub-field `" + subName + "' of complex field `" + name + "'")
                      .addCompletions(ParseUtil.complete(Iterables.transform(Iterables.filter(
                        field.getSubFields(), new IsIndexedPredicate()), new FieldNameFunction()), name));
                }
                return new Result(this.verifyIndexedSimple(subField), field);
            }

            @Override
            protected <T> Result caseField(Field<T> field) {
                return new Result(this.verifyIndexedSimple(field));
            }

            private SimpleField<?> verifyIndexedSimple(Field<?> field) {
                if (!(field instanceof SimpleField) || !((SimpleField<?>)field).isIndexed())
                    throw new ParseException(ctx, "expected indexed field");
                return (SimpleField<?>)field;
            }
        });
    }

// Return type

    /**
     * Result returned from an {@link IndexedFieldParser} parse.
     */
    public static class Result {

        private final SimpleField<?> field;
        private final ComplexField<?> parentField;

        Result(SimpleField<?> field) {
            this(field, null);
        }

        Result(SimpleField<?> field, ComplexField<?> parentField) {
            this.field = field;
            this.parentField = parentField;
        }

        /**
         * Get the indexed field.
         */
        public SimpleField<?> getField() {
            return this.field;
        }

        /**
         * Get the indexed field's parent field, if any.
         *
         * @return indexed field's parent field, or null if the indexed field is not a sub-field
         */
        public ComplexField<?> getParentField() {
            return this.parentField;
        }
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

