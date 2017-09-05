
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse;

import io.permazen.core.ComplexField;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitchAdapter;
import io.permazen.core.ObjType;
import io.permazen.core.SimpleField;
import io.permazen.util.ParseContext;

import java.util.function.Predicate;
import java.util.regex.Matcher;

/**
 * Parses the name of an indexed field.
 *
 * <p>
 * Syntax examples:
 * <ul>
 *  <li><code>Person.lastName</code></li>
 *  <li><code>Person.friends.element</code></li>
 *  <li><code>Person.grades.key</code></li>
 * </ul>
 */
public class IndexedFieldParser implements Parser<IndexedFieldParser.Result> {

    private final SpaceParser spaceParser = new SpaceParser();
    private final Predicate<Field<?>> hasIndexedPredicate;

    @SuppressWarnings("rawtypes")   // https://bugs.openjdk.java.net/browse/JDK-8012685
    public IndexedFieldParser() {
        this.hasIndexedPredicate = field ->
          field instanceof SimpleField ? ((SimpleField<?>)field).isIndexed() :
          field instanceof ComplexField ? ((ComplexField<?>)field).getSubFields().stream().anyMatch(SimpleField::isIndexed) :
          false;
    }

    @Override
    @SuppressWarnings("rawtypes")   // https://bugs.openjdk.java.net/browse/JDK-8012685
    public Result parse(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Get object type
        final ObjType objType = new ObjTypeParser().parse(session, ctx, complete);

        // Get indexed field name
        ctx.skipWhitespace();
        if (!ctx.tryLiteral("."))
            throw new ParseException(ctx, "expected field name").addCompletion(".");
        ctx.skipWhitespace();
        final Matcher fieldMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
        if (fieldMatcher == null) {
            throw new ParseException(ctx, "expected field name").addCompletions(
              objType.getFields().values().stream().filter(this.hasIndexedPredicate).map(Field::getName));
        }
        final String fieldName = fieldMatcher.group();

        // Get indexed field
        final Field<?> field = objType.getFields().values().stream()
          .filter(f -> f.getName().equals(fieldName))
          .filter(this.getFieldFilter())
          .findAny().orElseThrow(() ->
            new ParseException(ctx, "error accessing field `" + fieldName + "': there is no such indexed field in " + objType)
              .addCompletions(ParseUtil.complete(
                objType.getFields().values().stream()
                  .filter(this.getFieldFilter())
                  .filter(this.hasIndexedPredicate)
                  .map(Field::getName),
                fieldName)));

        // Get sub-field if field is a complex field
        return field.visit(new FieldSwitchAdapter<Result>() {

            @Override
            protected <T> Result caseComplexField(ComplexField<T> field) {

                // Get sub-field name
                ctx.skipWhitespace();
                if (!ctx.tryLiteral(".")) {
                    throw new ParseException(ctx, "expected sub-field name").addCompletion(".");
                }
                final Matcher subfieldMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
                if (subfieldMatcher == null) {
                    throw new ParseException(ctx, "expected sub-field name").addCompletions(field.getSubFields().stream()
                      .filter(SimpleField::isIndexed)
                      .map(Field::getName));
                }
                final String subFieldName = subfieldMatcher.group();

                // Get sub-field
                final SimpleField<?> subField = field.getSubFields().stream()
                  .filter(f -> f.getName().equals(subFieldName))
                  .filter(IndexedFieldParser.this.getSubFieldFilter())
                  .findAny().orElseThrow(() ->
                    new ParseException(ctx, "unknown sub-field `" + subFieldName + "' of complex field `" + fieldName + "'")
                    .addCompletions(ParseUtil.complete(
                      field.getSubFields().stream()
                        .filter(IndexedFieldParser.this.getSubFieldFilter())
                        .filter(SimpleField::isIndexed)
                        .map(Field::getName),
                      fieldName)));

                // Done
                return new Result(fieldName + "." + subFieldName, this.verifyIndexedSimple(subField), field);
            }

            @Override
            protected <T> Result caseField(Field<T> field) {
                return new Result(fieldName, this.verifyIndexedSimple(field));
            }

            private SimpleField<?> verifyIndexedSimple(Field<?> field) {
                if (!(field instanceof SimpleField) || !((SimpleField<?>)field).isIndexed())
                    throw new ParseException(ctx, "expected indexed field");
                return (SimpleField<?>)field;
            }
        });
    }

    protected Predicate<? super Field<?>> getFieldFilter() {
        return field -> true;
    }

    protected Predicate<? super SimpleField<?>> getSubFieldFilter() {
        return subField -> true;
    }

// Return type

    /**
     * Result returned from an {@link IndexedFieldParser} parse.
     */
    public static class Result {

        private final String fieldName;
        private final SimpleField<?> field;
        private final ComplexField<?> parentField;

        Result(String fieldName, SimpleField<?> field) {
            this(fieldName, field, null);
        }

        Result(String fieldName, SimpleField<?> field, ComplexField<?> parentField) {
            this.fieldName = fieldName;
            this.field = field;
            this.parentField = parentField;
        }

        /**
         * Get the field's name.
         *
         * @return field name
         */
        public String getFieldName() {
            return this.fieldName;
        }

        /**
         * Get the indexed field.
         *
         * @return indexed field
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
}
