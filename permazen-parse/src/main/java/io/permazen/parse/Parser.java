
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse;

import com.google.common.base.Preconditions;

import io.permazen.parse.expr.Node;
import io.permazen.util.ParseContext;

import java.util.function.Function;
import java.util.regex.Matcher;

/**
 * Generic parsing interface.
 *
 * @param <T> parsed value type
 */
@FunctionalInterface
public interface Parser<T> {

    /**
     * Parse text from the given parse context.
     *
     * <p>
     * Generally speaking, this method may assume that any whitespace allowed before the item
     * being parsed has already been skipped over (that's a matter for the containing parser).
     *
     * @param session parse session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @return parsed value
     * @throws ParseException if parse fails, or if {@code complete} is true and there are valid completions
     */
    T parse(ParseSession session, ParseContext ctx, boolean complete);

    /**
     * Apply a new identifier scope, defined by the {@code scope} function, to the given {@link Parser}.
     *
     * <p>
     * The returned {@link Parser} will parse the same as {@code parser}, except that any
     * {@linkplain ParseUtil#IDENT_PATTERN identifiers} recognized by {@code scope} will be substituted accordingly.
     *
     * @param parser original parser
     * @param scope new identifier scope
     * @return new {@link Parser} with identifier scope defined
     * @throws IllegalArgumentException if either parameter is null
     */
    static <T> Parser<T> applyIdentifierScope(Parser<T> parser, Function<String, Node> scope) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkArgument(scope != null, "null scope");
        return (session, ctx, complete) -> {

            // Configure new identifier that checks scope, and then defers to previous identifier parser
            final Parser<? extends Node> identifierParser = session.getIdentifierParser();
            session.setIdentifierParser((session2, ctx2, complete2) -> {
                final int startIndex = ctx2.getIndex();
                try {
                    final Matcher identMatcher = ctx2.tryPattern(ParseUtil.IDENT_PATTERN);
                    if (identMatcher == null)
                        throw new ParseException(ctx2);
                    final String name = identMatcher.group();
                    final Node node = scope.apply(name);
                    if (node == null)
                        throw new ParseException(ctx2, "unknown variable `" + name + "'");
                    return node;
                } catch (ParseException e) {
                    if (identifierParser == null)
                        throw e;
                    ctx2.setIndex(startIndex);
                    return identifierParser.parse(session2, ctx2, complete2);
                }
            });

            // Proceed with parse, then restore original scope
            try {
                return parser.parse(session, ctx, complete);
            } finally {
                session.setIdentifierParser(identifierParser);
            }
        };
    }
}
