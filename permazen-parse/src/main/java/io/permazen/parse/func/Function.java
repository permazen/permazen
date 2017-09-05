
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import io.permazen.SessionMode;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.Value;
import io.permazen.util.ParseContext;

import java.util.EnumSet;

/**
 * Function hook in a {@link io.permazen.parse.ParseSession}.
 *
 * @see AbstractFunction
 * @see SimpleFunction
 */
public interface Function {

    /**
     * Get the name of this function.
     *
     * @return function name
     */
    String getName();

    /**
     * Get function usage string. For example: {@code pow(base, exponent)}.
     *
     * @return function usage string
     */
    String getUsage();

    /**
     * Get summarized help (typically a single line).
     *
     * @return one line summarized description of this function
     */
    String getHelpSummary();

    /**
     * Get expanded help (typically multiple lines). May refer to the {@linkplain #getUsage usage string}.
     *
     * <p>
     * The implementation in {@link AbstractFunction} delegates to {@link #getHelpSummary getHelpSummary()}.
     *
     * @return detailed description of this function
     */
    String getHelpDetail();

    /**
     * Get the {@link SessionMode}(s) supported by this function.
     *
     * @return set of supported {@link SessionMode}s
     */
    EnumSet<SessionMode> getSessionModes();

    /**
     * Parse function parameters.
     *
     * <p>
     * The {@code ctx} will be pointing at the first parameter (if any) or closing parenthesis. This method should parse
     * (but not evaluate) function parameters up through the closing parenthesis. The return value is an opaque value
     * representing the parsed parameters and subsequently passed to {@link #apply apply()}.
     *
     * @param session parse session
     * @param ctx parse context
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @return parsed parameters object to be passed to {@link #apply apply()}
     * @throws io.permazen.parse.ParseException if parse fails, or if {@code complete} is true and there are valid completions
     */
    Object parseParams(ParseSession session, ParseContext ctx, boolean complete);

    /**
     * Evaluate this function. There will be a transaction open.
     *
     * @param session parse session
     * @param params parsed parameters returned by {@link #parseParams parseParams()}
     * @return value returned by this function
     * @throws RuntimeException if there is an error
     */
    Value apply(ParseSession session, Object params);
}
