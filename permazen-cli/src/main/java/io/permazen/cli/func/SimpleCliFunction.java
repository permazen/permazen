
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.func;

import io.permazen.cli.CliSession;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.Value;
import io.permazen.parse.func.SimpleFunction;

/**
 * CLI extension of {@link SimpleFunction}.
 */
public abstract class SimpleCliFunction extends SimpleFunction {

// Constructors

    /**
     * Constructor.
     *
     * @param name function name
     * @param minArgs minimum number of arguments (inclusive)
     * @param maxArgs maximum number of arguments (inclusive)
     */
    protected SimpleCliFunction(String name, int minArgs, int maxArgs) {
        super(name, minArgs, maxArgs);
    }

    @Override
    protected final Value apply(ParseSession session, Value[] params) {
        return this.apply((CliSession)session, params);
    }

    /**
     * Apply this function to the given values.
     *
     * @param session CLI session
     * @param params parsed parameters; will already be checked between {@link #getMinArgs} and {@link #getMaxArgs}
     * @return value returned by this function
     * @throws RuntimeException if there is an error
     */
    protected abstract Value apply(CliSession session, Value[] params);
}

