
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Exception for parse errors associated with a {@link ParseContext}.
 */
@SuppressWarnings("serial")
public class ParseException extends IllegalArgumentException {

    private final ArrayList<String> completions = new ArrayList<>();
    private final ParseContext ctx;

    public ParseException(ParseContext ctx) {
        this(ctx, null, null);
    }

    public ParseException(ParseContext ctx, String message) {
        this(ctx, message, null);
    }

    public ParseException(ParseContext ctx, Throwable cause) {
        this(ctx, null, cause);
    }

    public ParseException(ParseContext ctx, String message, Throwable cause) {
        super((message != null ? message : "parse error") + " at "
          + (ctx.isEOF() ? "end of input" : "\"" + ParseContext.encode(ParseContext.truncate(ctx.getInput(), 50)) + "\""),
          cause);
        this.ctx = ctx;
    }

    /**
     * Get the associated {@link ParseContext}.
     *
     * @return context for parse error
     */
    public ParseContext getParseContext() {
        return this.ctx;
    }

    public List<String> getCompletions() {
        return this.completions;
    }

    public ParseException addCompletion(String completion) {
        return this.addCompletions(Arrays.asList(completion));
    }

    public ParseException addCompletions(String... completions) {
        return this.addCompletions(Stream.of(completions));
    }

    public ParseException addCompletions(Iterable<String> completions) {
        for (String completion : completions)
            this.completions.add(completion);
        return this;
    }

    public ParseException addCompletions(Stream<String> completions) {
        Streams.iterate(completions, this.completions::add);
        return this;
    }
}
