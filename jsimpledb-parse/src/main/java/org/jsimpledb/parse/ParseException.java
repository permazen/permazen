
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.dellroad.stuff.string.StringEncoder;
import org.jsimpledb.util.ParseContext;

@SuppressWarnings("serial")
public class ParseException extends RuntimeException {

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
          + (ctx.isEOF() ? "end of input" : "`" + StringEncoder.encode(ParseContext.truncate(ctx.getInput(), 50), true) + "'"),
          cause);
        this.ctx = ctx;
    }

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
        completions.forEach(this.completions::add);
        return this;
    }
}

