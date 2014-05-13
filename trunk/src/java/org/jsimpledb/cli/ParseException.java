
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    public ParseException(ParseContext ctx, String message, Throwable cause) {
        super(message, cause);
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
        return this.addCompletions(Arrays.asList(completions));
    }

    public ParseException addCompletions(Iterable<String> completions) {
        for (String completion : completions)
            this.completions.add(completion);
        return this;
    }
}

