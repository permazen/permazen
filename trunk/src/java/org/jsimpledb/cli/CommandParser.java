
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;

import org.jsimpledb.util.ParseContext;

/**
 * Parses a command with optional flags.
 */
public class CommandParser {

    private final int minParams;
    private final int maxParams;
    private final String name;
    private final String usage;
    private final List<String> options = new ArrayList<>();
    private final HashSet<String> takesArgument = new HashSet<>();

    private final HashMap<String, String> flags = new HashMap<>();
    private final List<String> params = new ArrayList<>();

    public CommandParser(int minParams, int maxParams, String usage, String... options) {
        this.minParams = minParams;
        this.maxParams = maxParams;
        this.usage = usage;
        final int space = usage.indexOf(' ');
        this.name = space != -1 ? usage.substring(0, space) : usage;
        for (String option : options) {
            if (option.endsWith("@")) {
                option = option.substring(0, option.length() - 1);
                this.takesArgument.add(option);
            }
            this.options.add(option);
        }
    }

    public CommandParser parse(ParseContext ctx) {

        // Parse option flags
        this.parseOptions(ctx);

        // Parse parameters
        this.parseParams(ctx);
        if (this.params.size() < this.minParams || this.params.size() > this.maxParams)
            throw new ParseException(ctx, "Usage: " + this.usage);

        // Done
        return this;
    }

    protected void parseOptions(ParseContext ctx) {
        this.flags.clear();
        while (true) {
            final Matcher matcher = ctx.tryPattern("(\\s*)(-[^\\s,|@]+)");
            if (matcher == null)
                break;
            final String whitespace = matcher.group(1);
            final String flag = matcher.group(2);
            if (flag.equals("--"))
                break;
            if (!this.options.contains(flag)) {
                throw new ParseException(ctx, "unknown flag `" + flag + "' given to the `" + this.name + "' command")
                  .addCompletions(Util.complete(this.options, flag, whitespace != null));
            }
            String arg = "";
            if (this.takesArgument.contains(flag)) {
                final Matcher argMatcher = ctx.tryPattern("\\s*([^\\s,|)]+)");
                if (argMatcher == null)
                    throw new ParseException(ctx, "flag `" + flag + "' for command `"
                      + this.name + "' requires an argument but none was given");
                arg = argMatcher.group(1);
            }
            this.flags.put(flag, arg);
        }
    }

    protected void parseParams(ParseContext ctx) {
        this.params.clear();
        while (true) {
            final Matcher matcher = ctx.tryPattern("\\s*([^\\s,|)]+)");
            if (matcher == null)
                break;
            this.params.add(matcher.group(1));
        }
    }

    public boolean hasFlag(String flag) {
        return this.flags.containsKey(flag);
    }

    public String getFlag(String flag) {
        return this.flags.get(flag);
    }

    public List<String> getParams() {
        return this.params;
    }

    public String getParam(int index) {
        return this.params.get(index);
    }
}

