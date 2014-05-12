
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;

import org.jsimpledb.util.ParseContext;

/**
 * CLI command.
 */
public abstract class Command {

    public static final Function<Command, String> NAME_FUNCTION = new Function<Command, String>() {
        @Override
        public String apply(Command command) {
            return command.getName();
        }
    };

    private final String prefix;
    private final String name;

    protected Command(String name) {
        this((String)null, name);
    }

    protected Command(AggregateCommand parent, String name) {
        this(parent instanceof RootCommand ? null : parent.getFullName(), name);
    }

    protected Command(String prefix, String name) {
        this.prefix = prefix;
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.name = name;
    }

    public String getPrefix() {
        return this.prefix;
    }

    public String getName() {
        return this.name;
    }

    public String getFullName() {
        return this.getFullName(this.name);
    }

    public String getFullName(String commandName) {
        return (this.prefix != null ? this.prefix + " " : "") + commandName;
    }

    /**
     * Parse the command.
     *
     * @throws ParseException if parse fails, preferably with completions
     */
    public abstract Action parse(Session session, ParseContext ctx) throws ParseException;

    /**
     * Get a one-line help summary.
     */
    public abstract String getHelpSummary();

    /**
     * Get help information.
     *
     * <p>
     * The implementation in {@link Command} just delegates to {@link #getHelpSummary}.
     * </p>
     */
    public String getHelpDetail() {
        return this.getHelpSummary();
    }
}

