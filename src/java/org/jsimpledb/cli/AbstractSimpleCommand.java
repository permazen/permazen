
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Collections;

import org.jsimpledb.util.ParseContext;

/**
 * Support superclass for commands that just output a single string.
 */
public abstract class AbstractSimpleCommand<P> extends AbstractCommand {

    protected AbstractSimpleCommand(String name) {
        super(name);
    }

    @Override
    public Channels parseParameters(Session session, final Channels input, ParseContext ctx) {
        final P params = this.getParameters(session, input, ctx);
        return new Channels(new AbstractChannel<String>(String.class) {
            @Override
            public Iterable<String> getItems(Session session) {
                final String result = AbstractSimpleCommand.this.getResult(session, input, params);
                return result != null ? Collections.<String>singleton(result) : Collections.<String>emptySet();
            }
        });
    }

    @Override
    public String getUsage() {
        return this.name;
    }

    /**
     * Parse the command line.
     *
     * <p>
     * The implementation in {@link AbstractSimpleCommand} checks that no command line flags or parameters
     * were given and always returns null.
     * </p>
     *
     * @return parsed command line info
     */
    protected P getParameters(Session session, Channels input, ParseContext ctx) {
        new CommandParser(0, 0, this.getUsage()).parse(ctx);
        return null;
    }

    /**
     * Get result.
     *
     * @return result {@link String}, or null for no result
     */
    protected abstract String getResult(Session session, Channels channels, P params);
}

