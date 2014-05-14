
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.reflect.TypeToken;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.jsimpledb.util.ParseContext;

public abstract class AbstractCommand {

    protected final String name;

// Constructors

    protected AbstractCommand(String name) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.name = name;
    }

// Command stuff

    public String getName() {
        return this.name;
    }

    public String getUsage() {
        return this.getName();
    }

    public abstract String getHelpSummary();

    public String getHelpDetail() {
        return this.getHelpSummary();
    }

    /**
     * Parse command parameters and return command action.
     *
     * @throws ParseException if command is invalid
     */
    public abstract Action parseParameters(Session session, ParseContext ctx);

// Channel access

    public Channel<?> get(Session session, int depth) {
        this.checkDepth(session, depth + 1);
        return Iterables.get(session.getStack(), depth);
    }

    public <T> Channel<? extends T> get(Session session, int depth, Class<T> type) {
        return this.get(session, depth, TypeToken.of(type));
    }

    public <T> Channel<? extends T> get(Session session, int depth, TypeToken<T> typeToken) {
        return this.cast(this.get(session, depth), typeToken);
    }

    public Channel<?> pop(Session session) {
        this.checkDepth(session, 1);
        return session.getStack().pop();
    }

    public <T> Channel<? extends T> pop(Session session, Class<T> type) {
        return this.pop(session, TypeToken.of(type));
    }

    public <T> Channel<? extends T> pop(Session session, TypeToken<T> typeToken) {
        return this.cast(this.pop(session), typeToken);
    }

    public Channel<?> remove(Session session, int depth) {
        this.checkDepth(session, depth + 1);
        final Iterator<Channel<?>> i = session.getStack().iterator();
        Iterators.advance(i, depth);
        Channel<?> channel = i.next();
        i.remove();
        return channel;
    }

    public <T> Channel<? extends T> remove(Session session, int depth, Class<T> type) {
        return this.remove(session, depth, TypeToken.of(type));
    }

    public <T> Channel<? extends T> remove(Session session, int depth, TypeToken<T> typeToken) {
        return this.cast(this.remove(session, depth), typeToken);
    }

    public <T> Channel<? extends T> cast(Channel<?> channel, Class<T> type) {
        return this.cast(channel, TypeToken.of(type));
    }

    public void push(Session session, Channel<?> channel) {
        session.getStack().push(channel);
    }

    public <T> void push(Session session, Class<T> type, final T value) {
        this.push(session, new AbstractChannel<T>(type) {
            @Override
            public Set<T> getItems(Session session) {
                return Collections.<T>singleton(value);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public <T> Channel<? extends T> cast(Channel<?> channel, TypeToken<T> typeToken) {
        final TypeToken<?> itemType = channel.getItemType().getTypeToken();
        if (!typeToken.isAssignableFrom(itemType)) {
            throw new IllegalArgumentException("the `" + this.name + "' command expects a channel of type "
              + typeToken + " but the channel found has type " + itemType);
        }
        return (Channel<? extends T>)channel;
    }

    protected void checkDepth(Session session, int min) {
        final int depth = session.getStack().size();
        if (depth < min) {
            throw new IllegalArgumentException("the `" + this.name + "' command expects at least " + min
              + " input channel" + (min != 1 ? "s" : "") + ", but there are only "
              + depth + " channel" + (depth != 1 ? "s" : "") + " on the stack");
        }
    }
}

