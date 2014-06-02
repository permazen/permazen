
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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Superclass of all CLI commands.
 */
public abstract class Command implements Parser<Action> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String name;
    protected final ParamParser paramParser;

// Constructors

    protected Command(String spec) {
        if (spec == null)
            throw new IllegalArgumentException("null spec");
        final Matcher matcher = Pattern.compile("([^\\s]+)(\\s+(.*))?").matcher(spec);
        if (!matcher.matches())
            throw new IllegalArgumentException("invalid command specification `" + spec + "'");
        this.name = matcher.group(1);
        final String paramSpec = matcher.group(3);
        this.paramParser = new ParamParser(paramSpec != null ? paramSpec : "") {
            @Override
            protected Parser<?> getParser(String typeName) {
                try {
                    return super.getParser(typeName);
                } catch (IllegalArgumentException e) {
                    return Command.this.getParser(typeName);
                }
            }
        };
    }

// Command stuff

    public String getName() {
        return this.name;
    }

    public String getUsage() {
        return this.paramParser.getUsage(this.name);
    }

    public abstract String getHelpSummary();

    public String getHelpDetail() {
        return this.getHelpSummary();
    }

    /**
     * Parse command line and return command action.
     *
     * <p>
     * The implementation in {@link ParamParser} parses the parameters and delegates to {@link #getAction} with the result.
     * </p>
     *
     * @throws ParseException if parameters are invalid
     */
    @Override
    public Action parse(Session session, ParseContext ctx, boolean complete) {
        return this.getAction(session, ctx, complete, this.paramParser.parse(session, ctx, complete));
    }

    /**
     * Process command line parameters and return action.
     *
     * @throws ParseException if parameters are invalid
     */
    public abstract Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params);

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
            throw new IllegalArgumentException("the `" + this.getName() + "' command expects a channel of type "
              + typeToken + " but the channel found has type " + itemType);
        }
        return (Channel<? extends T>)channel;
    }

    protected void checkDepth(Session session, int min) {
        final int depth = session.getStack().size();
        if (depth < min) {
            throw new IllegalArgumentException("the `" + this.getName() + "' command expects at least " + min
              + " input channel" + (min != 1 ? "s" : "") + ", but there are only "
              + depth + " channel" + (depth != 1 ? "s" : "") + " on the stack");
        }
    }

    /**
     * Convert parameter spec type name into a {@link Parser}. Used for custom type names not supported by {@link ParamParser}.
     *
     * <p>
     * The implementation in {@link ParamParser} supports all {@link org.jsimpledb.core.FieldType}s registered with the database,
     * {@code type} for an object type name (returns {@link Integer}), and {@code objid} for an object ID
     * (returns {@link org.jsimpledb.core.ObjId}).
     * </p>
     */
    protected Parser<?> getParser(String typeName) {
        if (typeName == null)
            throw new IllegalArgumentException("null typeName");
        if (typeName.equals("type"))
            return new ObjTypeParser();
        if (typeName.equals("objid"))
            return new ObjIdParser();
        if (typeName.equals("field"))
            return new FieldParser();
        return FieldTypeParser.getFieldTypeParser(typeName);
    }
}

