
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.reflect.TypeToken;

import org.jsimpledb.util.ParseContext;

public abstract class AbstractCommand {

    protected final String name;

    protected AbstractCommand(String name) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public abstract String getUsage();

    public abstract String getHelpSummary();

    public String getHelpDetail() {
        return this.getHelpSummary();
    }

    public abstract Channels parseParameters(Session session, Channels input, ParseContext ctx);

    protected void checkItemType(Channels channels, ParseContext ctx, Class<?> type) {
        for (Channel<?> channel : channels)
            this.checkItemType(channel, ctx, type);
    }

    protected void checkItemType(Channel<?> channel, ParseContext ctx, Class<?> type) {
        this.checkItemType(channel, ctx, TypeToken.of(type));
    }

    protected void checkItemType(Channel<?> channel, ParseContext ctx, TypeToken<?> typeToken) {
        final TypeToken<?> itemType = channel.getItemType().getTypeToken();
        if (!typeToken.isAssignableFrom(itemType)) {
            throw new ParseException(ctx, "the `" + this.name
              + "' command expects to input items of type " + typeToken + " but the channel contains " + itemType);
        }
    }

    protected void checkChannelCount(Channels channels, ParseContext ctx, int expected) {
        this.checkChannelCount(channels, ctx, expected, expected);
    }

    protected void checkChannelCount(Channels channels, ParseContext ctx, int min, int max) {
        if (channels.size() >= min && channels.size() <= max)
            return;
        final String expects;
        if (min == max)
            expects = "exactly " + min + " input channel" + (min != 1 ? "s" : "");
        else if (min != 0 && max != Integer.MAX_VALUE)
            expects = "between " + min + " and " + max + " input channels";
        else if (min != 0)
            expects = "at least " + min + " input channel" + (min != 1 ? "s" : "");
        else
            expects = "at most " + max + " input channel" + (max != 1 ? "s" : "");
        throw new ParseException(ctx, "the `" + this.name + "' command expects " + expects
          + ", but " + channels.size() + " " + (channels.size() != 1 ? "were" : "was") + " given");
    }
}

