
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;

public class GetCommand extends AbstractTransformChannelCommand<String> {

    public GetCommand() {
        super("get");
    }

    @Override
    public String getUsage() {
        return this.name + " field-name";
    }

    @Override
    public String getHelpSummary() {
        return "gets a field from incoming objects";
    }

    @Override
    public String getHelpDetail() {
        return "The 'get' command takes one argument which is a field name. The field content is retrieved from"
          + " each incoming object and sent to the output channel.";
    }

    @Override
    public String getParameters(Session session, Channels channels, ParseContext ctx) {
        this.checkChannelCount(channels, ctx, 1, Integer.MAX_VALUE);
        this.checkItemType(channels, ctx, ObjId.class);
        return new CommandParser(1, 1, this.getUsage()).parse(ctx).getParams().get(0);
    }

    @Override
    protected <T> Channel<?> transformChannel(Session session, Channel<T> input, String path) {
        throw new UnsupportedOperationException();
    }
}

