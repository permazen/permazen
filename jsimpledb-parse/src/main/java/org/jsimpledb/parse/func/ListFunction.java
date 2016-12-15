
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.EnumSet;

import org.jsimpledb.SessionMode;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.ConstValue;
import org.jsimpledb.parse.expr.Value;

public class ListFunction extends SimpleFunction {

    public ListFunction() {
        super("list", 0, Integer.MAX_VALUE);
    }

    @Override
    public String getHelpSummary() {
        return "Creates a list of items from explicitly provided values";
    }

    @Override
    public String getUsage() {
        return "list(item, ...)";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected Value apply(final ParseSession session, Value[] params) {
        return new ConstValue(Lists.transform(Arrays.asList(params), item -> item.get(session)));
    }
}

