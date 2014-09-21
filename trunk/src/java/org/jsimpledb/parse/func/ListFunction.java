
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import com.google.common.collect.Lists;

import java.util.Arrays;

import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.ConstValue;
import org.jsimpledb.parse.expr.Value;

@Function
public class ListFunction extends SimpleFunction {

    public ListFunction() {
        super("list", 0, Integer.MAX_VALUE);
    }

    @Override
    public String getHelpSummary() {
        return "creates a list of items from explicitly provided values";
    }

    @Override
    public String getUsage() {
        return "list(item, ...)";
    }

    @Override
    protected Value apply(final ParseSession session, Value[] params) {
        return new ConstValue(Lists.transform(Arrays.asList(params), new Value.GetFunction(session)));
    }
}

