
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import com.google.common.collect.Lists;

import java.util.Arrays;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.Value;

@CliFunction
public class ListFunction extends SimpleFunction {

    public ListFunction() {
        super("list", 0, Integer.MAX_VALUE);
    }

    @Override
    public String getHelpSummary() {
        return "creates a list of items";
    }

    @Override
    public String getUsage() {
        return "list(item, ...)";
    }

    @Override
    protected Value apply(final Session session, Value[] params) {
        return new Value(Lists.transform(Arrays.asList(params), new com.google.common.base.Function<Value, Object>() {
            @Override
            public Object apply(Value item) {
                return item.get(session);
            }
        }));
    }
}

