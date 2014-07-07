
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;

@CliFunction
public class CountFunction extends SimpleFunction {

    public CountFunction() {
        super("count", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "counts the number of elements in the provided iteration or collection";
    }

    @Override
    public String getUsage() {
        return "count(items)";
    }

    @Override
    protected Value apply(Session session, Value[] params) {
        Object obj = params[0].checkNotNull(session, "count()");
        if (obj instanceof Map)
            obj = ((Map<?, ?>)obj).entrySet();
        if (obj instanceof Collection)
            return new Value(((Collection<?>)obj).size());
        if (obj instanceof Iterable)
            obj = ((Iterable<?>)obj).iterator();
        if (obj instanceof Iterator) {
            int count = 0;
            for (Iterator<?> i = (Iterator<?>)obj; i.hasNext(); )
                count++;
            return new Value(count);
        }
        throw new EvalException("count() cannot be applied to object of type " + obj.getClass().getName());
    }
}

