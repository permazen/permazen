
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.ConstValue;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Value;

public class CountFunction extends SimpleFunction {

    public CountFunction() {
        super("count", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "Counts the number of elements in the provided iteration or collection";
    }

    @Override
    public String getUsage() {
        return "count(items)";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected Value apply(ParseSession session, Value[] params) {
        Object obj = params[0].checkNotNull(session, "count()");
        if (obj instanceof Map)
            obj = ((Map<?, ?>)obj).entrySet();
        if (obj instanceof Collection)
            return new ConstValue(((Collection<?>)obj).size());
        if (obj instanceof Iterable)
            obj = ((Iterable<?>)obj).iterator();
        if (obj instanceof Iterator) {
            int count = 0;
            for (Iterator<?> i = (Iterator<?>)obj; i.hasNext(); )
                count++;
            return new ConstValue(count);
        }
        throw new EvalException("count() cannot be applied to object of type " + obj.getClass().getName());
    }
}
