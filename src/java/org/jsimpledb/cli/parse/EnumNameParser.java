
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.regex.Matcher;

import org.dellroad.stuff.java.EnumUtil;
import org.jsimpledb.cli.Session;
import org.jsimpledb.util.ParseContext;

public class EnumNameParser<T extends Enum<T>> implements Parser<T> {

    private final Class<T> type;
    private final boolean lowerCase;
    private final Function<T, String> nameFunction = new Function<T, String>() {
        @Override
        public String apply(T value) {
            String name = value.name();
            if (EnumNameParser.this.lowerCase)
                name = name.toLowerCase();
            return name;
        }
    };

    public EnumNameParser(Class<T> type) {
        this(type, true);
    }

    public EnumNameParser(Class<T> type, boolean lowerCase) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.type = type;
        this.lowerCase = lowerCase;
    }

    @Override
    public T parse(Session session, ParseContext ctx, boolean complete) {

        // Get name
        final Matcher matcher = ctx.tryPattern("[^\\s;]*");
        if (matcher == null && ctx.isEOF())
            throw new ParseException(ctx).addCompletions(Lists.transform(EnumUtil.getValues(this.type), this.nameFunction));
        final String name = matcher.group();

        // Match name
        for (T value : EnumUtil.getValues(this.type)) {
            final String valueName = this.nameFunction.apply(value);
            if (name.equals(valueName))
                return value;
        }

        // Not found
        throw new ParseException(ctx, "unknown value `" + name + "'").addCompletions(
            ParseUtil.complete(Lists.transform(EnumUtil.getValues(this.type), this.nameFunction), name));
    }
}

