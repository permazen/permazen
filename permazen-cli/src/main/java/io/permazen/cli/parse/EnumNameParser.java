
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

import java.util.function.Function;
import java.util.regex.Matcher;

import org.dellroad.stuff.java.EnumUtil;

public class EnumNameParser<T extends Enum<T>> implements Parser<T> {

    private final Class<T> type;
    private final Function<T, String> nameFunction;

    public EnumNameParser(Class<T> type) {
        this(type, true);
    }

    public EnumNameParser(Class<T> type, boolean lowerCase) {
        Preconditions.checkArgument(type != null, "null type");
        this.type = type;
        this.nameFunction = value -> {
            String name = value.name();
            if (lowerCase)
                name = name.toLowerCase();
            return name;
        };
    }

    @Override
    public T parse(Session session, ParseContext ctx, boolean complete) {

        // Get enum value name
        final Matcher matcher = ctx.tryPattern("[^\\s;]*");
        if (matcher == null) {
            final ParseException e = new ParseException(ctx);
            if (ctx.isEOF() && complete)
                e.addCompletions(EnumUtil.getValues(this.type).stream().map(this.nameFunction));
            throw e;
        }
        final String valueName = matcher.group();

        // Find corresponding enum value
        return EnumUtil.getValues(this.type).stream()
          .filter(value -> valueName.equals(this.nameFunction.apply(value)))
          .findAny().orElseThrow(() -> new ParseException(ctx, "unknown value `" + valueName + "'")
            .addCompletions(ParseUtil.complete(EnumUtil.getValues(this.type).stream().map(this.nameFunction), valueName)));
    }
}

