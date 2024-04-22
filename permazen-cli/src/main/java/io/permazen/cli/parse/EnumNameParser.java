
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;

import java.util.function.Function;

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
    public T parse(Session session, String text) {

        // Sanity check
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(text != null, "null text");

        // Find corresponding enum value
        return EnumUtil.getValues(this.type).stream()
          .filter(value -> text.equals(this.nameFunction.apply(value)))
          .findAny().orElseThrow(() -> new IllegalArgumentException("unknown value \"" + text + "\""));
    }
}
