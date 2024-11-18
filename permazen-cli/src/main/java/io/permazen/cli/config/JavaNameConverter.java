
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.config;

import com.google.common.base.Preconditions;

import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;

public class JavaNameConverter implements ValueConverter<String> {

    public static final String IDENT_PATTERN = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
    public static final String NAME_PATTERN = IDENT_PATTERN + "(\\." + IDENT_PATTERN + ")*";

    private final String what;

    public JavaNameConverter(String what) {
        Preconditions.checkArgument(what != null, "null what");
        this.what = what;
    }

    @Override
    public String convert(String value) {
        if (!value.matches(NAME_PATTERN))
            throw new ValueConversionException(String.format("invalid Java %s name", this.what));
        return value;
    }

    @Override
    public Class<String> valueType() {
        return String.class;
    }

    @Override
    public String valuePattern() {
        return this.what + "-name";
    }
}
