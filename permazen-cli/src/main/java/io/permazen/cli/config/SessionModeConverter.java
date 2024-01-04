
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.config;

import io.permazen.cli.SessionMode;

import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;

public class SessionModeConverter implements ValueConverter<SessionMode> {

    @Override
    public SessionMode convert(String value) {
        try {
            return SessionModeConverter.toMode(value);
        } catch (IllegalArgumentException e) {
            throw new ValueConversionException("invalid session mode \"" + value + "\"");
        }
    }

    @Override
    public Class<SessionMode> valueType() {
        return SessionMode.class;
    }

    @Override
    public String valuePattern() {
        return "session-mode";
    }

    public static String toString(SessionMode mode) {
        return mode.name().toLowerCase().replace('_', '-');
    }

    public static SessionMode toMode(String mode) {
        return SessionMode.valueOf(mode.replace('-', '_').toUpperCase());
    }
}
