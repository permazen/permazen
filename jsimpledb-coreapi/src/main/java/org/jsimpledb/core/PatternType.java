
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Converter;

import java.util.regex.Pattern;

/**
 * {@link Pattern} type. Null values are supported by this class.
 *
 * <b>Note:</b> equality is not consistent with {@link Pattern#equals}, which is not implemented.
 */
class PatternType extends StringEncodedType<Pattern> {

    PatternType() {
        super(Pattern.class, 0, new Converter<Pattern, String>() {

            @Override
            protected String doForward(Pattern pattern) {
                if (pattern == null)
                    return null;
                return pattern.toString();
            }

            @Override
            protected Pattern doBackward(String string) {
                if (string == null)
                    return null;
                return Pattern.compile(string);
            }
        });
    }
}

