
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Converter;

import io.permazen.core.EncodingIds;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * {@link Pattern} type. Null values are supported by this class.
 *
 * <p>
 * <b>Note:</b> equality is defined by equal pattern strings, which is not consistent with
 * the method {@link Pattern#equals Pattern.equals()}, which is not implemented and therefore
 * defaults to object identity for comparision.
 */
public class PatternType extends StringEncodedType<Pattern> {

    private static final long serialVersionUID = -6406385779194286899L;

    public PatternType() {
        super(EncodingIds.builtin("Pattern"), Pattern.class, new PatternConverter());
    }

// PatternConverter

    private static class PatternConverter extends Converter<Pattern, String> implements Serializable {

        private static final long serialVersionUID = -809996726052177917L;

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
    }
}
