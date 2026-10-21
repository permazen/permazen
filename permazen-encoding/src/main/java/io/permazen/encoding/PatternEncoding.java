
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link Pattern} type.
 *
 * <p>
 * Null values are supported by this class.
 *
 * <p>
 * <b>Note:</b> equality is defined by this encoder as having exactly equal flags and pattern strings.
 * This is not consistent with the method {@link Pattern#equals Pattern.equals()}, which is not implemented
 * and therefore defaults to object identity.
 */
public class PatternEncoding extends StringConvertedEncoding<Pattern> {

    private static final long serialVersionUID = -6406385779194286899L;

    public PatternEncoding(EncodingId encodingId) {
        super(encodingId, Pattern.class, Converter.from(PatternEncoding::encodeToString, PatternEncoding::decodeFromString));
    }

// The converter methods below add and remove a "{NNN" suffix when there are non-zero flags.
// Non-LITERAL patterns can never end with such a suffix, and LITERAL patterns always have
// non-zero flags, so this encoding is unambiguous.

    private static String encodeToString(Pattern pattern) {
        final String regex = pattern.toString();
        final int flags = pattern.flags();
        if (flags == 0)
            return regex;
        return String.format("%s{%d", regex, flags);
    }

    private static Pattern decodeFromString(String string) {

        // Check for a flags prefix
        final Matcher matcher = Pattern.compile("(.*)\\{([0-9]+)").matcher(string);
        if (!matcher.matches())
            return Pattern.compile(string);

        // Decode regex and flags
        final String regex = matcher.group(1);
        final int flags;
        try {
            flags = Integer.parseInt(matcher.group(2));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid pattern flags " + matcher.group(2), e);
        }

        // Build pattern
        return Pattern.compile(regex, flags);
    }
}
