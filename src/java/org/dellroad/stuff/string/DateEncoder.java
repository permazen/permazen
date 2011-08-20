
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encodes {@code Date} objects to and from strings.
 */
public final class DateEncoder {

    /**
     * Regular expression matching properly encoded strings.
     */
    public static final String PATTERN = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{3})?(Z)?";

    private static final String FORMAT_SECONDS = "yyyy-MM-dd'T'HH:mm:ss";
    private static final String FORMAT_MILLIS = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final String FORMAT_Z_SUFFIX = "'Z'";

    private DateEncoder() {
    }

    /**
     * Encode the given date as a string of the form {@code 2009-12-01T15:33:07.763Z}.
     * If the fractional seconds portion is zero, it will be omitted.
     *
     * @param date date to encode
     * @throws NullPointerException if {@code date} is {@code null}
     */
    public static String encode(Date date) {
        String format = (date.getTime() % 1000 != 0 ? FORMAT_MILLIS : FORMAT_SECONDS) + FORMAT_Z_SUFFIX;
        return getDateFormat(format).format(date);
    }

    /**
     * Decode the given date.
     *
     * @param string encoded date
     * @throws IllegalArgumentException if {@code string} is malformed
     * @throws NullPointerException if {@code string} is {@code null}
     */
    public static Date decode(String string) {
        Matcher matcher = Pattern.compile(PATTERN).matcher(string);
        if (!matcher.matches())
            throw new IllegalArgumentException("malformed date string");
        String format = matcher.group(1) != null ? FORMAT_MILLIS : FORMAT_SECONDS;
        if (matcher.group(2) != null)
            format += FORMAT_Z_SUFFIX;
        try {
            return DateEncoder.getDateFormat(format).parse(matcher.group());
        } catch (ParseException e) {
            throw new RuntimeException("unexpected");
        }
    }

    private static SimpleDateFormat getDateFormat(String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        dateFormat.setLenient(false);
        return dateFormat;
    }
}

