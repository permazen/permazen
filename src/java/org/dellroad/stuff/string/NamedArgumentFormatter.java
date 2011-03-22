
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Interpreter for printf-style formatting strings that identifies arguments using names rather than indicies.
 *
 * <p>
 * Works just like {@link java.util.Formatter} but arguments are specified using names instead of numbers.
 * A mapping from argument name to argument value must be provided during the format operation.
 * </p>
 *
 * @since 1.0.64
 */
public class NamedArgumentFormatter {

    private static final Pattern FORMAT_PARAM_PATTERN = Pattern.compile("%(\\w+)\\$");

    /**
     * The list of argument names in the order they are used. This is the inverse mapping of {@link #fieldMap}.
     */
    protected final ArrayList<String> fieldList = new ArrayList<String>();

    /**
     * Mapping from argument name to argument list index. This is the inverse mapping of {@link #fieldList}.
     */
    protected final HashMap<String, Integer> fieldMap = new HashMap<String, Integer>();

    /**
     * The original format string provided to the constructor.
     */
    protected final String originalFormat;

    /**
     * The modified format string containing indexes instead of argument names.
     */
    protected final String indexedFormat;

    /**
     * Constructor.
     *
     * @param format format string containing argument names instead of indicies
     */
    public NamedArgumentFormatter(final String format) {
        Matcher matcher = FORMAT_PARAM_PATTERN.matcher(format);
        StringBuilder buf = new StringBuilder();
        for (int offset = 0; true; offset = matcher.end(1)) {

            // Find next conversion
            if (!matcher.find(offset)) {
                buf.append(format.substring(offset));
                break;
            }

            // Extract argument name
            String fieldName = matcher.group(1);

            // If we haven't seen this argument name yet, assign it the next parameter index
            Integer fieldIndex = this.fieldMap.get(fieldName);
            if (fieldIndex == null) {
                fieldIndex = this.fieldList.size();
                this.fieldMap.put(fieldName, fieldIndex);
                this.fieldList.add(fieldName);
            }

            // Replace field name in format string with parameter index
            buf.append(format.substring(offset, matcher.start(1)));
            buf.append(fieldIndex + 1);
        }
        this.originalFormat = format;
        this.indexedFormat = buf.toString();
    }

    /**
     * Format the string using the given arguments.
     *
     * @param argMap mapping from argument name to argument value
     * @throws IllegalFormatException if the format provided to the constructor contained illegal syntax
     * @throws IllegalFormatException if an argument value is incompatible or missing (and null would be invalid)
     */
    public String format(Map<String, Object> argMap) {

        // Put values into format parameter array
        ArrayList<Object> parameterList = new ArrayList<Object>(this.fieldList.size());
        for (String fieldName : this.fieldList)
            parameterList.add(argMap.get(fieldName));

        // Format string
        return String.format(this.indexedFormat, parameterList.toArray());
    }

    /**
     * Get the original format string provided to the constructor.
     */
    public String getFormat() {
        return this.originalFormat;
    }

    /**
     * Get the argument names found in the configured format string.
     *
     * @return argument names as a unmodifiable set
     */
    public Set<String> getArgumentNames() {
        return Collections.unmodifiableSet(this.fieldMap.keySet());
    }
}

