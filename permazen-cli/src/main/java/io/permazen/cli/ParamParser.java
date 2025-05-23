
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import io.permazen.cli.parse.Parser;
import io.permazen.cli.parse.WhateverParser;
import io.permazen.util.ParseException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses command line parameters, including optional flags, based on a specification string.
 *
 * <p>
 * The specification string contains whitespace-separated parameter specifications; see {@link Param} for syntax.
 * Subclasses must provide the parsers for the parameter type specification names via {@link #getParser(String)}.
 */
public abstract class ParamParser {

    private final LinkedHashSet<Param> optionFlags = new LinkedHashSet<>();
    private final ArrayList<Param> params = new ArrayList<>();

    @SuppressWarnings("this-escape")
    public ParamParser(String spec) {
        if (spec.length() > 0) {
            for (String pspec : spec.split("\\s+")) {
                final Param param = new Param(pspec);
                if (param.isOption())
                    this.optionFlags.add(param);
                else
                    this.params.add(param);
            }
        }
    }

    /**
     * Build a usage summary string.
     *
     * @return usage string, not including the command name
     */
    public String getUsage() {
        final StringBuilder buf = new StringBuilder();
        for (Param param : this.optionFlags) {
            if (buf.length() > 0)
                buf.append(' ');
            buf.append('[').append(param.getOptionFlag());
            if (param.getTypeName() != null)
                buf.append(' ').append(param.getName());
            buf.append(']');
        }
        for (Param param : this.params) {
            if (buf.length() > 0)
                buf.append(' ');
            if (param.getMin() == 0)
                buf.append('[');
            buf.append(param.getName());
            if (param.getMax() > 1)
                buf.append(" ...");
            if (param.getMin() == 0)
                buf.append(']');
        }
        return buf.toString();
    }

    /**
     * Get option flags.
     *
     * @return optional parameters
     */
    public Set<Param> getOptionFlags() {
        return this.optionFlags;
    }

    /**
     * Get regular parameters, in expected order.
     *
     * @return mandatory parameters
     */
    public List<Param> getParameters() {
        return this.params;
    }

    /**
     * Convert parameter spec type name into a {@link Parser}.
     *
     * @param typeName name of type
     * @return parser for parameters of the specified type
     * @throws IllegalArgumentException if {@code typeName} is unknown
     */
    protected abstract Parser<?> getParser(String typeName);

    /**
     * Parse command line parameters.
     *
     * @param session associated session
     * @param params command parameters
     * @throws ParseException if parse fails
     */
    public Map<String, Object> parse(Session session, List<String> params) {

        // Store results here
        final HashMap<String, Object> values = new HashMap<>();

        // First parse option flags
        int pos = 0;
        while (pos < params.size()) {
            final String param = params.get(pos);

            // Explicit end of option flags?
            if (param.equals("--")) {
                pos++;
                break;
            }

            // Done with option flags?
            if (!param.matches("-.+"))
                break;
            pos++;

            // Find matching Param
            final Param optionFlag = this.optionFlags.stream()
              .filter(p -> param.equals(p.getOptionFlag()))
              .findAny().orElseThrow(() -> new IllegalArgumentException(String.format("unrecognized option \"%s\"", param)));

            // Parse option argument, if any
            final Parser<?> parser = optionFlag.getParser();
            if (parser != null) {
                if (pos == params.size())
                    throw new IllegalArgumentException(String.format("option \"%s\" requires an argument", param));
                values.put(optionFlag.getName(), parser.parse(session, params.get(pos++)));
            } else
                values.put(optionFlag.getName(), true);
        }

        // Next parse regular parameters
        for (Param param : this.params) {
            final ArrayList<Object> paramValues = new ArrayList<>();
            final String typeName = param.getTypeName();
            final Parser<?> parser = param.getParser();
            while (paramValues.size() < param.getMax() && pos < params.size())
                paramValues.add(parser.parse(session, params.get(pos++)));
            if (paramValues.size() < param.getMin())
                throw new IllegalArgumentException(String.format("missing \"%s\" parameter", param.getName()));
            if (param.getMax() > 1)
                values.put(param.getName(), Arrays.asList(paramValues.toArray()));
            else if (!paramValues.isEmpty())
                values.put(param.getName(), paramValues.get(0));
        }

        // Check for trailing garbage
        if (pos != params.size())
            throw new IllegalArgumentException("too many parameters");

        // Done
        return values;
    }

// Param

    /**
     * Represents one parsed parameter specification.
     *
     * <p>
     * {@link String} form is {@code -flag:name:type}, where the {@code -flag} is optional and indicates
     * an option flag, {@code name} is the name of the flag or parameter, and {@code type} is optional as well:
     * if missing, it indicates either an argument-less option flag, or a arbitrary string. Otherwise {@code type}
     * is the name of a parameter type supported by {@link ParamParser#getParser ParamParser.getParser()}.
     *
     * <p>
     * Non-option parameters may have a {@code ?} suffix if optional,
     * or a {@code +}, or {@code *} suffix if repeatable, in which case the result is a {@link List}.
     *
     * <p>
     * Spec string syntax examples:
     * <ul>
     *  <li><code>-v:foo</code> - boolean flag named {@code foo}</li>
     *  <li><code>-v:foo:int</code> - integer flag named {@code foo}</li>
     *  <li><code>foo</code> - string (word) parameter</li>
     *  <li><code>foo:int</code> - {@code int} parameter</li>
     *  <li><code>foo?</code> - optional final parameter</li>
     *  <li><code>foo*</code> - array of zero or more final parameters</li>
     *  <li><code>foo+</code> - array of one or more final parameters</li>
     *  <li><code>foo:int+</code> - array of one or more final {@code int} parameters</li>
     * </ul>
     */
    public class Param {

        private final String optionFlag;
        private final String name;
        private final String typeName;
        private final Parser<?> parser;
        private final int min;
        private final int max;

        @SuppressWarnings("this-escape")
        public Param(String spec) {

            // Sanity check
            Preconditions.checkArgument(spec != null, "null spec");

            // Apply pattern
            final Pattern pattern = Pattern.compile("((-[^\\s:]+):)?([^-][^\\s:?+*]*)(:([^\\s?+*]+))?([?+*])?");
            final Matcher matcher = pattern.matcher(spec);
            if (!matcher.matches())
                throw new IllegalArgumentException(String.format("invalid parameter spec \"%s\"", spec));

            // Get components
            this.optionFlag = matcher.group(2);
            this.name = matcher.group(3);
            this.typeName = matcher.group(5);
            final String repeat = matcher.group(6);
            if (repeat == null) {
                this.min = 1;
                this.max = 1;
            } else if (repeat.charAt(0) == '?') {
                this.min = 0;
                this.max = 1;
            } else if (repeat.charAt(0) == '*') {
                this.min = 0;
                this.max = Integer.MAX_VALUE;
            } else if (repeat.charAt(0) == '+') {
                this.min = 1;
                this.max = Integer.MAX_VALUE;
            } else
                throw new IllegalArgumentException(String.format("invalid parameter spec \"%s\"", spec));

            // Get parser
            this.parser = this.typeName != null ?
              ParamParser.this.getParser(typeName) : !this.isOption() ? new WhateverParser() : null;
        }

        public String getOptionFlag() {
            return this.optionFlag;
        }

        public boolean isOption() {
            return this.optionFlag != null;
        }

        public String getName() {
            return this.name;
        }

        public String getTypeName() {
            return this.typeName;
        }

        public int getMin() {
            return this.min;
        }

        public int getMax() {
            return this.max;
        }

        public Parser<?> getParser() {
            return this.parser;
        }

        @Override
        public String toString() {
            return (this.optionFlag != null ? this.optionFlag + ":" : "")
              + this.name
              + (this.typeName != null ? ":" + this.typeName : "")
              + (this.min == 0 && this.max == 1 ? "?" :
               this.min == 0 && this.max == Integer.MAX_VALUE ? "*" :
               this.min == 1 && this.max == Integer.MAX_VALUE ? "+" : "");
        }
    }
}
