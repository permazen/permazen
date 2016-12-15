
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.FieldTypeRegistry;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.WordParser;
import org.jsimpledb.util.ParseContext;

/**
 * Parses command line parameters, including optional flags, based on a specification string.
 *
 * <p>
 * The specification string contains whitespace-separated parameter specifications; see {@link Param} for syntax.
 */
public class ParamParser implements Parser<Map<String, Object>> {

    private final LinkedHashSet<Param> optionFlags = new LinkedHashSet<>();
    private final ArrayList<Param> params = new ArrayList<>();
    private final FieldTypeRegistry fieldTypeRegistry = new FieldTypeRegistry();

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
     * @param commandName command name
     * @return usage string
     */
    public String getUsage(String commandName) {
        final StringBuilder buf = new StringBuilder(commandName);
        for (Param param : this.optionFlags) {
            buf.append(" [").append(param.getOptionFlag());
            if (param.getTypeName() != null)
                buf.append(' ').append(param.getName());
            buf.append(']');
        }
        for (Param param : this.params) {
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
     * <p>
     * The implementation in {@link ParamParser} supports all of the pre-defined types of {@link FieldTypeRegistry}
     * (identified by their names), plus {@code word} to parse a {@link String} containing one or more non-whitespace characters.
     * Subclasses should override as required to add additional supported types.
     *
     * @param typeName name of type
     * @return parser for parameters of the specified type
     * @throws IllegalArgumentException if {@code typeName} is unknown
     */
    protected Parser<?> getParser(String typeName) {
        Preconditions.checkArgument(typeName != null, "null typeName");
        if (typeName.equals("word"))
            return new WordParser("parameter");
        final FieldType<?> fieldType = this.fieldTypeRegistry.getFieldType(typeName);
        if (fieldType != null) {
            return (session, ctx, complete) -> {
                try {
                    return fieldType.fromParseableString(ctx);
                } catch (IllegalArgumentException e) {
                    throw new ParseException(ctx, "invalid " + fieldType.getName() + " value", e);
                }
            };
        }
        throw new IllegalArgumentException("unknown parameter type `" + typeName + "'");
    }

    /**
     * Parse command line parameters.
     *
     * @param session associated session
     * @param ctx parse context positioned at whitespace preceeding parameters (if any)
     * @param complete true if we're only determining completions
     * @throws ParseException if parse fails
     */
    @Override
    public Map<String, Object> parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Store results here
        final HashMap<String, Object> values = new HashMap<>();

        // First parse options
        boolean needSpace = !this.params.isEmpty() && this.params.get(0).getMin() > 0;
        while (true) {
            new SpaceParser(needSpace).parse(ctx, complete);
            needSpace = false;
            if (ctx.getInput().matches("(?s)^--([\\s;].*)?$")) {
                ctx.setIndex(ctx.getIndex() + 2);
                needSpace = !this.params.isEmpty() && this.params.get(0).getMin() > 0;
                break;
            }
            final Matcher matcher = ctx.tryPattern("(-[^\\s;]+)");
            if (matcher == null)
                break;
            final String option = matcher.group(1);
            final Param param = Iterables.find(this.optionFlags, new Predicate<Param>() {
                @Override
                public boolean apply(Param param) {
                    return option.equals(param.getOptionFlag());
                }
            }, null);
            if (param == null) {
                throw new ParseException(ctx, "unrecognized option `" + option + "'").addCompletions(
                  ParseUtil.complete(Iterables.transform(this.optionFlags, new Function<Param, String>() {
                    @Override
                    public String apply(Param param) {
                        return param.getOptionFlag();
                    }
                }), option));
            }
            final Parser<?> parser = param.getParser();
            if (parser != null) {
                new SpaceParser(true).parse(ctx, complete);
                values.put(param.getName(), parser.parse(session, ctx, complete));
            } else
                values.put(param.getName(), true);
            needSpace = !this.params.isEmpty() && this.params.get(0).getMin() > 0;
        }

        // Next parse parameters
        for (Param param : this.params) {
            final ArrayList<Object> paramValues = new ArrayList<>();
            final String typeName = param.getTypeName();
            final Parser<?> parser = param.getParser();
            while (paramValues.size() < param.getMax()) {
                new SpaceParser(needSpace).parse(ctx, complete);
                needSpace = false;
                if (!ctx.getInput().matches("(?s)^[^\\s;].*$")) {
                    if (complete) {
                        parser.parse(session, new ParseContext(""), true);      // calculate completions from empty string
                        throw new ParseException(ctx, "");                      // should never get here
                    }
                    break;
                }
                paramValues.add(parser.parse(session, ctx, complete));
                needSpace = paramValues.size() < param.getMin();
            }
            if (paramValues.size() < param.getMin()) {
                final ParseException e = new ParseException(ctx, "missing `" + param.getName() + "' parameter");
                if (complete) {
                    try {
                        parser.parse(session, new ParseContext(""), true);
                    } catch (ParseException e2) {
                        e.addCompletions(e2.getCompletions());
                    }
                }
                throw e;
            }
            if (param.getMax() > 1)
                values.put(param.getName(), Arrays.asList(paramValues.toArray()));
            else if (!paramValues.isEmpty())
                values.put(param.getName(), paramValues.get(0));
        }

        // Check for trailing garbage
        new SpaceParser().parse(ctx, complete);
        if (!ctx.getInput().matches("(?s)^(;.*)?$"))
            throw new ParseException(ctx);

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
     * if missing, it indicates either an argumment-less option flag or a "word" type ({@link String} that is a
     * sequence of one or more non-whitespace characters). Otherwise {@code type} is the name of a parameter type
     * supported by {@link ParamParser#getParser ParamParser.getParser()}.
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

        public Param(String spec) {

            // Sanity check
            Preconditions.checkArgument(spec != null, "null spec");

            // Apply pattern
            final Pattern pattern = Pattern.compile("((-[^\\s:]+):)?([^-][^\\s:?+*]*)(:([^\\s?+*]+))?([?+*])?");
            final Matcher matcher = pattern.matcher(spec);
            if (!matcher.matches())
                throw new IllegalArgumentException("invalid parameter spec `" + spec + "'");

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
                throw new IllegalArgumentException("invalid parameter spec `" + spec + "'");

            // Get parser
            this.parser = this.typeName != null ?
              ParamParser.this.getParser(typeName) : !this.isOption() ? new WordParser("parameter") : null;
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

