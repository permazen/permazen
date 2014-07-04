
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.FieldTypeParser;
import org.jsimpledb.cli.parse.ObjIdParser;
import org.jsimpledb.cli.parse.ObjTypeParser;
import org.jsimpledb.cli.parse.ParamParser;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.expr.ExprParser;
import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Superclass of all CLI commands.
 *
 * @see CliCommand
 */
public abstract class Command implements Parser<Action> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String name;
    protected final ParamParser paramParser;

// Constructors

    /**
     * Constructor.
     *
     * @param spec {@link ParamParser} spec string, possibly containing custom type names (see {@link #getParser getParser()})
     */
    protected Command(String spec) {
        if (spec == null)
            throw new IllegalArgumentException("null spec");
        final Matcher matcher = Pattern.compile("([^\\s]+)(\\s+(.*))?").matcher(spec);
        if (!matcher.matches())
            throw new IllegalArgumentException("invalid command specification `" + spec + "'");
        this.name = matcher.group(1);
        final String paramSpec = matcher.group(3);
        this.paramParser = new ParamParser(paramSpec != null ? paramSpec : "") {
            @Override
            protected Parser<?> getParser(String typeName) {
                try {
                    return super.getParser(typeName);
                } catch (IllegalArgumentException e) {
                    return Command.this.getParser(typeName);
                }
            }
        };
    }

// Command stuff

    /**
     * Get the name of this command.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get command usage string.
     *
     * <p>
     * The implementation in {@link Command} delegates to {@link ParamParser#getUsage ParamParser.getUsage()}.
     * </p>
     */
    public String getUsage() {
        return this.paramParser.getUsage(this.name);
    }

    /**
     * Get summarized help (typically a single line).
     */
    public abstract String getHelpSummary();

    /**
     * Get expanded help (typically multiple lines).
     *
     * <p>
     * The implementation in {@link Command} delegates to {@link #getHelpSummary getHelpSummary()}.
     * </p>
     */
    public String getHelpDetail() {
        return this.getHelpSummary();
    }

    /**
     * Parse command line and return command action.
     *
     * <p>
     * The implementation in {@link ParamParser} parses the parameters and delegates to {@link #getAction} with the result.
     * </p>
     *
     * @param session CLI session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @throws ParseException if parse fails, or if {@code complete} is true and there are valid completions
     * @throws ParseException if parameters are invalid
     */
    @Override
    public Action parse(Session session, ParseContext ctx, boolean complete) {
        return this.getAction(session, ctx, complete, this.paramParser.parse(session, ctx, complete));
    }

    /**
     * Process command line parameters and return action.
     *
     * @param session CLI session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @throws ParseException if parse fails, or if {@code complete} is true and there are valid completions
     * @throws ParseException if parameters are invalid
     */
    public abstract Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params);

    /**
     * Convert parameter spec type name into a {@link Parser}. Used for custom type names not supported by {@link ParamParser}.
     *
     * <p>
     * The implementation in {@link ParamParser} supports all {@link org.jsimpledb.core.FieldType}s registered with the database,
     * {@code type} for an object type name (returns {@link Integer}), and {@code objid} for an object ID
     * (returns {@link org.jsimpledb.core.ObjId}).
     * </p>
     */
    protected Parser<?> getParser(String typeName) {
        if (typeName == null)
            throw new IllegalArgumentException("null typeName");
        if (typeName.equals("type"))
            return new ObjTypeParser();
        if (typeName.equals("objid"))
            return new ObjIdParser();
        if (typeName.equals("expr"))
            return new ExprParser();
        return FieldTypeParser.getFieldTypeParser(typeName);
    }
}

