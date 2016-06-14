
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import com.google.common.base.Preconditions;

import java.util.EnumSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.cli.ParamParser;
import org.jsimpledb.parse.FieldTypeParser;
import org.jsimpledb.parse.ObjIdParser;
import org.jsimpledb.parse.ObjTypeParser;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for CLI {@link Command} implementations.
 *
 * @see Command
 */
public abstract class AbstractCommand implements Command {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String name;
    protected final ParamParser paramParser;

// Constructors

    /**
     * Constructor.
     *
     * @param spec {@link ParamParser} spec string, possibly containing custom type names (see {@link #getParser getParser()})
     */
    protected AbstractCommand(String spec) {
        Preconditions.checkArgument(spec != null, "null spec");
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
                    return AbstractCommand.this.getParser(typeName);
                }
            }
        };
    }

// Command stuff

    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The implementation in {@link AbstractCommand} delegates to {@link ParamParser#getUsage ParamParser.getUsage()}.
     *
     * @return command usage string
     */
    @Override
    public String getUsage() {
        return this.paramParser.getUsage(this.name);
    }

    /**
     * Get summarized help (typically a single line).
     *
     * @return one line command summary
     */
    @Override
    public abstract String getHelpSummary();

    /**
     * {@inheritDoc}
     *
     * <p>
     * The implementation in {@link AbstractCommand} delegates to {@link #getHelpSummary getHelpSummary()}.
     *
     * @return detailed command description
     */
    @Override
    public String getHelpDetail() {
        return this.getHelpSummary();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The implementation in {@link AbstractCommand} returns an {@link EnumSet} containing
     * {@link SessionMode#CORE_API} and {@link SessionMode#JSIMPLEDB}.
     *
     * @return set of supported {@link SessionMode}s
     */
    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.<SessionMode>of(SessionMode.CORE_API, SessionMode.JSIMPLEDB);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The implementation in {@link AbstractCommand} parses the parameters and delegates to {@link #getAction getAction()}
     * with the result.
     *
     * @param session CLI session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @return action to perform for the parsed command
     * @throws org.jsimpledb.parse.ParseException if parse fails, or if {@code complete} is true and there are valid completions
     * @throws org.jsimpledb.parse.ParseException if parameters are invalid
     */
    @Override
    public CliSession.Action parse(ParseSession session, ParseContext ctx, boolean complete) {
        return this.getAction((CliSession)session, ctx, complete, this.paramParser.parse(session, ctx, complete));
    }

    /**
     * Process command line parameters and return action.
     *
     * @param session CLI session
     * @param ctx input to parse
     * @param complete false if parse is "for real", true if only for tab completion calculation
     * @param params parsed parameters indexed by name
     * @return action to perform for the parsed command
     * @throws org.jsimpledb.parse.ParseException if parse fails, or if {@code complete} is true and there are valid completions
     * @throws org.jsimpledb.parse.ParseException if parameters are invalid
     */
    protected abstract CliSession.Action getAction(CliSession session,
      ParseContext ctx, boolean complete, Map<String, Object> params);

    /**
     * Convert parameter spec type name into a {@link Parser}. Used for custom type names not supported by {@link ParamParser}.
     *
     * <p>
     * The implementation in {@link AbstractCommand} supports all {@link org.jsimpledb.core.FieldType}s registered with the
     * database, {@code type} for an object type name (returns {@link Integer}), and {@code objid} for an object ID
     * (returns {@link org.jsimpledb.core.ObjId}).
     * </p>
     *
     * @param typeName parameter type name
     * @return parser for parameters of the specified type
     */
    protected Parser<?> getParser(String typeName) {
        Preconditions.checkArgument(typeName != null, "null typeName");
        if (typeName.equals("type"))
            return new ObjTypeParser();
        if (typeName.equals("objid"))
            return new ObjIdParser();
        if (typeName.equals("expr"))
            return new ExprParser();
        return FieldTypeParser.getFieldTypeParser(typeName);
    }
}

