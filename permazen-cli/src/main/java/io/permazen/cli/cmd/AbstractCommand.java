
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.cli.ParamParser;
import io.permazen.parse.FieldTypeParser;
import io.permazen.parse.ObjIdParser;
import io.permazen.parse.ObjTypeParser;
import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.parse.expr.ExprParser;
import io.permazen.parse.expr.Node;
import io.permazen.util.ParseContext;

import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * @throws io.permazen.parse.ParseException if parse fails, or if {@code complete} is true and there are valid completions
     * @throws io.permazen.parse.ParseException if parameters are invalid
     * @throws IllegalArgumentException if {@code session} is not a {@link CliSession}
     */
    @Override
    public CliSession.Action parse(ParseSession session, ParseContext ctx, boolean complete) {
        Preconditions.checkArgument(session instanceof CliSession, "session is not a CliSession");
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
     * @throws io.permazen.parse.ParseException if parse fails, or if {@code complete} is true and there are valid completions
     * @throws io.permazen.parse.ParseException if parameters are invalid
     */
    protected abstract CliSession.Action getAction(CliSession session,
      ParseContext ctx, boolean complete, Map<String, Object> params);

    /**
     * Convert parameter spec type name into a {@link Parser}. Used for custom type names not supported by {@link ParamParser}.
     *
     * <p>
     * The implementation in {@link AbstractCommand} supports all {@link io.permazen.core.FieldType}s registered with the
     * database, plus:
     * <ul>
     *  <li>{@code type} for an object type name (returns {@link Integer})</li>
     *  <li>{@code objid} for an object ID of the form {@code 64e8f29755302fe1} (returns {@link io.permazen.core.ObjId})</li>
     *  <li>{@code expr} for an arbitrary Java expression</li>
     * </ul>
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

    /**
     * Evaluate an {@code "expr"} parameter, expecting the specified type.
     *
     * @param session CLI session
     * @param node parsed parameter
     * @param name parameter name
     * @param type expected type
     * @param <T> expected result type
     * @return parameter value
     */
    protected <T> T getExprParam(CliSession session, Node node, String name, Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        return this.getExprParam(session, node, name, obj -> {
            if (!type.isInstance(obj)) {
                throw new IllegalArgumentException("must be of type " + type.getName()
                  + " (found " + (obj != null ? obj.getClass().getName() : "null") + ")");
            }
            return type.cast(obj);
        });
    }

    /**
     * Evaluate an {@code "expr"} parameter, expecting the parameter to pass the given test.
     *
     * @param session CLI session
     * @param node parsed parameter
     * @param name parameter name
     * @param validator validates value, or throws {@link IllegalArgumentException} if value is invalid
     * @param <T> expected result type
     * @return parameter value
     */
    protected <T> T getExprParam(CliSession session, Node node, String name, Function<Object, T> validator) {
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(node != null, "null node");
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(validator != null, "null validator");
        final Object value = node.evaluate(session).get(session);
        try {
            return validator.apply(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid `" + name + "' parameter: " + e.getMessage());
        }
    }
}

