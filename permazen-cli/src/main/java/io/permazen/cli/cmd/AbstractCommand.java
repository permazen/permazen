
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.cli.ParamParser;
import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.parse.FieldTypeParser;
import io.permazen.cli.parse.ObjIdParser;
import io.permazen.cli.parse.ObjTypeParser;
import io.permazen.cli.parse.Parser;
import io.permazen.core.FieldType;
import io.permazen.core.ObjId;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
            throw new IllegalArgumentException("invalid command specification \"" + spec + "\"");
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
     * {@link SessionMode#CORE_API} and {@link SessionMode#PERMAZEN}.
     *
     * @return set of supported {@link SessionMode}s
     */
    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.<SessionMode>of(SessionMode.CORE_API, SessionMode.PERMAZEN);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * The implementation in {@link AbstractCommand} parses the parameters, delegates to {@link #getAction getAction()}
     * to generate an action, and then executes the action.
     */
    @Override
    public int execute(Session session, String name, List<String> params) throws InterruptedException {

        // Sanity check
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(params != null, "null params");

        // Parse command line arguments and ask the command for an action to perform
        final AtomicReference<Session.Action> ref = new AtomicReference<>();
        if (!session.performSessionAction(s -> ref.set(this.getAction(s, this.paramParser.parse(s, params)))))
            return 1;

        // Perform the action
        if (!session.performSessionAction(ref.get()))
            return 1;

        // Done
        return 0;
    }

    /**
     * Process command line parameters and return action.
     *
     * @param session CLI session
     * @param params parsed parameters indexed by name
     * @return action to perform for the parsed command
     * @throws IllegalArgumentException if parameters are invalid
     */
    protected abstract Session.Action getAction(Session session, Map<String, Object> params);

    /**
     * Convert parameter spec type name into a {@link Parser}. Used for custom type names not supported by {@link ParamParser}.
     *
     * <p>
     * The implementation in {@link AbstractCommand} supports all {@link FieldType}s registered with the
     * database, plus:
     * <ul>
     *  <li>{@code type} for an object type name (returns {@link Integer})</li>
     *  <li>{@code objid} for an object ID of the form {@code 64e8f29755302fe1} (returns {@link ObjId})</li>
     * </ul>
     *
     * @param typeName parameter encoding ID or encoding ID alias
     * @return parser for parameters of the specified type
     */
    protected Parser<?> getParser(String typeName) {
        Preconditions.checkArgument(typeName != null, "null typeName");
        if (typeName.equals("type"))
            return new ObjTypeParser();
        if (typeName.equals("objid"))
            return new ObjIdParser();
        return FieldTypeParser.getFieldTypeParser(typeName);
    }
}
