
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.SessionMode;
import io.permazen.ValidationMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.EnumNameParser;
import io.permazen.parse.Parser;
import io.permazen.util.ParseContext;

import java.util.EnumSet;
import java.util.Map;

public class SetValidationModeCommand extends AbstractCommand {

    public SetValidationModeCommand() {
        super("set-validation-mode mode:mode");
    }

    @Override
    public String getHelpSummary() {
        return "Sets the validation mode for Permazen transactions";
    }

    @Override
    public String getHelpDetail() {
        return "Sets the validation mode for Permazen transactions, one of `disabled', `manual', or `automatic' (the default).";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.of(SessionMode.JSIMPLEDB);
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "mode".equals(typeName) ? new EnumNameParser<>(ValidationMode.class) : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final ValidationMode mode = (ValidationMode)params.get("mode");
        return session -> {
            session.setValidationMode(mode);
            session.getWriter().println("Setting validation mode to " + mode);
        };
    }
}

