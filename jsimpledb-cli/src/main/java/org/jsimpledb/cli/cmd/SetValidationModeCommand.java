
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.EnumSet;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.EnumNameParser;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.util.ParseContext;

public class SetValidationModeCommand extends AbstractCommand {

    public SetValidationModeCommand() {
        super("set-validation-mode mode:mode");
    }

    @Override
    public String getHelpSummary() {
        return "Sets the validation mode for JSimpleDB transactions";
    }

    @Override
    public String getHelpDetail() {
        return "Sets the validation mode for JSimpleDB transactions, one of `disabled', `manual', or `automatic' (the default).";
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

