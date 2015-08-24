
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.EnumNameParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.Parser;

@Command(modes = SessionMode.JSIMPLEDB)
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
    protected Parser<?> getParser(String typeName) {
        return "mode".equals(typeName) ? new EnumNameParser<ValidationMode>(ValidationMode.class) : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final ValidationMode mode = (ValidationMode)params.get("mode");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                session.setValidationMode(mode);
                session.getWriter().println("Setting validation mode to " + mode);
            }
        };
    }
}

