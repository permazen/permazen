
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.ValidationMode;
import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.EnumNameParser;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.util.ParseContext;

@CliCommand
public class SetValidationModeCommand extends Command {

    public SetValidationModeCommand() {
        super("set-validation-mode mode:mode");
    }

    @Override
    public String getHelpSummary() {
        return "Sets the validation mode for JSimpleDB transactions";
    }

    @Override
    public String getHelpDetail() {
        return "Sets the validation mode for JSimpleDB transactions, one of `disabled', `manual', or `automatic' (the default)."
          + " This setting is only used when a JSimpleDB instance has been created by the `--schema-pkg' command line flag.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "mode".equals(typeName) ? new EnumNameParser<ValidationMode>(ValidationMode.class) : super.getParser(typeName);
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final ValidationMode mode = (ValidationMode)params.get("mode");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.setValidationMode(mode);
                session.getWriter().println("Set validation mode to " + mode);
                if (session.getJSimpleDB() == null)
                    session.getWriter().println("Warning: no JSimpleDB instance was created, so validation mode will not be used");
            }
        };
    }
}

