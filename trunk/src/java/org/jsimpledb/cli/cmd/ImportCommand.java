
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;

@Command
public class ImportCommand extends AbstractCommand {

    public ImportCommand() {
        super("import name");
    }

    @Override
    public String getHelpSummary() {
        return "adds a package name to the search path for unqualified Java class names";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String name = (String)params.get("name");
        if (!name.matches("(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)+"
          + "(\\*|\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)"))
            throw new ParseException(ctx, "invalid Java import `" + name + "'");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                session.getImports().add(name);
            }
        };
    }
}

