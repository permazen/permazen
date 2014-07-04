
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.util.Map;

import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.util.ParseContext;

@CliCommand
public class ImportCommand extends Command {

    public ImportCommand() {
        super("import name");
    }

    @Override
    public String getHelpSummary() {
        return "adds a package name to the search path for unqualified Java class names";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String name = (String)params.get("name");
        if (!name.matches("(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)+"
          + "(\\*|\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)"))
            throw new ParseException(ctx, "invalid Java import `" + name + "'");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                session.getImports().add(name);
            }
        };
    }
}

