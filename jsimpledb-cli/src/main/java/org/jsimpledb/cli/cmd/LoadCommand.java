
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.core.util.XMLObjectSerializer;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.util.ParseContext;

public class LoadCommand extends AbstractCommand {

    public LoadCommand() {
        super("load file.xml:file");
    }

    @Override
    public String getHelpSummary() {
        return "Load objects from an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Imports objects from an XML file created previously via `save'. Does not remove any objects already"
          + " in the database.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new InputFileParser() : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final File file = (File)params.get("file.xml");

        // Return import action
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                final int count;
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
                    count = new XMLObjectSerializer(session.getTransaction()).read(input);
                }
                session.getWriter().println("Read " + count + " objects from `" + file + "'");
            }
        };
    }
}

