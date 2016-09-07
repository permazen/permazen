
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
        super("load -R:reset file.xml:file");
    }

    @Override
    public String getHelpSummary() {
        return "Load objects from an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Imports objects from an XML file created previously via `save'. Does NOT remove any objects already"
          + " in the database unless the `-R' flag is given, in which case the database is completely wiped first.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new InputFileParser() : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean reset = params.containsKey("reset");
        final File file = (File)params.get("file.xml");
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                final Transaction tx = session.getTransaction();
                if (reset) {
                    int deleteCount = 0;
                    for (ObjId id : tx.getAll()) {
                        tx.delete(id);
                        deleteCount++;
                    }
                    session.getWriter().println("Deleted " + deleteCount + " object(s)");
                }
                final int readCount;
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
                    readCount = new XMLObjectSerializer(session.getTransaction()).read(input);
                }
                session.getWriter().println("Read " + readCount + " object(s) from `" + file + "'");
            }
        };
    }
}

