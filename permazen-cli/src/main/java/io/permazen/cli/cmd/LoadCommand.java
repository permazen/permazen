
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import io.permazen.Session;
import io.permazen.cli.CliSession;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.util.XMLObjectSerializer;
import io.permazen.parse.Parser;
import io.permazen.util.ParseContext;

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
        return new LoadAction(file, reset);
    }

    private static class LoadAction implements CliSession.Action, Session.RetryableAction {

        private final File file;
        private final boolean reset;

        LoadAction(File file, boolean reset) {
            this.file = file;
            this.reset = reset;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final Transaction tx = session.getTransaction();
            if (this.reset) {
                int deleteCount = 0;
                for (ObjId id : tx.getAll()) {
                    tx.delete(id);
                    deleteCount++;
                }
                session.getWriter().println("Deleted " + deleteCount + " object(s)");
            }
            final int readCount;
            try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(this.file))) {
                readCount = new XMLObjectSerializer(session.getTransaction()).read(input);
            }
            session.getWriter().println("Read " + readCount + " object(s) from `" + this.file + "'");
        }
    }
}

