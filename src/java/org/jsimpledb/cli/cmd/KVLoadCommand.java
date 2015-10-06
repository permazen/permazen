
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.util.XMLSerializer;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.Parser;

@Command(modes = { SessionMode.KEY_VALUE })
public class KVLoadCommand extends AbstractKVCommand {

    public KVLoadCommand() {
        super("kvload file.xml:file");
    }

    @Override
    public String getHelpSummary() {
        return "Load key/value pairs from an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Imports key/value pairs from an XML file created previously via `kvsave'. Does not remove any key/value pairs"
          + "already in the database."
          + "\n\nWARNING: this command can corrupt a JSimpleDB database.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new InputFileParser() : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final File file = (File)params.get("file.xml");
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                final int count;
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(file))) {
                    count = new XMLSerializer(session.getKVTransaction()).read(input);
                }
                session.getWriter().println("Read " + count + " key/value pairs from `" + file + "'");
            }
        };
    }
}

