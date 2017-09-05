
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.Session;
import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.util.XMLSerializer;
import io.permazen.parse.Parser;
import io.permazen.util.ParseContext;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.EnumSet;
import java.util.Map;

public class KVLoadCommand extends AbstractKVCommand {

    public KVLoadCommand() {
        super("kvload -R:reset file.xml:file");
    }

    @Override
    public String getHelpSummary() {
        return "Load key/value pairs from an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Imports key/value pairs from an XML file created previously via `kvsave'. Does NOT remove any key/value pairs"
          + "already in the database unless the `-R' flag is given, in which case the database is completely wiped first."
          + "\n\nWARNING: this command can corrupt a JSimpleDB database.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new InputFileParser() : super.getParser(typeName);
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.of(SessionMode.KEY_VALUE);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return new LoadAction(params.containsKey("reset"), (File)params.get("file.xml"));
    }

    private static class LoadAction implements CliSession.Action, Session.RetryableAction {

        private final boolean reset;
        private final File file;

        LoadAction(boolean reset, File file) {
            this.reset = reset;
            this.file = file;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final KVTransaction kvt = session.getKVTransaction();
            if (this.reset)
                kvt.removeRange(null, null);
            final int count;
            try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(this.file))) {
                count = new XMLSerializer(kvt).read(input);
            }
            session.getWriter().println("Read " + count + " key/value pairs from `" + this.file + "'");
        }
    }
}

