
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.parse.Parser;
import io.permazen.core.util.XMLObjectSerializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;

public class SaveCommand extends AbstractCommand {

    public SaveCommand() {
        super("save --storage-id-format:storageIdFormat -w:weak file.xml:file");
    }

    @Override
    public String getHelpSummary() {
        return "Exports objects to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Writes all database objects to the specified XML file. Objects can be read back in later via `load'.\n"
          + "If the `-w' flag is given, for certain key/value stores a weaker consistency level is used for"
          + " the tranasction to reduce the chance of conflicts.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new OutputFileParser() : super.getParser(typeName);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {

        // Parse parameters
        final boolean nameFormat = !params.containsKey("storageIdFormat");
        final boolean weak = params.containsKey("weak");
        final File file = (File)params.get("file.xml");

        // Return action
        return new SaveAction(nameFormat, weak, file);
    }

    private static class SaveAction implements Session.RetryableTransactionalAction, Session.TransactionalActionWithOptions {

        private final boolean nameFormat;
        private final boolean weak;
        private final File file;

        SaveAction(boolean nameFormat, boolean weak, File file) {
            this.nameFormat = nameFormat;
            this.weak = weak;
            this.file = file;
        }

        @Override
        public void run(Session session) throws Exception {
            final FileOutputStream updateOutput = !this.isWindows() ?
              new AtomicUpdateFileOutputStream(this.file) : new FileOutputStream(this.file);
            final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
            boolean success = false;
            final int count;
            try {
                count = new XMLObjectSerializer(session.getTransaction()).write(output, this.nameFormat, true);
                success = true;
            } finally {
                if (success) {
                    try {
                        output.close();
                    } catch (IOException e) {
                        // ignore
                    }
                } else if (updateOutput instanceof AtomicUpdateFileOutputStream)
                    ((AtomicUpdateFileOutputStream)updateOutput).cancel();
            }
            session.getOutput().println("Wrote " + count + " objects to \"" + this.file + "\"");
        }

        // Use EVENTUAL_COMMITTED consistency for Raft key/value stores to avoid retries
        @Override
        public Map<String, ?> getTransactionOptions() {
            return this.weak ? Collections.singletonMap("consistency", "EVENTUAL") : null;
        }

        private boolean isWindows() {
            return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).contains("win");
        }
    }
}
