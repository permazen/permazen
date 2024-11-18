
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.parse.Parser;
import io.permazen.core.TransactionConfig;
import io.permazen.core.util.XMLObjectSerializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;

public class SaveCommand extends AbstractCommand {

    public SaveCommand() {
        super("save --plain:plain --storage-ids:storageIds --weak:weak"
          + " --no-schemas:noSchemas --no-objects:noObjects file.xml:file");
    }

    @Override
    public String getHelpSummary() {
        return "Exports objects to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Writes all database objects to the specified XML file. Objects can be read back in later via \"load\".\n"
          + "By default, objects are written in the custom format; add the \"--plain\" flag to get the plain format.\n"
          + "You can optionally omit schemas or objects via \"--no-schemas\" and \"--no-objects\", respectively.\n"
          + "To include explicit storage ID's in the output, use the \"--storage-ids\" flag.\n"
          + "Add the \"--weak\" flag for a weaker transaction consistency level in certain key/value stores to reduce conflicts.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new OutputFileParser() : super.getParser(typeName);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {

        // Parse parameters
        final boolean plain = !params.containsKey("plain");
        final boolean storageIds = params.containsKey("storageIds");
        final boolean weak = params.containsKey("weak");
        final boolean noSchemas = params.containsKey("noSchemas");
        final boolean noObjects = params.containsKey("noObjects");
        final File file = (File)params.get("file.xml");

        // Return action
        return new SaveAction(plain, storageIds, weak, noSchemas, noObjects, file);
    }

    private static class SaveAction implements Session.RetryableTransactionalAction, Session.TransactionalActionWithOptions {

        private final boolean plain;
        private final boolean storageIds;
        private final boolean weak;
        private final boolean noSchemas;
        private final boolean noObjects;
        private final File file;

        SaveAction(boolean plain, boolean storageIds, boolean weak, boolean noSchemas, boolean noObjects, File file) {
            this.plain = plain;
            this.storageIds = storageIds;
            this.weak = weak;
            this.noSchemas = noSchemas;
            this.noObjects = noObjects;
            this.file = file;
        }

        @Override
        public SessionMode getTransactionMode(Session session) {
            return SessionMode.CORE_API;
        }

        @Override
        public TransactionConfig getTransactionConfig(Session session) {
            return Session.RetryableTransactionalAction.super.getTransactionConfig(session).copy()
              .schemaRemoval(TransactionConfig.SchemaRemoval.NEVER)
              .build();
        }

        @Override
        public void run(Session session) throws Exception {
            final FileOutputStream updateOutput = !this.isWindows() ?
              new AtomicUpdateFileOutputStream(this.file) : new FileOutputStream(this.file);
            final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
            boolean success = false;
            final long count;
            try {
                final XMLObjectSerializer.OutputOptions options = XMLObjectSerializer.OutputOptions.builder()
                  .schemaGenerator(this.noSchemas ? tx -> Stream.empty() : null)
                  .objectGenerator(this.noObjects ? tx -> Stream.empty() : null)
                  .includeStorageIds(this.storageIds)
                  .elementsAsNames(!this.plain)
                  .build();
                count = new XMLObjectSerializer(session.getTransaction()).write(output, options);
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
