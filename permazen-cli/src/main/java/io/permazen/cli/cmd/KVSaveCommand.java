
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.parse.Parser;
import io.permazen.kv.util.XMLSerializer;
import io.permazen.util.ByteData;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.xml.IndentXMLStreamWriter;

public class KVSaveCommand extends AbstractKVCommand {

    public KVSaveCommand() {
        super("kvsave -i:indent -w:weak file.xml:file minKey:bytes? maxKey:bytes?");
    }

    @Override
    public String getHelpSummary() {
        return "Exports key/value pairs to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Writes all key/value pairs to the specified XML file. Data can be read back in later via \"kvload\"."
          + "\n\nIf \"minKey\" and/or \"maxKey\" are specified, the keys are restricted to the specified range."
          + " \"minKey\" and \"maxKey\" may be given as hexadecimal strings or C-style doubly-quoted strings.\n"
          + "The \"-i\" flag causes the output XML to be indented.\n"
          + "If the \"-w\" flag is given, for certain key/value stores a weaker consistency level is used for"
          + " the tranasction to reduce the chance of conflicts.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new OutputFileParser() : super.getParser(typeName);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {

        // Parse parameters
        final File file = (File)params.get("file.xml");
        final boolean indent = params.containsKey("indent");
        final boolean weak = params.containsKey("weak");
        final ByteData minKey = (ByteData)params.get("minKey");
        final ByteData maxKey = (ByteData)params.get("maxKey");

        // Return action
        return new SaveAction(file, indent, weak, minKey, maxKey);
    }

    private static class SaveAction implements KVAction, Session.TransactionalActionWithOptions {

        private final File file;
        private final boolean indent;
        private final boolean weak;
        private final ByteData minKey;
        private final ByteData maxKey;

        SaveAction(File file, boolean indent, boolean weak, ByteData minKey, ByteData maxKey) {
            this.file = file;
            this.indent = indent;
            this.weak = weak;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        @Override
        public void run(Session session) throws Exception {
            final FileOutputStream updateOutput = !this.isWindows() ?
              new AtomicUpdateFileOutputStream(this.file) : new FileOutputStream(this.file);
            final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
            boolean success = false;
            final long count;
            try {
                XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8");
                if (this.indent)
                    writer = new IndentXMLStreamWriter(writer);
                writer.writeStartDocument("UTF-8", "1.0");
                final XMLSerializer serializer = new XMLSerializer(session.getKVTransaction());
                count = serializer.write(writer, this.minKey, this.maxKey);
                output.flush();
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
            session.getOutput().println(String.format("Wrote %d key/value pairs to \"%s\"", count, this.file));
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
