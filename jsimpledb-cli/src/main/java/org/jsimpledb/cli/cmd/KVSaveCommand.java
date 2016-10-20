
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.Session;
import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.util.XMLSerializer;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.util.ParseContext;

public class KVSaveCommand extends AbstractCommand {

    public KVSaveCommand() {
        super("kvsave -i:indent file.xml:file minKey? maxKey?");
    }

    @Override
    public String getHelpSummary() {
        return "Exports key/value pairs to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Writes all key/value pairs to the specified XML file. Data can be read back in later via `kvload'."
          + "\n\nIf `minKey' and/or `maxKey' are specified, the keys are restricted to the specified range."
          + " `minKey' and `maxKey' may be given as hexadecimal strings or C-style doubly-quoted strings."
          + " The `-i' flag causes the output XML to be indented.";
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
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final File file = (File)params.get("file.xml");
        final boolean indent = params.containsKey("indent");
        final byte[] minKey = (byte[])params.get("minKey");
        final byte[] maxKey = (byte[])params.get("maxKey");

        // Return action
        return new SaveAction(file, indent, minKey, maxKey);
    }

    private static class SaveAction implements CliSession.Action, Session.RetryableAction {

        private final File file;
        private final boolean indent;
        private final byte[] minKey;
        private final byte[] maxKey;

        SaveAction(File file, boolean indent, byte[] minKey, byte[] maxKey) {
            this.file = file;
            this.indent = indent;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final FileOutputStream updateOutput = !this.isWindows() ?
              new AtomicUpdateFileOutputStream(this.file) : new FileOutputStream(this.file);
            final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
            boolean success = false;
            final int count;
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
            session.getWriter().println("Wrote " + count + " key/value pairs to `" + this.file + "'");
        }

        private boolean isWindows() {
            return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).indexOf("win") != -1;
        }
    }
}

