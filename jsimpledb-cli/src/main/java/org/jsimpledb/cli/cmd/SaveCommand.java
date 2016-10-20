
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import com.google.common.collect.Iterables;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.JObject;
import org.jsimpledb.Session;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.util.XMLObjectSerializer;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.util.ParseCastFunction;
import org.jsimpledb.util.ParseContext;

public class SaveCommand extends AbstractCommand {

    public SaveCommand() {
        super("save --storage-id-format:storageIdFormat -w:weak file.xml:file expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Exports objects to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Evaluates the expression, which must evaluate to an Iterator (or Iterable) of database objects,"
          + " and writes the objects to the specified XML file. Objects can be read back in later via `load'.\n"
          + "If the `-w' flag is given, for certain key/value stores a weaker consistency level is used for"
          + " the tranasction to reduce the chance of conflicts.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new OutputFileParser() : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final boolean nameFormat = !params.containsKey("storageIdFormat");
        final boolean weak = params.containsKey("weak");
        final File file = (File)params.get("file.xml");
        final Node expr = (Node)params.get("expr");

        // Return action
        return new SaveAction(nameFormat, weak, file, expr);
    }

    private static class SaveAction implements CliSession.Action, Session.RetryableAction, Session.HasTransactionOptions {

        private final boolean nameFormat;
        private final boolean weak;
        private final File file;
        private final Node expr;

        SaveAction(boolean nameFormat, boolean weak, File file, Node expr) {
            this.nameFormat = nameFormat;
            this.weak = weak;
            this.file = file;
            this.expr = expr;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final Value value = this.expr.evaluate(session);
            final Iterable<?> i = value.checkType(session, "save", Iterable.class);
            final FileOutputStream updateOutput = !this.isWindows() ?
              new AtomicUpdateFileOutputStream(this.file) : new FileOutputStream(this.file);
            final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
            boolean success = false;
            final int count;
            try {
                final XMLStreamWriter writer = new IndentXMLStreamWriter(
                  XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8"));
                writer.writeStartDocument("UTF-8", "1.0");
                count = new XMLObjectSerializer(session.getTransaction()).write(writer, this.nameFormat,
                  Iterables.transform(i, new ParseCastFunction<ObjId>(ObjId.class) {
                    @Override
                    public ObjId apply(Object obj) {
                        return obj instanceof JObject ? ((JObject)obj).getObjId() : super.apply(obj);
                    }
                  }));
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
            session.getWriter().println("Wrote " + count + " objects to `" + this.file + "'");
        }

        // Use EVENTUAL_COMMITTED consistency for Raft key/value stores to avoid retries
        @Override
        public Map<String, ?> getTransactionOptions() {
            return this.weak ? Collections.singletonMap("consistency", "EVENTUAL") : null;
        }

        private boolean isWindows() {
            return System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH).indexOf("win") != -1;
        }
    }
}

