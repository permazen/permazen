
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.JObject;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.util.ParseCastFunction;
import org.jsimpledb.parse.util.StripPrefixFunction;
import org.jsimpledb.util.XMLObjectSerializer;

import jline.console.completer.FileNameCompleter;

@Command
public class SaveCommand extends AbstractCommand {

    public SaveCommand() {
        super("save --storage-id-format:storageIdFormat file.xml:file expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Exports objects to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Evaluates the expression, which must evaluate to an Iterator (or Iterable) of database objects,"
          + " and writes the objects to the specified XML file. Objects can be read back in later via `load'.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new FileParser() : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final boolean nameFormat = !params.containsKey("storageIdFormat");
        final File file = (File)params.get("file.xml");
        final Node expr = (Node)params.get("expr");

        // Return action
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final Value value = expr.evaluate(session);
                final Iterable<?> i = value.checkType(session, "save", Iterable.class);
                final AtomicUpdateFileOutputStream updateOutput = new AtomicUpdateFileOutputStream(file);
                final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
                boolean success = false;
                final int count;
                try {
                    final XMLStreamWriter writer = new IndentXMLStreamWriter(
                      XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8"));
                    writer.writeStartDocument("UTF-8", "1.0");
                    count = new XMLObjectSerializer(session.getTransaction()).write(writer, nameFormat,
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
                    } else
                        updateOutput.cancel();
                }
                session.getWriter().println("Wrote " + count + " objects to `" + file + "'");
            }
        };
    }

// FileParser

    private class FileParser implements Parser<File> {

        @Override
        public File parse(ParseSession session, ParseContext ctx, boolean complete) {

            // Get filename
            final Matcher matcher = ctx.tryPattern("[^\\s;]*");
            if (matcher == null)
                throw new ParseException(ctx);
            final String path = matcher.group();

            // Check file
            final File file = new File(path);
            if (file.isDirectory() || (!file.exists() && complete)) {
                final ArrayList<CharSequence> list = new ArrayList<>();
                final int index = new FileNameCompleter().complete(path, path.length(), list);
                throw new ParseException(ctx, "can't write to file `" + path + "'").addCompletions(
                  Lists.transform(Lists.transform(list, new ParseCastFunction<String>(String.class)),
                    new StripPrefixFunction(path.substring(index))));
            }

            // Done
            return file;
        }
    }
}

