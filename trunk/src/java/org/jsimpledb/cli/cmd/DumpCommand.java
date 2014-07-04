
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.cli.Action;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.expr.Node;
import org.jsimpledb.cli.util.CastFunction;
import org.jsimpledb.cli.util.StripPrefixFunction;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;
import org.jsimpledb.util.XMLObjectSerializer;

import jline.console.completer.FileNameCompleter;

@CliCommand
public class DumpCommand extends Command {

    public DumpCommand() {
        super("dump --storage-id-format:storageIdFormat file.xml:file expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "exports objects to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Evaluates the expression, which must evaluate to an Iterator (or Iterable) of database objects,"
          + " and writes the objects to the specified XML file.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        if ("file".equals(typeName))
            return new FileParser();
        return super.getParser(typeName);
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final boolean nameFormat = !params.containsKey("storageIdFormat");
        final File file = (File)params.get("file.xml");
        final Node expr = (Node)params.get("expr");

        // Return export action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final Object value = expr.evaluate(session);
                final Iterator<?> i = value instanceof Iterator ?
                  (Iterator<?>)value : value instanceof Iterable ? ((Iterable<?>)value).iterator() : null;
                if (i == null)
                    throw new IllegalArgumentException("value of expression is neither an Iterator nor an Iterable");
                final AtomicUpdateFileOutputStream updateOutput = new AtomicUpdateFileOutputStream(file);
                final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
                boolean success = false;
                final int count;
                try {
                    final XMLStreamWriter writer = new IndentXMLStreamWriter(
                      XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8"));
                    writer.writeStartDocument("UTF-8", "1.0");
                    count = new XMLObjectSerializer(session.getTransaction()).write(writer, nameFormat,
                      Iterators.transform(i, new CastFunction<ObjId>(ObjId.class)));
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
        public File parse(Session session, ParseContext ctx, boolean complete) {

            // Get filename
            final String path = ctx.matchPrefix("[^\\s;]*").group();

            // Check file
            final File file = new File(path);
            if (file.isDirectory() || (!file.exists() && complete)) {
                final ArrayList<CharSequence> list = new ArrayList<>();
                final int index = new FileNameCompleter().complete(path, path.length(), list);
                throw new ParseException(ctx, "can't write to file `" + path + "'").addCompletions(
                  Lists.transform(Lists.transform(list, new CastFunction<String>(String.class)),
                    new StripPrefixFunction(path.substring(index))));
            }

            // Done
            return file;
        }
    }
}

