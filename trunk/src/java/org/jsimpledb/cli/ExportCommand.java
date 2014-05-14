
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Lists;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;
import org.jsimpledb.util.XMLObjectSerializer;

import jline.console.completer.FileNameCompleter;

public class ExportCommand extends AbstractCommand {

    public ExportCommand() {
        super("export");
    }

    @Override
    public String getUsage() {
        return this.name + " [--storage-id-format] file.xml";
    }

    @Override
    public String getHelpSummary() {
        return "Exports objects in the top channel to the specified XML file.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx) {
        final ParamParser parser = new ParamParser(1, 1, this.getUsage(), "--storage-id-format").parse(ctx);
        final boolean nameFormat = !parser.hasFlag("--storage-id-format");

        // Check file
        final String path = parser.getParam(0);
        final File file = new File(path);
        if (!(file.exists() && !file.isDirectory() && file.canWrite())
          && !(file.getParentFile() == null || (file.getParentFile().exists() && file.getParentFile().canWrite()))) {
            final ArrayList<CharSequence> list = new ArrayList<>();
            new FileNameCompleter().complete(path, ctx.getIndex(), list);
            throw new ParseException(ctx, "can't write to file `" + path + "'")
              .addCompletions(Lists.transform(list, new CastFunction<String>(String.class)));
        }

        // Return export action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final Channel<? extends ObjId> channel = ExportCommand.this.pop(session, ObjId.class);
                final AtomicUpdateFileOutputStream updateOutput = new AtomicUpdateFileOutputStream(file);
                final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
                boolean success = false;
                final int count;
                try {
                    final XMLStreamWriter writer = new IndentXMLStreamWriter(
                      XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8"));
                    writer.writeStartDocument("UTF-8", "1.0");
                    count = new XMLObjectSerializer(session.getTransaction()).write(
                      writer, nameFormat, channel.getItems(session).iterator());
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
}

