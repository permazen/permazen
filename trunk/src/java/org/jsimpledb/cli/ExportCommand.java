
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterators;
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

public class ExportCommand extends AbstractSimpleCommand<CommandParser> {

    public ExportCommand() {
        super("export");
    }

    @Override
    public String getUsage() {
        return this.name + " [--storage-id-format] file.xml";
    }

    @Override
    public String getHelpSummary() {
        return "Exports objects from input channel to the specified XML file.";
    }

    @Override
    protected CommandParser getParameters(Session session, Channels channels, ParseContext ctx) {

        // Verify only one input channel
        this.checkChannelCount(channels, ctx, 1);

        // Verify channel has the correct item type
        this.checkItemType(channels, ctx, ObjId.class);

        // Parse command line
        final CommandParser parser = new CommandParser(1, 1, this.getUsage(), "--storage-id-format").parse(ctx);

        // Check file
        final String path = parser.getParam(0);
        final File file = new File(path);
        if (file.exists() && !file.isDirectory() && file.canWrite())
            return parser;
        if (file.getParentFile() == null || (file.getParentFile().exists() && file.getParentFile().canWrite()))
            return parser;

        // Not possible to write file, compute completions
        final ArrayList<CharSequence> list = new ArrayList<>();
        new FileNameCompleter().complete(path, ctx.getIndex(), list);
        throw new ParseException(ctx, "can't write to file `" + path + "'")
          .addCompletions(Lists.transform(list, new CastFunction<String>(String.class)));
    }

    @Override
    protected String getResult(Session session, Channels channels, CommandParser parser) {
        final File file = new File(parser.getParam(0));
        final boolean nameFormat = !parser.hasFlag("--storage-id-format");
        try {
            final AtomicUpdateFileOutputStream updateOutput = new AtomicUpdateFileOutputStream(file);
            final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
            boolean success = false;
            final int count;
            try {
                XMLStreamWriter writer = new IndentXMLStreamWriter(
                  XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8"));
                writer.writeStartDocument("UTF-8", "1.0");
                count = new XMLObjectSerializer(session.getTransaction()).write(writer, nameFormat,
                  Iterators.transform(channels.get(0).getItems(session).iterator(), new CastFunction<ObjId>(ObjId.class)));
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
        } catch (Exception e) {
            session.report(e);
        }
        return null;
    }
}

