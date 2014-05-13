
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.jsimpledb.util.ParseContext;
import org.jsimpledb.util.XMLObjectSerializer;

import jline.console.completer.FileNameCompleter;

public class ImportCommand extends AbstractSimpleCommand<File> {

    public ImportCommand() {
        super("import");
    }

    @Override
    public String getUsage() {
        return this.name + " file.xml";
    }

    @Override
    public String getHelpSummary() {
        return "Imports objects from an XML file created previously via `export'";
    }

    @Override
    protected File getParameters(Session session, Channels input, ParseContext ctx) {
        this.checkChannelCount(input, ctx, 0);
        final String path = new CommandParser(1, 1, this.getUsage()).parse(ctx).getParams().get(0);
        final File file = new File(path);
        if (file.exists() && !file.isDirectory() && file.canRead())
            return file;
        final ArrayList<CharSequence> list = new ArrayList<>();
        new FileNameCompleter().complete(path, ctx.getIndex(), list);
        throw new ParseException(ctx, "can't read file `" + file + "'")
          .addCompletions(Lists.transform(list, new Function<CharSequence, String>() {
            @Override
            public String apply(CharSequence seq) {
                return ((String)seq).substring(path.length());
            }
        }));
    }

    @Override
    protected String getResult(Session session, Channels channels, File file) {
        try {
            final BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
            final int count;
            try {
                count = new XMLObjectSerializer(session.getTransaction()).read(input);
            } finally {
                try {
                    input.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            session.getWriter().println("Read " + count + " objects from `" + file + "'");
        } catch (Exception e) {
            session.report(e);
        }
        return null;
    }
}

