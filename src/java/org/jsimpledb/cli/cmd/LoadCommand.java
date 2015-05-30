
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import com.google.common.collect.Lists;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.util.ParseCastFunction;
import org.jsimpledb.parse.util.StripPrefixFunction;
import org.jsimpledb.util.XMLObjectSerializer;

import jline.console.completer.FileNameCompleter;

@Command
public class LoadCommand extends AbstractCommand {

    public LoadCommand() {
        super("load file.xml:file");
    }

    @Override
    public String getHelpSummary() {
        return "Imports objects from an XML file created previously via `save'";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new FileParser() : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final File file = (File)params.get("file.xml");

        // Return import action
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
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
            if (!file.exists() || file.isDirectory() || !file.canRead()) {
                final ArrayList<CharSequence> list = new ArrayList<>();
                final int index = new FileNameCompleter().complete(path, path.length(), list);
                throw new ParseException(ctx, "can't read file `" + file + "'").addCompletions(
                  Lists.transform(Lists.transform(list, new ParseCastFunction<String>(String.class)),
                    new StripPrefixFunction(path.substring(index))));
            }

            // Done
            return file;
        }
    }
}

