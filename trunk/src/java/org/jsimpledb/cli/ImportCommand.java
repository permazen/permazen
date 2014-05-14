
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Lists;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.jsimpledb.util.ParseContext;
import org.jsimpledb.util.XMLObjectSerializer;

import jline.console.completer.FileNameCompleter;

public class ImportCommand extends AbstractCommand {

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
    public Action parseParameters(Session session, ParseContext ctx) {
        final String path = new ParamParser(1, 1, this.getUsage()).parse(ctx).getParam(0);

        // Check file
        final File file = new File(path);
        if (!file.exists() || file.isDirectory() || !file.canRead()) {
            final ArrayList<CharSequence> list = new ArrayList<>();
            new FileNameCompleter().complete(path, ctx.getIndex(), list);
            throw new ParseException(ctx, "can't read file `" + file + "'").addCompletions(
              Lists.transform(Lists.transform(list, new CastFunction<String>(String.class)), new StripPrefixFunction(path)));
        }

        // Return import action
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
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
}

