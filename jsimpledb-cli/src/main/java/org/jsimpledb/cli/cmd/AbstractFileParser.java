
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import com.google.common.collect.Lists;

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.util.ParseCastFunction;
import org.jsimpledb.parse.util.StripPrefixFunction;
import org.jsimpledb.util.ParseContext;

import jline.console.completer.FileNameCompleter;

abstract class AbstractFileParser implements Parser<File> {

    @Override
    public File parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Get filename
        final Matcher matcher = ctx.tryPattern("[^\\s;]*");
        if (matcher == null)
            throw new ParseException(ctx);
        final String path = matcher.group();

        // Check file for validity
        final File file = new File(path);
        if (!complete && this.validateFile(file, complete))
            return file;

        // Create parse exception
        final ParseException e = this.createParseException(ctx, file);

        // Add filename completions, if requested
        if (complete) {
            final ArrayList<CharSequence> list = new ArrayList<>();
            final int index = new FileNameCompleter().complete(path, path.length(), list);
            if (index != -1) {
                e.addCompletions(Lists.transform(Lists.transform(list, new ParseCastFunction<String>(String.class)),
                  new StripPrefixFunction(path.substring(index))));
            }
        }

        // Done
        throw e;
    }

    protected abstract boolean validateFile(File file, boolean complete);

    protected abstract ParseException createParseException(ParseContext ctx, File file);
}

