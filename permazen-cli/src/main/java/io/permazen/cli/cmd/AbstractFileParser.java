
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.Parser;
import io.permazen.parse.util.ParseCastFunction;
import io.permazen.util.ParseContext;

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Matcher;

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
                final int suffixLength = path.length() - index;
                e.addCompletions(list.stream()
                  .map(new ParseCastFunction<String>(String.class))
                  .map(string -> string.substring(suffixLength)));
            }
        }

        // Done
        throw e;
    }

    protected abstract boolean validateFile(File file, boolean complete);

    protected abstract ParseException createParseException(ParseContext ctx, File file);
}

