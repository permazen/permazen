
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import com.google.common.collect.Lists;

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.util.ParseCastFunction;
import org.jsimpledb.parse.util.StripPrefixFunction;

import jline.console.completer.FileNameCompleter;

class InputFileParser implements Parser<File> {

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

