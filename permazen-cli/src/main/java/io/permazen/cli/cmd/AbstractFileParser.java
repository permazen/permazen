
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.parse.Parser;
import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

import java.io.File;
import java.util.regex.Matcher;

abstract class AbstractFileParser implements Parser<File> {

    @Override
    public File parse(Session session, ParseContext ctx, boolean complete) {

        // Get filename
        final Matcher matcher = ctx.tryPattern("[^\\s;]*");
        if (matcher == null)
            throw new ParseException(ctx);
        final String path = matcher.group();

        // Check file for validity
        final File file = new File(path);
        if (!complete && this.validateFile(file, complete))
            return file;

        // Throw parse exception
        throw this.createParseException(ctx, file);
    }

    protected abstract boolean validateFile(File file, boolean complete);

    protected abstract ParseException createParseException(ParseContext ctx, File file);
}
