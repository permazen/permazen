
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

import java.io.File;

class OutputFileParser extends AbstractFileParser {

    @Override
    protected boolean validateFile(File file, boolean complete) {
        return !file.isDirectory() && (file.exists() || !complete);
    }

    @Override
    protected ParseException createParseException(ParseContext ctx, File file) {
        return new ParseException(ctx, "can't write to file \"" + file + "\"");
    }
}

