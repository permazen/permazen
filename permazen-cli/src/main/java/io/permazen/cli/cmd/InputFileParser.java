
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.parse.ParseException;
import io.permazen.util.ParseContext;

import java.io.File;

class InputFileParser extends AbstractFileParser {

    @Override
    protected boolean validateFile(File file, boolean complete) {
        return file.exists() && !file.isDirectory() && file.canRead();
    }

    @Override
    protected ParseException createParseException(ParseContext ctx, File file) {
        return new ParseException(ctx, "can't read file `" + file + "'");
    }
}

