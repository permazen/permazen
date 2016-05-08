
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.File;

import org.jsimpledb.parse.ParseException;
import org.jsimpledb.util.ParseContext;

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

