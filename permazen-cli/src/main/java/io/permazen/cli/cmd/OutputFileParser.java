
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.io.File;

class OutputFileParser extends AbstractFileParser {

    @Override
    protected boolean validateFile(File file) {
        return !file.isDirectory() && file.exists();
    }

    @Override
    protected IllegalArgumentException createParseException(File file) {
        return new IllegalArgumentException(String.format("can't write to file \"%s\"", file));
    }
}
