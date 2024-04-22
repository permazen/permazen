
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import java.io.File;

class InputFileParser extends AbstractFileParser {

    @Override
    protected boolean validateFile(File file) {
        return file.exists() && !file.isDirectory() && file.canRead();
    }

    @Override
    protected IllegalArgumentException createParseException(File file) {
        return new IllegalArgumentException(String.format("can't read file \"%s\"", file));
    }
}
