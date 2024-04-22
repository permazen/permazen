
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.cli.parse.Parser;

import java.io.File;

abstract class AbstractFileParser implements Parser<File> {

    @Override
    public File parse(Session session, String text) {

        // Sanity check
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(text != null, "null text");

        // Check file for validity
        final File file = new File(text);
        if (this.validateFile(file))
            return file;

        // Throw parse exception
        throw this.createParseException(file);
    }

    protected abstract boolean validateFile(File file);

    protected abstract IllegalArgumentException createParseException(File file);
}
