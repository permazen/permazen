
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.io.File;

import org.dellroad.stuff.string.ParseContext;

/**
 * Non-null {@link File} type. Null values are not supported by this class.
 */
class FileType extends StringEncodedType<File> {

    FileType() {
        super(File.class);
    }

// FieldType

    @Override
    public File fromString(ParseContext ctx) {
        return new File(ctx.getInput());
    }
}

