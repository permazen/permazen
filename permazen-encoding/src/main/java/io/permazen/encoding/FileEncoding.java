
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;

import java.io.File;

/**
 * {@link File} type.
 *
 * <p>
 * Null values are supported by this class.
 */
public class FileEncoding extends StringConvertedEncoding<File> {

    private static final long serialVersionUID = -8784371602920299513L;

    public FileEncoding(EncodingId encodingId) {
        super(encodingId, File.class, Converter.from(File::toString, File::new));
    }
}
