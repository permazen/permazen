
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Converter;

import java.io.File;

/**
 * {@link File} type. Null values are supported by this class.
 */
class FileType extends StringEncodedType<File> {

    FileType() {
        super(File.class, 0, new Converter<File, String>() {

            @Override
            protected String doForward(File file) {
                if (file == null)
                    return null;
                return file.toString();
            }

            @Override
            protected File doBackward(String string) {
                if (string == null)
                    return null;
                return new File(string);
            }
        });
    }
}

