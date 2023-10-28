
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Converter;

import io.permazen.core.EncodingIds;

import java.io.File;
import java.io.Serializable;

/**
 * Non-null {@link File} type. Null values are not supported by this class.
 */
public class FileEncoding extends StringConvertedEncoding<File> {

    private static final long serialVersionUID = -8784371602920299513L;

    public FileEncoding() {
        super(EncodingIds.builtin("File"), File.class, new FileConverter());
    }

// FileConverter

    private static class FileConverter extends Converter<File, String> implements Serializable {

        private static final long serialVersionUID = 6790052264207886810L;

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
    }
}
