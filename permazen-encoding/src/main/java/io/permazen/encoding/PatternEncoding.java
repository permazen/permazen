
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;

import java.util.regex.Pattern;

/**
 * Non-null {@link Pattern} type. Null values are not supported by this class.
 *
 * <p>
 * <b>Note:</b> equality is defined by equal pattern strings, which is not consistent with
 * the method {@link Pattern#equals Pattern.equals()}, which is not implemented and therefore
 * defaults to object identity for comparision.
 */
public class PatternEncoding extends StringConvertedEncoding<Pattern> {

    private static final long serialVersionUID = -6406385779194286899L;

    public PatternEncoding(EncodingId encodingId) {
        super(encodingId, Pattern.class, Converter.from(Pattern::toString, Pattern::compile));
    }
}
