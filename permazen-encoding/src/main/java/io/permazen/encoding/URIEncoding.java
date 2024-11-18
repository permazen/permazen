
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * {@link URI} type.
 *
 * <p>
 * Null values are supported by this class.
 *
 * <p>
 * <b>Note:</b> sort order is not consistent with {@link URI#compareTo URI.compareTo()}.
 */
public class URIEncoding extends StringConvertedEncoding<URI> {

    private static final long serialVersionUID = -7746505152033541526L;

    public URIEncoding(EncodingId encodingId) {
        super(encodingId, URI.class,
          Converter.from(URI::toString, string -> {
            try {
                return new URI(string);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(String.format("invalid URI \"%s\"", string), e);
            }
        }));
    }
}
