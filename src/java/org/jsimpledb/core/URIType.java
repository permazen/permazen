
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Converter;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * {@link URI} type. Null values are supported by this class.
 *
 * <b>Note:</b> sort is not consistent with {@link URI#compareTo}.
 */
class URIType extends StringEncodedType<URI> {

    URIType() {
        super(URI.class, new Converter<URI, String>() {

            @Override
            protected String doForward(URI uri) {
                if (uri == null)
                    return null;
                return uri.toString();
            }

            @Override
            protected URI doBackward(String string) {
                if (string == null)
                    return null;
                try {
                    return new URI(string);
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("invalid URI `" + string + "'", e);
                }
            }
        });
    }
}

