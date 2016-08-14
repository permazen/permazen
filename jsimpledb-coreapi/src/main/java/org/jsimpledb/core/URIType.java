
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Converter;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * {@link URI} type. Null values are supported by this class.
 *
 * <b>Note:</b> sort is not consistent with {@link URI#compareTo}.
 */
class URIType extends StringEncodedType<URI> {

    private static final long serialVersionUID = -7746505152033541526L;

    URIType() {
        super(URI.class, 0, new URIConverter());
    }

// URIConverter

    private static class URIConverter extends Converter<URI, String> implements Serializable {

        private static final long serialVersionUID = 5035968898458406721L;

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
    }
}

