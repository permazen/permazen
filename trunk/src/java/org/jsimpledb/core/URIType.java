
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.net.URI;
import java.net.URISyntaxException;

import org.jsimpledb.util.ParseContext;

/**
 * Non-null {@link URI} type. Null values are not supported by this class.
 *
 * <b>Note:</b> sort is not consistent with {@link URI#compareTo}.
 */
class URIType extends StringEncodedType<URI> {

    URIType() {
        super(URI.class);
    }

// FieldType

    @Override
    public URI fromString(ParseContext ctx) {
        final String uri = ctx.getInput();
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid URI `" + uri + "'", e);
        }
    }
}

