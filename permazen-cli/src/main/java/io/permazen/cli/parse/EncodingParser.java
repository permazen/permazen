
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

/**
 * Parses a value having type supported by an {@link Encoding}.
 */
public class EncodingParser<T> implements Parser<T> {

    private final Encoding<?> encoding;
    private final String typeName;

    /**
     * Constructor.
     *
     * @param encoding type to parse
     */
    public EncodingParser(Encoding<?> encoding) {
        this(encoding, null);
        Preconditions.checkArgument(encoding != null, "null encoding");
    }

    private EncodingParser(Encoding<?> encoding, String typeName) {
        this.encoding = encoding;
        this.typeName = typeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parse(Session session, ParseContext ctx, boolean complete) {

        // Get Encoding, if we don't already have it
        Encoding<?> actualEncoding = this.encoding;
        if (actualEncoding == null) {
            final EncodingId encodingId = session.getDatabase().getEncodingRegistry().idForAlias(this.typeName);
            if ((actualEncoding = session.getDatabase().getEncodingRegistry().getEncoding(encodingId)) == null)
                throw new ParseException(ctx, "no known encoding \"" + this.typeName + "\" registered with database");
        }

        // Parse value
        try {
            return (T)actualEncoding.fromParseableString(ctx);
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid parameter (" + actualEncoding + ")");
        }
    }

    /**
     * Create an instance based on type name.
     * Resolution of {@code typeName} is deferred until parse time when the database is available.
     *
     * @param typeName the name of an {@link Encoding}
     * @return parser for the named type
     */
    public static EncodingParser<?> getEncodingParser(String typeName) {
        Preconditions.checkArgument(typeName != null, "null typeName");
        return new EncodingParser<>(null, typeName);
    }
}
