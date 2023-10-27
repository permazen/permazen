
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.core.EncodingId;
import io.permazen.core.FieldType;
import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

/**
 * Parses a value having type supported by a {@link FieldType}.
 */
public class FieldTypeParser<T> implements Parser<T> {

    private final FieldType<?> fieldType;
    private final String typeName;

    /**
     * Constructor.
     *
     * @param fieldType type to parse
     */
    public FieldTypeParser(FieldType<?> fieldType) {
        this(fieldType, null);
        Preconditions.checkArgument(fieldType != null, "null fieldType");
    }

    private FieldTypeParser(FieldType<?> fieldType, String typeName) {
        this.fieldType = fieldType;
        this.typeName = typeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parse(Session session, ParseContext ctx, boolean complete) {

        // Get FieldType, if we don't already have it
        FieldType<?> actualFieldType = this.fieldType;
        if (actualFieldType == null) {
            final EncodingId encodingId = session.getDatabase().getFieldTypeRegistry().idForAlias(this.typeName);
            if ((actualFieldType = session.getDatabase().getFieldTypeRegistry().getFieldType(encodingId)) == null)
                throw new ParseException(ctx, "no known field type \"" + this.typeName + "\" registered with database");
        }

        // Parse value
        try {
            return (T)actualFieldType.fromParseableString(ctx);
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid parameter (" + actualFieldType + ")");
        }
    }

    /**
     * Create an instance based on type name.
     * Resolution of {@code typeName} is deferred until parse time when the database is available.
     *
     * @param typeName the name of a {@link FieldType}
     * @return parser for the named type
     */
    public static FieldTypeParser<?> getFieldTypeParser(String typeName) {
        Preconditions.checkArgument(typeName != null, "null typeName");
        return new FieldTypeParser<>(null, typeName);
    }
}
