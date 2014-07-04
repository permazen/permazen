
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import org.jsimpledb.cli.Session;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.util.ParseContext;

/**
 * Parses a value having type supported by a {@link FieldType}.
 */
public class FieldTypeParser<T> implements Parser<T> {

    private final FieldType<?> fieldType;
    private final String typeName;

    /**
     * Constructor.
     */
    public FieldTypeParser(FieldType<?> fieldType) {
        this(fieldType, null);
        if (fieldType == null)
            throw new IllegalArgumentException("null fieldType");
    }

    private FieldTypeParser(FieldType<?> fieldType, String typeName) {
        if (fieldType == null)
            throw new IllegalArgumentException("null fieldType");
        this.fieldType = fieldType;
        this.typeName = typeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parse(Session session, ParseContext ctx, boolean complete) {

        // Get FieldType
        final FieldType<?> actualFieldType = this.fieldType != null ?
          this.fieldType : session.getDatabase().getFieldTypeRegistry().getFieldType(this.typeName);
        if (actualFieldType == null)
            throw new ParseException(ctx, "no known field type `" + this.typeName + "' registered with database");
        final int start = ctx.getIndex();
        try {
            return (T)actualFieldType.fromString(ctx);
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid " + actualFieldType.getName() + " parameter starting with `"
              + ParseUtil.truncate(ctx.getOriginalInput().substring(start), 16) + "'");
        }
    }

    /**
     * Create an instance based on type name.
     * Resolution of {@code typeName} is deferred until parse time when the database is available.
     */
    public static FieldTypeParser<?> getFieldTypeParser(String typeName) {
        if (typeName == null)
            throw new IllegalArgumentException("null typeName");
        return new FieldTypeParser<Object>(null, typeName);
    }
}

