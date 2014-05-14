
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.reflect.TypeToken;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

public class FieldTypeItemType<T> implements ItemType<T> {

    private final FieldType<T> fieldType;

    public FieldTypeItemType(Session session, Class<T> type) {
        this(session, TypeToken.of(type));
    }

    public FieldTypeItemType(Session session, TypeToken<T> typeToken) {
        this(session.getDatabase().getFieldTypeRegistry().getFieldType(typeToken));
    }

    public FieldTypeItemType(FieldType<T> fieldType) {
        if (fieldType == null)
            throw new IllegalArgumentException("null fieldType");
        this.fieldType = fieldType;
    }

    @Override
    public TypeToken<T> getTypeToken() {
        return this.fieldType.getTypeToken();
    }

    @Override
    public void print(Session session, T value, boolean verbose) {
        final String string = this.fieldType.toString(value);
        if (!verbose) {
            session.getWriter().println(string);
            return;
        }
        final ByteWriter writer = new ByteWriter();
        this.fieldType.write(writer, value);
        final String encoding = ByteUtil.toString(writer.getBytes());
        session.getWriter().println("value " + string + " encoding " + encoding);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final FieldTypeItemType<?> that = (FieldTypeItemType<?>)obj;
        return this.fieldType.equals(that.fieldType);
    }

    @Override
    public int hashCode() {
        return this.fieldType.hashCode();
    }
}

