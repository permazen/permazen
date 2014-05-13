
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.reflect.TypeToken;

import java.io.IOException;

import org.jsimpledb.core.FieldType;

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
    public void print(Session session, T value) throws IOException {
        session.getWriter().println(this.fieldType.toString(value));
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

