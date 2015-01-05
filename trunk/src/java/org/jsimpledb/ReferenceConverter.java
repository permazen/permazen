
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import org.jsimpledb.core.ObjId;

/**
 * Converts {@link ObjId}s into {@link JObject}s and vice-versa.
 */
class ReferenceConverter<T> extends Converter<T, ObjId> {

    private final JTransaction jtx;
    private final Class<T> type;

    ReferenceConverter(JTransaction jtx, Class<T> type) {
        if (jtx == null)
            throw new IllegalArgumentException("null jtx");
        if (type == null)
            throw new IllegalArgumentException("null type");
        this.jtx = jtx;
        this.type = type;
    }

    @Override
    protected ObjId doForward(T jobj) {
        if (jobj == null)
            return null;
        return ((JObject)jobj).getObjId();
    }

    @Override
    protected T doBackward(ObjId id) {
        if (id == null)
            return null;
        final JObject jobj = this.jtx.getJObject(id);
        return this.type.cast(jobj);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ReferenceConverter<?> that = (ReferenceConverter<?>)obj;
        return this.jtx == that.jtx;                                    // we don't include type in the comparison
    }

    @Override
    public int hashCode() {
        return this.jtx.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[type=" + this.type + ",jtx=" + this.jtx + "]";
    }
}

