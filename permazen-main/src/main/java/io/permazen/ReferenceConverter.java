
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;

import io.permazen.core.ObjId;

/**
 * Converts {@link ObjId}s into {@link PermazenObject}s and vice-versa.
 */
class ReferenceConverter<T> extends Converter<T, ObjId> {

    private final PermazenTransaction ptx;
    private final Class<T> type;

    ReferenceConverter(PermazenTransaction ptx, Class<T> type) {
        assert ptx != null;
        assert type != null;
        this.ptx = ptx;
        this.type = type;
    }

    @Override
    protected ObjId doForward(T pobj) {
        if (pobj == null)
            return null;
        return ((PermazenObject)pobj).getObjId();
    }

    @Override
    protected T doBackward(ObjId id) {
        if (id == null)
            return null;
        final PermazenObject pobj = this.ptx.get(id);
        return this.type.cast(pobj);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ReferenceConverter<?> that = (ReferenceConverter<?>)obj;
        return this.ptx == that.ptx;                                    // we don't include type in the comparison
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.ptx.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[type=" + this.type + ",ptx=" + this.ptx + "]";
    }
}
