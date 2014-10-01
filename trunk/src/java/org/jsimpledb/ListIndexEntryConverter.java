
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

class ListIndexEntryConverter<T> extends Converter<ListIndexEntry<T>, org.jsimpledb.core.ListIndexEntry> {

    private final ReferenceConverter referenceConverter;

    ListIndexEntryConverter(ReferenceConverter referenceConverter) {
        if (referenceConverter == null)
            throw new IllegalArgumentException("null referenceConverter");
        this.referenceConverter = referenceConverter;
    }

    @Override
    protected org.jsimpledb.core.ListIndexEntry doForward(ListIndexEntry<T> entry) {
        if (entry == null)
            return null;
        return new org.jsimpledb.core.ListIndexEntry(
          this.referenceConverter.convert((JObject)entry.getObject()), entry.getIndex());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ListIndexEntry<T> doBackward(org.jsimpledb.core.ListIndexEntry entry) {
        if (entry == null)
            return null;
        return new ListIndexEntry<T>((T)this.referenceConverter.reverse().convert(entry.getObjId()), entry.getIndex());
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ListIndexEntryConverter<?> that = (ListIndexEntryConverter<?>)obj;
        return this.referenceConverter.equals(that.referenceConverter);
    }

    @Override
    public int hashCode() {
        return this.referenceConverter.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[referenceConverter=" + this.referenceConverter + "]";
    }
}

