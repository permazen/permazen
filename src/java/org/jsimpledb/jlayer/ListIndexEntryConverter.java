
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import com.google.common.base.Converter;

class ListIndexEntryConverter<T> extends Converter<ListIndexEntry<T>, org.jsimpledb.ListIndexEntry> {

    private final ReferenceConverter referenceConverter;

    ListIndexEntryConverter(ReferenceConverter referenceConverter) {
        this.referenceConverter = referenceConverter;
    }

    @Override
    protected org.jsimpledb.ListIndexEntry doForward(ListIndexEntry<T> entry) {
        if (entry == null)
            return null;
        return new org.jsimpledb.ListIndexEntry(
          this.referenceConverter.convert((JObject)entry.getObject()), entry.getIndex());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ListIndexEntry<T> doBackward(org.jsimpledb.ListIndexEntry entry) {
        if (entry == null)
            return null;
        return new ListIndexEntry<T>((T)this.referenceConverter.reverse().convert(entry.getObjId()), entry.getIndex());
    }
}

