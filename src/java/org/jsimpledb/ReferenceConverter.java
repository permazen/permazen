
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
class ReferenceConverter extends Converter<JObject, ObjId> {

    private final JSimpleDB jdb;

    ReferenceConverter(JSimpleDB jdb) {
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        this.jdb = jdb;
    }

    @Override
    protected ObjId doForward(JObject jobj) {
        if (jobj == null)
            return null;
        return jobj.getObjId();
    }

    @Override
    protected JObject doBackward(ObjId id) {
        if (id == null)
            return null;
        return this.jdb.getJObject(id);
    }
}

