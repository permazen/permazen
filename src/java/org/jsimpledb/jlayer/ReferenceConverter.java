
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import com.google.common.base.Converter;

import org.jsimpledb.ObjId;

/**
 * Converts {@link ObjId}s into {@link JObject}s and vice-versa.
 */
class ReferenceConverter extends Converter<JObject, ObjId> {

    private final JLayer jlayer;

    ReferenceConverter(JLayer jlayer) {
        if (jlayer == null)
            throw new IllegalArgumentException("null jlayer");
        this.jlayer = jlayer;
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
        return this.jlayer.getJObject(id);
    }
}

