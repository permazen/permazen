
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import org.jsimpledb.core.ObjId;

class JReferenceFieldInfo extends JSimpleFieldInfo {

    JReferenceFieldInfo(JReferenceField jfield, JComplexFieldInfo parent) {
        super(jfield, parent);
    }

    @Override
    public Converter<ObjId, JObject> getConverter(JTransaction jtx) {
        return jtx.referenceConverter.reverse();
    }
}

