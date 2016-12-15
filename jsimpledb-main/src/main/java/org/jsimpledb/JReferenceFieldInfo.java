
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import org.jsimpledb.core.ObjId;

class JReferenceFieldInfo extends JSimpleFieldInfo {

    JReferenceFieldInfo(JReferenceField jfield, int parentStorageId) {
        super(jfield, parentStorageId);
    }

    @Override
    public Converter<ObjId, JObject> getConverter(JTransaction jtx) {
        return new ReferenceConverter<>(jtx, JObject.class).reverse();
    }
}

