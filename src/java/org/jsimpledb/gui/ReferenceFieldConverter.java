
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui;

import com.vaadin.data.util.converter.Converter;

import java.util.Locale;

import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.core.ObjId;

/**
 * Vaadin {@link Converter} for that converts between {@link ObjId}'s and {@link JObject}s.
 */
@SuppressWarnings("serial")
public class ReferenceFieldConverter implements Converter<ObjId, JObject> {

    private final JSimpleDB jdb;

    public ReferenceFieldConverter(JSimpleDB jdb) {
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        this.jdb = jdb;
    }

    @Override
    public Class<ObjId> getPresentationType() {
        return ObjId.class;
    }

    @Override
    public Class<JObject> getModelType() {
        return JObject.class;
    }

    @Override
    public ObjId convertToPresentation(JObject jobj, Class<? extends ObjId> targetType, Locale locale) {
        if (jobj == null)
            return null;
        return jobj.getObjId();
    }

    @Override
    public JObject convertToModel(ObjId id, Class<? extends JObject> targetType, Locale locale) {
        if (id == null)
            return null;
        return this.jdb.getJObject(id);
    }
}

