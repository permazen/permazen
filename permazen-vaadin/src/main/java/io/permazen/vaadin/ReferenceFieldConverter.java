
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Preconditions;
import com.vaadin.data.util.converter.Converter;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.core.ObjId;

import java.util.Locale;

/**
 * Vaadin {@link Converter} for that converts between {@link ObjId}'s and {@link JObject}s.
 */
@SuppressWarnings("serial")
public class ReferenceFieldConverter implements Converter<ObjId, JObject> {

    private final JTransaction jtx;

    /**
     * Constructor.
     *
     * @param jtx transaction containing the {@link JObject}
     */
    public ReferenceFieldConverter(JTransaction jtx) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        this.jtx = jtx;
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
        return this.jtx.get(id);
    }
}
