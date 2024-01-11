
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Preconditions;
import com.vaadin.data.util.converter.Converter;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.core.ObjId;

import java.util.Locale;

/**
 * Vaadin {@link Converter} for that converts between {@link ObjId}'s and {@link PermazenObject}s.
 */
@SuppressWarnings("serial")
public class ReferenceFieldConverter implements Converter<ObjId, PermazenObject> {

    private final PermazenTransaction jtx;

    /**
     * Constructor.
     *
     * @param jtx transaction containing the {@link PermazenObject}
     */
    public ReferenceFieldConverter(PermazenTransaction jtx) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        this.jtx = jtx;
    }

    @Override
    public Class<ObjId> getPresentationType() {
        return ObjId.class;
    }

    @Override
    public Class<PermazenObject> getModelType() {
        return PermazenObject.class;
    }

    @Override
    public ObjId convertToPresentation(PermazenObject jobj, Class<? extends ObjId> targetType, Locale locale) {
        if (jobj == null)
            return null;
        return jobj.getObjId();
    }

    @Override
    public PermazenObject convertToModel(ObjId id, Class<? extends PermazenObject> targetType, Locale locale) {
        if (id == null)
            return null;
        return this.jtx.get(id);
    }
}
