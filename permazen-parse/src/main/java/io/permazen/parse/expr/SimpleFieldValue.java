
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.core.ObjId;
import io.permazen.core.SimpleField;
import io.permazen.parse.ParseSession;

/**
 * {@link Value} that reflects a core API {@link SimpleField} in some database object.
 */
public class SimpleFieldValue extends FieldValue implements LValue {

    /**
     * Constructor.
     *
     * @param id object ID
     * @param field database field
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code field} is null
     */
    public SimpleFieldValue(ObjId id, SimpleField<?> field) {
        super(id, field);
    }

    @Override
    public void set(ParseSession session, Value value) {
        final Object obj = value.get(session);
        try {
            session.getTransaction().writeSimpleField(this.id, this.field.getStorageId(), obj, false);
        } catch (IllegalArgumentException e) {
            throw new EvalException("invalid " + AbstractValue.describeType(obj) + " for " + this.field
              + (e.getMessage() != null ? ": " + e.getMessage() : ""), e);
        }
    }
}

