
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Field;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.parse.ParseSession;

/**
 * {@link Value} that reflects a core API {@link org.jsimpledb.core.Field} in some database object.
 *
 * @see JFieldValue
 */
public class FieldValue extends AbstractValue {

    protected final ObjId id;
    protected final org.jsimpledb.core.Field<?> field;

    /**
     * Constructor.
     *
     * @param id object ID
     * @param field database field
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code field} is null
     */
    public FieldValue(ObjId id, org.jsimpledb.core.Field<?> field) {
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (field == null)
            throw new IllegalArgumentException("null field");
        this.id = id;
        this.field = field;
    }

    @Override
    public Object get(ParseSession session) {
        try {
            return this.field.getValue(session.getTransaction(), this.id);
        } catch (Exception e) {
            throw new EvalException("error reading field `" + this.field.getName() + "' from object " + this.id + ": "
              + (e.getMessage() != null ? e.getMessage() : e));
        }
    }
}

