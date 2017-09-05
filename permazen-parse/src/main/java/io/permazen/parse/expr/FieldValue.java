
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;

import io.permazen.core.ObjId;
import io.permazen.parse.ParseSession;

import java.lang.reflect.Field;

/**
 * {@link Value} that reflects a core API {@link io.permazen.core.Field} in some database object.
 *
 * @see JFieldValue
 */
public class FieldValue extends AbstractValue {

    protected final ObjId id;
    protected final io.permazen.core.Field<?> field;

    /**
     * Constructor.
     *
     * @param id object ID
     * @param field database field
     * @throws IllegalArgumentException if {@code id} is null
     * @throws IllegalArgumentException if {@code field} is null
     */
    public FieldValue(ObjId id, io.permazen.core.Field<?> field) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(field != null, "null field");
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

    @Override
    public Class<?> getType(ParseSession session) {
        return this.field.getTypeToken().getRawType();
    }
}

