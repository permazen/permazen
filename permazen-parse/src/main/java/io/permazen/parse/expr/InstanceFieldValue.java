
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;

import io.permazen.parse.ParseSession;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * {@link Value} that reflects a non-static field in some Java object.
 */
public class InstanceFieldValue extends AbstractFieldValue {

    protected final Object object;

    /**
     * Constructor.
     *
     * @param object object containing field
     * @param field field to access
     * @throws IllegalArgumentException if {@code object} is null
     * @throws IllegalArgumentException if {@code field} is null
     * @throws IllegalArgumentException if {@code field} is static
     */
    public InstanceFieldValue(Object object, Field field) {
        super(field);
        Preconditions.checkArgument(object != null, "null object");
        Preconditions.checkArgument((field.getModifiers() & Modifier.STATIC) == 0, "field is static");
        this.object = object;
    }

    @Override
    public Object get(ParseSession session) {
        try {
            return this.field.get(this.object);
        } catch (Exception e) {
            throw new EvalException("error reading field `" + this.field.getName() + "' in object of type "
              + this.object.getClass().getName() + ": " + e, e);
        }
    }

    @Override
    public void set(ParseSession session, Value value) {
        final Object obj = value.get(session);
        try {
            this.field.set(this.object, obj);
        } catch (Exception e) {
            throw new EvalException("error writing field `" + this.field.getName() + "' in object of type "
              + this.object.getClass().getName() + ": " + e, e);
        }
    }
}

