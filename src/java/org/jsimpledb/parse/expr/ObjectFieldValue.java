
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Value} that reflects a instance Java field in some object.
 */
public class ObjectFieldValue extends AbstractFieldValue {

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
    public ObjectFieldValue(Object object, Field field) {
        super(field);
        if (object == null)
            throw new IllegalArgumentException("null object");
        if ((field.getModifiers() & Modifier.STATIC) != 0)
            throw new IllegalArgumentException("field is static");
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

