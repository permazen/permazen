
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Value} that reflects a static Java field in some {@link Class}.
 */
public class StaticFieldValue extends AbstractFieldValue {

    /**
     * Constructor.
     *
     * @param field field
     * @throws IllegalArgumentException if {@code field} is null
     * @throws IllegalArgumentException if {@code field} is not static
     */
    public StaticFieldValue(Field field) {
        super(field);
        if ((field.getModifiers() & Modifier.STATIC) == 0)
            throw new IllegalArgumentException("field is not static");
    }

    @Override
    public Object get(ParseSession session) {
        try {
            return this.field.get(null);
        } catch (Exception e) {
            throw new EvalException("error reading static field `" + this.field.getName()
              + "' in class `" + this.field.getDeclaringClass().getName() + "': " + e, e);
        }
    }

    @Override
    public void set(ParseSession session, Value value) {
        final Object obj = value.get(session);
        try {
            this.field.set(null, obj);
        } catch (IllegalArgumentException e) {
            throw new EvalException("invalid " + AbstractValue.describeType(obj) + " for static field `"
              + this.field.getName() + "' in class `" + this.field.getDeclaringClass().getName() + "'", e);
        } catch (Exception e) {
            throw new EvalException("error writing static field `" + this.field.getName()
              + "' in class `" + this.field.getDeclaringClass().getName() + "': " + e, e);
        }
    }
}

