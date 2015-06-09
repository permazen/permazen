
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Value} that reflects a mutable bean property in some object.
 */
public class MutableBeanPropertyValue extends BeanPropertyValue implements LValue {

    protected final Method setter;

    /**
     * Constructor.
     *
     * @param bean bean object
     * @param name property name
     * @param getter getter method
     * @param setter setter method
     * @throws IllegalArgumentException if any parameter is null
     */
    public MutableBeanPropertyValue(Object bean, String name, Method getter, Method setter) {
        super(bean, name, getter);
        Preconditions.checkArgument(setter != null, "null setter");
        this.setter = setter;
    }

    @Override
    public void set(ParseSession session, Value value) {
        final Object obj = value.get(session);
        try {
            this.setter.invoke(this.bean, obj);
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ?
              ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error writing property `" + this.name + "' from object of type "
              + this.bean.getClass().getName() + ": " + t, t);
        }
    }
}

