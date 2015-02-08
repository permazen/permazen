
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Value} that reflects a bean property in some object.
 *
 * <p>
 * For mutable bean properties, use {@link MutableBeanPropertyValue}, which is also an {@link LValue}.
 * </p>
 */
public class BeanPropertyValue extends AbstractValue {

    protected final Object bean;
    protected final String name;
    protected final Method getter;

    /**
     * Constructor.
     *
     * @param bean bean object
     * @param name property name
     * @param getter getter method
     * @throws IllegalArgumentException if any parameter is null
     */
    public BeanPropertyValue(Object bean, String name, Method getter) {
        if (bean == null)
            throw new IllegalArgumentException("null bean");
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (getter == null)
            throw new IllegalArgumentException("null getter");
        this.bean = bean;
        this.name = name;
        this.getter = getter;
    }

    @Override
    public Object get(ParseSession session) {
        try {
            return getter.invoke(this.bean);
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ?
              ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error reading property `" + this.name + "' from object of type "
              + this.bean.getClass().getName() + ": " + t, t);
        }
    }
}

