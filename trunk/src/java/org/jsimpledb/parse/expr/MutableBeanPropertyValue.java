
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

import org.jsimpledb.parse.ParseSession;

/**
 * {@link Value} that reflects a mutable bean property in some object.
 */
public class MutableBeanPropertyValue extends BeanPropertyValue implements LValue {

    /**
     * Constructor.
     *
     * @param bean bean object
     * @param propertyDescriptor property descriptor
     * @throws IllegalArgumentException if {@code bean} is null
     * @throws IllegalArgumentException if {@code propertyDescriptor} is null, indexed, has no read method, or has no write method
     */
    public MutableBeanPropertyValue(Object bean, PropertyDescriptor propertyDescriptor) {
        super(bean, propertyDescriptor);
        if (propertyDescriptor.getWriteMethod() == null)
            throw new IllegalArgumentException("unwritable property");
    }

    @Override
    public void set(ParseSession session, Value value) {
        final Object obj = value.get(session);
        try {
            this.propertyDescriptor.getWriteMethod().invoke(this.bean, obj);
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ?
              ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error writing property `" + this.propertyDescriptor.getName() + "' from object of type "
              + this.bean.getClass().getName() + ": " + t, t);
        }
    }
}

