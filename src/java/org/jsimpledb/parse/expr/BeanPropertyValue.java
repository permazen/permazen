
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import java.beans.IndexedPropertyDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;

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
    protected final PropertyDescriptor propertyDescriptor;

    /**
     * Constructor.
     *
     * @param bean bean object
     * @param propertyDescriptor property descriptor
     * @throws IllegalArgumentException if {@code bean} is null
     * @throws IllegalArgumentException if {@code propertyDescriptor} is null, indexed, or has no read method
     */
    public BeanPropertyValue(Object bean, PropertyDescriptor propertyDescriptor) {
        if (bean == null)
            throw new IllegalArgumentException("null bean");
        if (propertyDescriptor == null)
            throw new IllegalArgumentException("null propertyDescriptor");
        if (propertyDescriptor instanceof IndexedPropertyDescriptor)
            throw new IllegalArgumentException("propertyDescriptor is indexed");
        if (propertyDescriptor.getReadMethod() == null)
            throw new IllegalArgumentException("unreadable property");
        this.bean = bean;
        this.propertyDescriptor = propertyDescriptor;
    }

    @Override
    public Object get(ParseSession session) {
        try {
            return this.propertyDescriptor.getReadMethod().invoke(this.bean);
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ?
              ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error reading property `" + this.propertyDescriptor.getName() + "' from object of type "
              + this.bean.getClass().getName() + ": " + t, t);
        }
    }
}

