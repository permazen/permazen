
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;

import io.permazen.parse.ParseSession;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * {@link Value} that reflects a bean property in some object.
 *
 * <p>
 * For mutable bean properties, use {@link MutableBeanPropertyValue}, which is also an {@link LValue}.
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
        Preconditions.checkArgument(bean != null, "null bean");
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(getter != null, "null getter");
        this.bean = bean;
        this.name = name;
        this.getter = getter;
    }

    @Override
    public Object get(ParseSession session) {
        try {
            return MethodUtil.invokeRefreshed(getter, this.bean);
        } catch (Exception e) {
            final Throwable t = e instanceof InvocationTargetException ?
              ((InvocationTargetException)e).getTargetException() : e;
            throw new EvalException("error reading property `" + this.name + "' from object of type "
              + this.bean.getClass().getName() + ": " + t, t);
        }
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return this.getter.getReturnType();
    }
}

