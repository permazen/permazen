
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.springframework.beans.factory.BeanFactory;

/**
 * Associates a {@link BeanFactory} with each thread, to be used for autowiring @{@link ThreadConfigurable}-annotated beans.
 *
 * @see ThreadConfigurable
 */
public class ThreadConfigurableBeanFactory extends InheritableThreadLocal<BeanFactory> {

    private static ThreadConfigurableBeanFactory instance = new ThreadConfigurableBeanFactory();

    /**
     * Subclass constructor. Intended to be a singleton, this class is not directly instantiable.
     */
    protected ThreadConfigurableBeanFactory() {
    }

    /**
     * Get the singleton instance.
     */
    public static ThreadConfigurableBeanFactory getInstance() {
        return ThreadConfigurableBeanFactory.instance;
    }

    /**
     * Change the singleton {@link ThreadConfigurableBeanFactory} instance.
     *
     * <p>
     * This method is not needed unless the thread local initial value or child thread inheritance behavior
     * needs to be changed from the default.
     */
    public static void setInstance(ThreadConfigurableBeanFactory alternate) {
        ThreadConfigurableBeanFactory.instance = alternate;
    }
}

