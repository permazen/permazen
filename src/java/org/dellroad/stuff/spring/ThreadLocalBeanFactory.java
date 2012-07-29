
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;

/**
 * Associates a {@link BeanFactory} with each thread. Typically used for autowiring
 * {@link ThreadConfigurable @ThreadConfigurable}-annotated beans.
 *
 * <p>
 * Also provides a default singleton instance (which may be substituted if necessary).
 * </p>
 *
 * <p>
 * If an instance of this class is included in a {@link BeanFactory}, then that {@link BeanFactory} will become the default
 * value for all threads. That is, that {@link BeanFactory} will be returned by {@link #get ThreadLocalBeanFactory.get()} unless
 * another {@link BeanFactory} has been explicitly configured for the current thread. Attempts to configure the default
 * {@link BeanFactory} more than once (prior to {@linkplain #destroy bean disposal}) will generate an exception.
 * </p>
 *
 * @see ThreadConfigurable
 */
public class ThreadLocalBeanFactory extends InheritableThreadLocal<BeanFactory> implements BeanFactoryAware, DisposableBean {

    private static ThreadLocalBeanFactory instance = new ThreadLocalBeanFactory();

    private BeanFactory defaultBeanFactory;

    /**
     * Get the singleton instance.
     */
    public static ThreadLocalBeanFactory getInstance() {
        return ThreadLocalBeanFactory.instance;
    }

    /**
     * Change the singleton {@link ThreadLocalBeanFactory} instance.
     *
     * <p>
     * This method is not needed unless the thread local initial value or child thread inheritance behavior
     * needs to be changed from the default.
     * </p>
     */
    public static void setInstance(ThreadLocalBeanFactory alternate) {
        ThreadLocalBeanFactory.instance = alternate;
    }

    /**
     * Configure the default {@link BeanFactory}. Automatically invoked when an instance of this
     * class is included in a {@link BeanFactory}.
     *
     * @throws IllegalStateException if the default {@link BeanFactory} is already configured
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        if (this.defaultBeanFactory != null) {
            throw new IllegalStateException("cannot configure default bean factory " + beanFactory
              + ": another BeanFactory is already configured: " + this.defaultBeanFactory);
        }
        this.defaultBeanFactory = beanFactory;
    }

    /**
     * Removes the default {@link BeanFactory}. Automatically invoked when an instance of this
     * class is included in a {@link BeanFactory} that is shut down.
     */
    @Override
    public void destroy() {
        this.defaultBeanFactory = null;
    }

    /**
     * Returns the default {@link BeanFactory} configured by {@link #setBeanFactory setBeanFactory()}, if any.
     */
    @Override
    protected BeanFactory initialValue() {
        return this.defaultBeanFactory;
    }
}

