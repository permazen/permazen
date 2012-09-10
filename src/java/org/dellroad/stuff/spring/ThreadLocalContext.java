
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Associates a {@link ConfigurableApplicationContext} with each thread. Child threads inherit their parents' value.
 *
 * <p>
 * Typically used as a singleton to determine the application context for autowiring
 * {@link ThreadConfigurable @ThreadConfigurable} and {@link ThreadTransactional @ThreadTransactional} beans.
 * </p>
 *
 * <p>
 * If an instance of this class is included in an application context, then that context will become the default value
 * (by virtue of this class being {@link ApplicationContextAware}). That is, that application context will be returned by
 * {@link #get ThreadLocalContext.get()} until another application context is explicitly set.
 * Doing this for the singleton instance makes the including application context the default for all
 * {@link ThreadConfigurable @ThreadConfigurable}-annotated beans.
 * </p>
 *
 * <p>
 * For any instance, attempting to configure the default application context more than once (prior to
 * {@linkplain #destroy bean disposal}) will generate an exception.
 * </p>
 *
 * @see ThreadConfigurable
 * @see ThreadTransactional
 */
public class ThreadLocalContext extends InheritableThreadLocal<ConfigurableApplicationContext>
  implements ApplicationContextAware, DisposableBean {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadLocalContext.class);

    private static ThreadLocalContext instance = new ThreadLocalContext();

    private ConfigurableApplicationContext defaultContext;

    /**
     * Get the singleton instance.
     */
    public static ThreadLocalContext getInstance() {
        return ThreadLocalContext.instance;
    }

    /**
     * Change the singleton {@link ThreadLocalContext} instance.
     *
     * <p>
     * This method is normally not needed. However, by changing the behavior of the singleton instance,
     * any arbitrary criteria can be used to determine which application context is used to configure
     * {@link ThreadConfigurable @ThreadConfigurable}-annotated beans.
     * </p>
     */
    public static void setInstance(ThreadLocalContext alternate) {
        if (LOG.isDebugEnabled())
            LOG.debug("replacing singleton " + ThreadLocalContext.instance + " with " + alternate);
        ThreadLocalContext.instance = alternate;
    }

    /**
     * Overridden to add trace logging.
     */
    @Override
    public ConfigurableApplicationContext get() {
        ConfigurableApplicationContext context = super.get();
        if (LOG.isTraceEnabled())
            LOG.trace(this + ": returning context " + context);
        return context;
    }

    /**
     * Overridden to add trace logging.
     */
    @Override
    public void set(ConfigurableApplicationContext context) {
        if (LOG.isTraceEnabled())
            LOG.trace(this + ": setting new value " + context);
        super.set(context);
    }

    /**
     * Change the application context associated with the current thread, but only for the duration of the given action.
     * When the action completes, whatever application context was previously associated with the current
     * thread is restored.
     *
     * @param context context to associate with the current thread for the duration of the {@code action}
     * @param action action to be performed while associated with {@code context}
     */
    public void invokeWith(ConfigurableApplicationContext context, Runnable action) {
        ConfigurableApplicationContext previous = this.get();
        if (LOG.isTraceEnabled())
            LOG.trace(this + ": temporarily changing from " + previous + " to " + context + " during " + action);
        try {
            this.set(context);
            action.run();
        } finally {
            if (LOG.isTraceEnabled())
                LOG.trace(this + ": restoring context back to " + previous + " after " + action);
            this.set(previous);
        }
    }

    /**
     * Configure the default application context. Automatically invoked when this instance
     * is included in a application context.
     *
     * @param context default application context, or null to have no default
     * @throws IllegalStateException if the default application context is already configured
     * @throws IllegalArgumentException if {@code context} is not a {@link ConfigurableApplicationContext}
     */
    @Override
    public void setApplicationContext(ApplicationContext context) {
        if (this.defaultContext != null) {
            throw new IllegalStateException("cannot configure default context " + context
              + ": another default context is already configured: " + this.defaultContext);
        }
        if (!(context instanceof ConfigurableApplicationContext)) {
            throw new IllegalArgumentException("cannot configure default context " + context
              + ": it must be an instance of " + ConfigurableApplicationContext.class.getName());
        }
        if (LOG.isDebugEnabled())
            LOG.debug(this + ": setting default context to " + context);
        this.defaultContext = (ConfigurableApplicationContext)context;
    }

    /**
     * Removes the default application context. Automatically invoked when an instance of this
     * class is included in a application context that is shut down.
     */
    @Override
    public void destroy() {
        if (LOG.isDebugEnabled())
            LOG.debug(this + ": clearing default context (destroy() invoked)");
        this.defaultContext = null;
    }

    /**
     * Returns the default application context configured by {@link #setApplicationContext setApplicationContext()}, if any.
     */
    @Override
    protected ConfigurableApplicationContext initialValue() {
        if (LOG.isDebugEnabled())
            LOG.debug(this + ": using default context " + this.defaultContext);
        return this.defaultContext;
    }
}

