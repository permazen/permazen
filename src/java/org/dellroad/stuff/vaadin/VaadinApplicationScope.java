
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import java.util.HashMap;

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;

/**
 * A Spring custom {@link Scope} for Vaadin applications.
 *
 * <p>
 * This works for applications that subclass {@link ContextApplication}; objects will be scoped to each
 * {@link ContextApplication} instance. Spring {@linkplain org.springframework.beans.factory.DisposableBean#destroy destroy-methods}
 * will be invoked when the {@link ContextApplication} is closed.
 * </p>
 *
 * <p>
 * To enable this scope, simply add this bean to your application context as a singleton (it will register itself):
 * <blockquote><pre>
 *  &lt;!-- Enable the "vaadinApplication" custom scope --&gt;
 *  &lt;bean class="org.dellroad.stuff.vaadin.VaadinApplicationScope"/&gt;
 * </pre></blockquote>
 * Then declare scoped beans normally using the scope name {@code "vaadinApplication"}.
 * </p>
 */
public class VaadinApplicationScope implements Scope, BeanFactoryPostProcessor, ContextApplication.CloseListener {

    /**
     * Key to the current application instance. For use by {@link #resolveContextualObject}.
     */
    public static final String APPLICATION_KEY = "application";

    /**
     * The name of this scope (i.e., <code>"{@value}"</code>).
     */
    public static final String SCOPE_NAME = "vaadinApplication";

    private final HashMap<ContextApplication, ApplicationBeanHolder> beanHolders
      = new HashMap<ContextApplication, ApplicationBeanHolder>();

// BeanFactoryPostProcessor methods

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        beanFactory.registerScope(VaadinApplicationScope.SCOPE_NAME, this);
    }

// ContextApplication.CloseListener methods

    @Override
    public void applicationClosed(ContextApplication.CloseEvent closeEvent) {
        ApplicationBeanHolder beanHolder;
        synchronized (this) {
            beanHolder = this.beanHolders.remove(closeEvent.getContextApplication());
        }
        if (beanHolder != null)
            beanHolder.close();
    }

// Scope methods

    @Override
    public synchronized Object get(String name, ObjectFactory<?> objectFactory) {
        return this.getApplicationBeanHolder(true).getBean(name, objectFactory);
    }

    @Override
    public synchronized Object remove(String name) {
        ApplicationBeanHolder beanHolder = this.getApplicationBeanHolder(false);
        return beanHolder != null ? beanHolder.remove(name) : null;
    }

    @Override
    public synchronized void registerDestructionCallback(String name, Runnable callback) {
        this.getApplicationBeanHolder(true).registerDestructionCallback(name, callback);
    }

    @Override
    public String getConversationId() {
        ContextApplication application = ContextApplication.currentApplication();
        if (application == null)
            return null;
        return application.getClass().getName() + "@" + System.identityHashCode(application);
    }

    @Override
    public Object resolveContextualObject(String key) {
        if (APPLICATION_KEY.equals(key))
            return ContextApplication.currentApplication();
        return null;
    }

// Internal methods

    private synchronized ApplicationBeanHolder getApplicationBeanHolder(boolean create) {
        ContextApplication application = ContextApplication.get();
        application.addListener(this);
        ApplicationBeanHolder beanHolder = this.beanHolders.get(application);
        if (beanHolder == null && create) {
            beanHolder = new ApplicationBeanHolder(application);
            this.beanHolders.put(application, beanHolder);
        }
        return beanHolder;
    }

// Bean holder class corresponding to a single Application instance

    private static class ApplicationBeanHolder {

        private final HashMap<String, Object> beans = new HashMap<String, Object>();
        private final HashMap<String, Runnable> destructionCallbacks = new HashMap<String, Runnable>();
        private final ContextApplication application;

        public ApplicationBeanHolder(ContextApplication application) {
            this.application = application;
        }

        public Object getBean(String name, ObjectFactory<?> objectFactory) {
            Object bean = this.beans.get(name);
            if (bean == null) {
                bean = objectFactory.getObject();
                this.beans.put(name, bean);
            }
            return bean;
        }

        public Object remove(String name) {
            this.destructionCallbacks.remove(name);
            return this.beans.remove(name);
        }

        public void registerDestructionCallback(String name, Runnable callback) {
            this.destructionCallbacks.put(name, callback);
        }

        public void close() {
            for (Runnable callback : this.destructionCallbacks.values())
                callback.run();
            this.beans.clear();
            this.destructionCallbacks.clear();
        }

        public boolean isEmpty() {
            return this.beans.isEmpty();
        }
    }
}

