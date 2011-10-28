
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

/**
 * Spring singleton factory bean that produces the currently running {@link ContextApplication} Vaadin application instance.
 *
 * <p>
 * This bean will only work if there is a current {@link ContextApplication} running at the time this class
 * is instantiated (see {@link ContextApplication#get} for details). Therefore, it is most appropriate for use
 * within a {@link SpringContextApplication}'s appplication context.
 * </p>
 *
 * @see ContextApplication#get
 * @see SpringContextApplication
 */
public class ContextApplicationFactoryBean extends AbstractFactoryBean {

    private ContextApplication application;

    private boolean autowire;

    /**
     * Should this instance autowire the application instance be autowired using the containing bean factory?
     * Requires the containing bean factory to be an {@link AutowireCapableBeanFactory}.
     * Default is false.
     */
    public boolean isAutowire() {
        return this.autowire;
    }
    public void setAutowire(boolean autowire) {
        this.autowire = autowire;
    }

    @Override
    public Class<? extends ContextApplication> getObjectType() {
        return this.getContextApplication().getClass();
    }

    @Override
    protected ContextApplication createInstance() {
        return this.getContextApplication();
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        // Invoke superclass
        super.afterPropertiesSet();

        // Should we autowire?
        if (!this.autowire)
            return;

        // Yes, autowire
        BeanFactory beanFactory = this.getBeanFactory();
        AutowireCapableBeanFactory autowireFactory;
        try {
            autowireFactory = (AutowireCapableBeanFactory)beanFactory;
        } catch (ClassCastException e) {
            throw new BeanInitializationException("containing bean factory is not autowire-capable");
        }
        autowireFactory.autowireBean(this.getContextApplication());
    }

    private ContextApplication getContextApplication() {
        if (this.application == null)
            this.application = ContextApplication.get();
        return this.application;
    }
}

