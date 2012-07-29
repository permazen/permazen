
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id: VaadinConfigurableAspect.aj 282 2012-02-16 20:48:06Z archie.cobbs $
 */

package org.dellroad.stuff.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.aspectj.AbstractDependencyInjectionAspect;
import org.springframework.beans.factory.wiring.BeanConfigurerSupport;
import org.springframework.beans.factory.wiring.BeanWiringInfoResolver;

/**
 * Abstract support super-aspect for aspects that autowire beans using a {@link BeanFactory}
 * (which is determined by the sub-aspect).
 */
public abstract aspect AbstractConfigurableAspect extends AbstractDependencyInjectionAspect {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Get the {@link BeanFactory} to use when autowiring beans.
     */
    protected abstract BeanFactory getBeanFactory(Object bean);

    /**
     * Get the {@link BeanWiringInfoResolver} to use when autowiring beans.
     */
    protected abstract BeanWiringInfoResolver getBeanWiringInfoResolver(Object bean);

    /**
     * Configure the given bean using the {@link BeanFactory} returned from {@link #getBeanFactory}.
     * If the latter is null, then this does nothing; sub-aspect should probably log something in that case.
     */
    @Override
    public void configureBean(Object bean) {
        final BeanFactory beanFactory = this.getBeanFactory(bean);
        if (beanFactory == null)
            return;
        BeanConfigurerSupport beanConfigurerSupport = new BeanConfigurerSupport();
        beanConfigurerSupport.setBeanFactory(beanFactory);
        beanConfigurerSupport.setBeanWiringInfoResolver(this.getBeanWiringInfoResolver(bean));
        beanConfigurerSupport.afterPropertiesSet();
        beanConfigurerSupport.configureBean(bean);
        beanConfigurerSupport.destroy();
    }
}

