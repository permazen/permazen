
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id: ThreadConfigurableAspect.aj 282 2012-02-16 20:48:06Z archie.cobbs $
 */

package org.dellroad.stuff.spring;

import java.io.ObjectStreamException;
import java.io.Serializable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.wiring.BeanWiringInfoResolver;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Aspect that autowires classes marked with the {@link ThreadConfigurable @ThreadConfigurable} annotation using
 * the per-thread Spring application context configured in the {@link ThreadLocalContext} singleton.
 *
 * <p>
 * This implementation is derived from Spring's {@code AnnotationBeanConfigurerAspect} implementation
 * and therefore shares its license (also Apache).
 * </p>
 *
 * @see ThreadConfigurable
 * @see ThreadLocalContext
 */
public aspect ThreadConfigurableAspect extends AbstractConfigurableAspect {

// Stuff copied from AbstractInterfaceDrivenDependencyInjectionAspect.aj

    public pointcut beanConstruction(Object bean) :
        initialization(ThreadConfigurableObject+.new(..)) && this(bean);

    public pointcut beanDeserialization(Object bean) :
        execution(Object ThreadConfigurableDeserializationSupport+.readResolve()) &&
        this(bean);

    public pointcut leastSpecificSuperTypeConstruction() : initialization(ThreadConfigurableObject.new(..));

    declare parents: 
        ThreadConfigurableObject+ && Serializable+ implements ThreadConfigurableDeserializationSupport;

    static interface ThreadConfigurableDeserializationSupport extends Serializable {
    }

    public Object ThreadConfigurableDeserializationSupport.readResolve() throws ObjectStreamException {
        return this;
    }

// Stuff copied from AnnotationBeanConfigurerAspect.aj

    public pointcut inConfigurableBean() : @this(ThreadConfigurable);

    public pointcut preConstructionConfiguration() : preConstructionConfigurationSupport(*);

    declare parents: @ThreadConfigurable * implements ThreadConfigurableObject;

    private pointcut preConstructionConfigurationSupport(ThreadConfigurable c) : @this(c) && if(c.preConstruction());

	declare parents: @ThreadConfigurable Serializable+ implements ThreadConfigurableDeserializationSupport;

// Our implementation

    @Override
    protected BeanFactory getBeanFactory(Object bean) {

        // Get ThreadLocalContext singleton
        ThreadLocalContext threadLocalContext = ThreadLocalContext.getInstance();
        if (threadLocalContext == null) {
            if (this.log.isDebugEnabled()) {
                this.log.debug("can't configure @" + ThreadConfigurable.class.getName()
                  + "-annotated bean of type " + bean.getClass().getName()
                  + " because the ThreadLocalContext singleton instance has been set to null;"
                  + " proceeding without injection");
            }
            return null;
        }

        // Get ConfigurableApplicationContext
        ConfigurableApplicationContext context = threadLocalContext.get();
        if (context == null) {
            if (this.log.isDebugEnabled()) {
                this.log.debug("can't configure @" + ThreadConfigurable.class.getName()
                  + "-annotated bean of type " + bean.getClass().getName()
                  + " because no ConfigurableApplicationContext has been configured for the current thread via "
                  + ThreadLocalContext.class.getName() + ".set() nor has a default been set;"
                  + " proceeding without injection");
            }
            return null;
        }

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("using application context " + context + " to configure @ThreadConfigurable bean " + bean);

        // Return associated BeanFactory
        return context.getBeanFactory();
    }

    @Override
    protected BeanWiringInfoResolver getBeanWiringInfoResolver(Object bean) {
        return new ThreadConfigurableBeanWiringInfoResolver();
    }
}

