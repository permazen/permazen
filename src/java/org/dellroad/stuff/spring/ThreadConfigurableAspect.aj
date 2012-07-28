
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id: ThreadConfigurableAspect.aj 282 2012-02-16 20:48:06Z archie.cobbs $
 */

package org.dellroad.stuff.spring;

import java.io.ObjectStreamException;
import java.io.Serializable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.aspectj.AbstractDependencyInjectionAspect;
import org.springframework.beans.factory.wiring.BeanConfigurerSupport;
import org.springframework.beans.factory.wiring.BeanWiringInfoResolver;

/**
 * Aspect that autowires classes marked with the {@link ThreadConfigurable @ThreadConfigurable} annotation using
 * the Spring application context returned by {@link ThreadConfigurableBeanFactory#get()}.
 *
 * <p>
 * This implementation is derived from Spring's {@code AnnotationBeanConfigurerAspect} implementation
 * and therefore shares its license (also Apache).
 * </p>
 *
 * @see ThreadConfigurableBeanFactory
 * @see ThreadConfigurable
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
        BeanFactory beanFactory = ThreadConfigurableBeanFactory.getInstance().get();
        if (beanFactory == null) {
            throw new IllegalStateException("can't configure @" + ThreadConfigurable.class.getName()
              + "-annotated bean of type " + bean.getClass().getName()
              + " because no BeanFactory has been configured for the current thread via "
              + ThreadConfigurableBeanFactory.class.getName() + ".set()");
        }
        return beanFactory;
    }

    @Override
    protected BeanWiringInfoResolver getBeanWiringInfoResolver(Object bean) {
        return new ThreadConfigurableBeanWiringInfoResolver();
    }
}

