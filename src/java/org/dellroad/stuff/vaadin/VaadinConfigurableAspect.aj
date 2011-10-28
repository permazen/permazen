
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id: SpringContextApplication.java 160 2011-10-24 18:00:49Z archie.cobbs $
 */

package org.dellroad.stuff.vaadin;

import java.io.ObjectStreamException;
import java.io.Serializable;

import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.aspectj.AbstractDependencyInjectionAspect;
import org.springframework.beans.factory.aspectj.ConfigurableObject;

/**
 * Aspect that autowires classes marked with the Spring's {@link Configurable @Configurable} annotation to a
 * {@link SpringContextApplication} application context.
 *
 * <p>
 * This aspect does the same thing that Spring's {@code AnnotationBeanConfigurerAspect} aspect does,
 * except that this aspect configures beans using the application context associated with the current
 * {@link SpringContextApplication} Vaadin application rather than the one associated with the overall
 * servlet context (i.e., its parent).
 * </p>
 *
 * <p>
 * As a result, objects so annotated will be configured for their specific {@link SpringContextApplication}
 * instance, and therefore can only be instantiated by threads associated with a current {@link SpringContextApplication}
 * (see {@link ContextApplication#get}). This will be the case when executing within a {@link SpringContextApplication}
 * Vaadin application HTTP request or an invocation of {@link ContextApplication#invoke} on same.
 * </p>
 *
 * <p>
 * This implementation is derived from Spring's {@code AnnotationBeanConfigurerAspect} implementation
 * and therefore shares its license (also Apache).
 * </p>
 *
 * @see SpringContextApplication
 * @see ContextApplication#get
 * @see Configurable
 */
public aspect VaadinConfigurableAspect extends AbstractDependencyInjectionAspect {

// Stuff copied from AbstractInterfaceDrivenDependencyInjectionAspect.aj

    public pointcut beanConstruction(Object bean) :
        initialization(ConfigurableObject+.new(..)) && this(bean);

    public pointcut beanDeserialization(Object bean) :
        execution(Object ConfigurableDeserializationSupport+.readResolve()) &&
        this(bean);

    public pointcut leastSpecificSuperTypeConstruction() : initialization(ConfigurableObject.new(..));

    static interface ConfigurableDeserializationSupport extends Serializable {
    }

    public Object ConfigurableDeserializationSupport.readResolve() throws ObjectStreamException {
        return this;
    }

// Stuff copied from AnnotationBeanConfigurerAspect.aj

    public pointcut inConfigurableBean() : @this(Configurable);

    public pointcut preConstructionConfiguration() : preConstructionConfigurationSupport(*);

    declare parents: @Configurable * implements ConfigurableObject;

    private pointcut preConstructionConfigurationSupport(Configurable c) : @this(c) && if(c.preConstruction());

	declare parents: @Configurable Serializable+ implements ConfigurableDeserializationSupport;

// Our implementation

    @Override
    public void configureBean(Object bean) {
        SpringContextApplication.get().configureBean(bean);
    }
}

