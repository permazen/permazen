
/*
 * Copyright (C) 2011 Archie L. Cobbs and other authors. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import java.io.ObjectStreamException;
import java.io.Serializable;

import org.dellroad.stuff.java.ErrorAction;
import org.dellroad.stuff.spring.AbstractConfigurableAspect;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.aspectj.AbstractDependencyInjectionAspect;
import org.springframework.beans.factory.wiring.BeanConfigurerSupport;
import org.springframework.beans.factory.wiring.BeanWiringInfoResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.web.context.ConfigurableWebApplicationContext;

/**
 * Aspect that autowires classes marked with the {@link VaadinConfigurable @VaadinConfigurable} annotation to a
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
 * @see VaadinConfigurable
 */
public aspect VaadinConfigurableAspect extends AbstractConfigurableAspect {

// Stuff copied from AbstractInterfaceDrivenDependencyInjectionAspect.aj

    public pointcut beanConstruction(Object bean) :
        initialization(VaadinConfigurableObject+.new(..)) && this(bean);

    public pointcut beanDeserialization(Object bean) :
        execution(Object VaadinConfigurableDeserializationSupport+.readResolve()) &&
        this(bean);

    public pointcut leastSpecificSuperTypeConstruction() : initialization(VaadinConfigurableObject.new(..));

    declare parents: 
        VaadinConfigurableObject+ && Serializable+ implements VaadinConfigurableDeserializationSupport;

    static interface VaadinConfigurableDeserializationSupport extends Serializable {
    }

    public Object VaadinConfigurableDeserializationSupport.readResolve() throws ObjectStreamException {
        return this;
    }

// Stuff copied from AnnotationBeanConfigurerAspect.aj

    public pointcut inConfigurableBean() : @this(VaadinConfigurable);

    public pointcut preConstructionConfiguration() : preConstructionConfigurationSupport(*);

    declare parents: @VaadinConfigurable * implements VaadinConfigurableObject;

    private pointcut preConstructionConfigurationSupport(VaadinConfigurable c) : @this(c) && if(c.preConstruction());

	declare parents: @VaadinConfigurable Serializable+ implements VaadinConfigurableDeserializationSupport;

// Our implementation

    @Override
    public void configureBean(Object bean) {

        // Verify that the application is locked by this thread, if configured to do so
        VaadinConfigurable annotation = AnnotationUtils.findAnnotation(bean.getClass(), VaadinConfigurable.class);
        if (annotation != null) {
            final ErrorAction errorAction = annotation.ifApplicationNotLocked();
            if (errorAction != ErrorAction.IGNORE) {
                ContextApplication application = ContextApplication.currentApplication();
                if (application != null && !Thread.holdsLock(application)) {
                    String message = "@VaadinConfigurable bean of type " + bean.getClass().getName() + " is being instantiated"
                      + " but the current thread has not locked the associated Vaadin application " + application;
                    switch (errorAction) {
                    case EXCEPTION:
                        throw new BeanInitializationException(message);
                    default:
                        errorAction.execute(message);
                        break;
                    }
                }
            }
        }

        // Proceed
        super.configureBean(bean);
    }

    @Override
    protected BeanFactory getBeanFactory(Object bean) {

        // Get application context
        ConfigurableWebApplicationContext context;
        try {
            context = SpringContextApplication.get().getApplicationContext();
        } catch (IllegalStateException e) {
            throw new BeanInitializationException("can't get application context for autowiring @VaadinConfigurable bean", e);
        }

        // Logging
        if (this.log.isTraceEnabled())
            this.log.trace("using application context " + context + " to configure @VaadinConfigurable bean " + bean);

        // Return associated bean factory
        return context.getBeanFactory();
    }

    @Override
    protected BeanWiringInfoResolver getBeanWiringInfoResolver(Object bean) {
        return new VaadinConfigurableBeanWiringInfoResolver();
    }
}

