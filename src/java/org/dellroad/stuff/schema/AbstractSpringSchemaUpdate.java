
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.HashSet;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

/**
 * Support superclass for {@link SchemaUpdate}s declared in Spring {@link BeanFactory}s that infer their update
 * name and required predecessors from their Spring bean attributes.
 *
 * <p>
 * Instances infer their {@linkplain #getName name} and {@linkplain #getRequiredPredecessors required predecessors} from
 * their Spring bean name (specified by the <code>id</code> XML attribute) and Spring dependencies (specified by
 * the <code>depends-on</code> XML attribute), respectively.
 *
 * <p>
 * The use of <code>depends-on</code> is an abuse of Spring's dependency notation for convenience. Normally
 * <code>depends-on</code> refers to bean intialization ordering, whereas this class uses it to refer to schema update ordering.
 * Schema updates are not normally expected to have any initialization ordering requirements, so this abuse shouldn't matter.
 * If they do, this class should not be used.
 *
 * <p>
 * Also, the containing {@link BeanFactory} must be a {@link ConfigurableBeanFactory} (normally this is always the case).
 */
public abstract class AbstractSpringSchemaUpdate extends AbstractSchemaUpdate
  implements BeanNameAware, BeanFactoryAware, InitializingBean {

    protected String beanName;
    protected BeanFactory beanFactory;

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /**
     * Configures the update name and required predecessors based on the Spring bean's name and
     * {@link BeanFactory} dependencies.
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        this.setName(this.beanName);
        AbstractSpringSchemaUpdate.setRequiredPredecessorsFromDependencies(this, this.beanFactory);
    }

    /**
     * Infer required predecessors from Spring dependencies.
     *
     * @param update the update in question; it's name must be configured and will be used to query for dependencies
     * @param beanFactory the bean factory containing the update and all the update's depdendencies
     * @throws IllegalArgumentException if {@code beanFactory} is not a {@link ConfigurableBeanFactory}
     */
    public static void setRequiredPredecessorsFromDependencies(ModifiableSchemaUpdate update, BeanFactory beanFactory) {

        // Check factory type
        if (!(beanFactory instanceof ConfigurableBeanFactory))
            throw new IllegalArgumentException("BeanFactory is not a ConfigurableBeanFactory: " + beanFactory);
        ConfigurableBeanFactory configurableBeanFactory = (ConfigurableBeanFactory)beanFactory;

        // Find required predecessors defined as Spring dependencies
        String[] predecessorNames = configurableBeanFactory.getDependenciesForBean(update.getName());
        HashSet<SchemaUpdate> predecessors = new HashSet<SchemaUpdate>(predecessorNames.length);
        for (String predecessorName : predecessorNames) {
            try {
                predecessors.add(configurableBeanFactory.getBean(predecessorName, SchemaUpdate.class));
            } catch (NoSuchBeanDefinitionException e) {
                continue;
            } catch (BeanNotOfRequiredTypeException e) {
                continue;
            }
        }
        update.setRequiredPredecessors(predecessors);
    }
}

