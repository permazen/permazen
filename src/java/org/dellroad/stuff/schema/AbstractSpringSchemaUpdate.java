
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
 * Support superclass for {@link SchemaUpdate}s declared in Spring {@link BeanFactory}s that infer their
 * names and required predecessors from their Spring bean attributes.
 *
 * <p>
 * Instances infer their {@linkplain #getName name} and {@linkplain #getRequiredPredecessors required predecessors} from
 * their Spring bean name (specified by the <code>id</code> XML attribute) and Spring dependencies (specified by
 * the <code>depends-on</code> XML attribute), respectively.
 *
 * <p>
 * Note: the use of <code>depends-on</code> is an abuse of Spring's dependency notation for convenience. Normally
 * <code>depends-on</code> refers to bean intialization ordering, whereas this class uses it to refer to schema update ordering.
 * Schema updates are not normally expected to have any initialization ordering requirements, so this abuse shouldn't matter.
 * If they do, this class should not be used.
 *
 * <p>
 * The containing {@link BeanFactory} must be a {@link ConfigurableBeanFactory} (normally this is always the case).
 *
 * @param <T> database transaction type
 */
public abstract class AbstractSpringSchemaUpdate<T> extends AbstractSchemaUpdate<T>
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
        if (this.beanFactory == null)
            throw new IllegalArgumentException("no BeanFactory configured");
        if (this.beanName == null)
            throw new IllegalArgumentException("no beanName configured");
        this.setName(this.beanName);
        this.setRequiredPredecessorsFromDependencies();
    }

    /**
     * Infer required predecessors from Spring dependencies.
     *
     * @throws IllegalArgumentException if this instance's {@code beanFactory} is not yet configured,
     *  or not a {@link ConfigurableBeanFactory}
     */
    @SuppressWarnings("unchecked")
    protected void setRequiredPredecessorsFromDependencies() {

        // Check factory type
        if (!(this.beanFactory instanceof ConfigurableBeanFactory))
            throw new IllegalArgumentException("BeanFactory is not a ConfigurableBeanFactory: " + this.beanFactory);
        ConfigurableBeanFactory configurableBeanFactory = (ConfigurableBeanFactory)this.beanFactory;

        // Find required predecessors defined as Spring dependencies
        String[] predecessorNames = configurableBeanFactory.getDependenciesForBean(this.beanName);
        HashSet<SchemaUpdate<T>> predecessors = new HashSet<SchemaUpdate<T>>(predecessorNames.length);
        for (String predecessorName : predecessorNames) {
            try {
                predecessors.add((SchemaUpdate<T>)configurableBeanFactory.getBean(predecessorName, SchemaUpdate.class));
            } catch (NoSuchBeanDefinitionException e) {
                continue;
            } catch (BeanNotOfRequiredTypeException e) {
                continue;
            }
        }
        this.setRequiredPredecessors(predecessors);
    }
}

