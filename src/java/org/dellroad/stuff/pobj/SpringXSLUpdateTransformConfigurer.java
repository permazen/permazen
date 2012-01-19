
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

/**
 * Spring {@link BeanPostProcessor} that looks for {@link SpringXSLPersistentObjectSchemaUpdate} beans that don't have
 * an explicit {@link SpringXSLPersistentObjectSchemaUpdate#setTransform transform resource configured}, and configures
 * them using a resource location based on the bean name, by simply adding a configured prefix and suffix.
 *
 * @param <T> type of the persistent object
 * @see SpringXSLPersistentObjectSchemaUpdate
 */
public class SpringXSLUpdateTransformConfigurer implements BeanPostProcessor, ResourceLoaderAware {

    /**
     * Default location prefix: <code>{@value}</code>.
     */
    public static final String DEFAULT_LOCATION_PREFIX = "/";

    /**
     * Default location suffix: <code>{@value}</code>.
     */
    public static final String DEFAULT_LOCATION_SUFFIX = ".xsl";

    private ResourceLoader resourceLoader = new DefaultResourceLoader();
    private String prefix = DEFAULT_LOCATION_PREFIX;
    private String suffix = DEFAULT_LOCATION_SUFFIX;

    /**
     * Set the location prefix.
     */
    public void setPrefix(String prefix) {
        if (prefix == null)
            prefix = "";
        this.prefix = prefix;
    }

    /**
     * Set the location suffix.
     */
    public void setSuffix(String suffix) {
        if (suffix == null)
            suffix = "";
        this.suffix = suffix;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        if (bean instanceof SpringXSLPersistentObjectSchemaUpdate) {
            SpringXSLPersistentObjectSchemaUpdate update = (SpringXSLPersistentObjectSchemaUpdate)bean;
            if (update.getTransform() == null)
                update.setTransform(this.resourceLoader.getResource(this.getImpliedTransformResourceLocation(beanName)));
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        return bean;
    }

    /**
     * Derive the implied transform resource location for the update with the given bean name.
     *
     * <p>
     * The implementation in {@link SpringXSLUpdateTransformConfigurer} simply prepends the configured
     * prefix and appends the configured suffix to the {@linkplain #beanName bean name}.
     */
    protected String getImpliedTransformResourceLocation(String beanName) {
        return this.prefix + beanName + this.suffix;
    }
}

