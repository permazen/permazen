
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.filter.TypeFilter;

abstract class ScanClassPathFactoryBean extends AbstractFactoryBean<List<Class<?>>>
  implements ResourceLoaderAware, EnvironmentAware {

    protected Environment environment;
    protected ResourceLoader resourceLoader;

    protected String[] basePackages;
    protected boolean useDefaultFilters = true;
    protected String resourcePattern;
    protected List<TypeFilter> includeFilters;
    protected List<TypeFilter> excludeFilters;

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public void setBasePackages(String[] basePackages) {
        this.basePackages = basePackages;
    }

    public void setUseDefaultFilters(boolean useDefaultFilters) {
        this.useDefaultFilters = useDefaultFilters;
    }

    public void setResourcePattern(String resourcePattern) {
        this.resourcePattern = resourcePattern;
    }

    public void setIncludeFilters(List<TypeFilter> includeFilters) {
        this.includeFilters = includeFilters;
    }

    public void setExcludeFilters(List<TypeFilter> excludeFilters) {
        this.excludeFilters = excludeFilters;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Preconditions.checkState(this.basePackages != null, "no basePackages configured");
        Preconditions.checkState(this.environment != null, "no environment configured");
        Preconditions.checkState(this.resourceLoader != null, "no resourceLoader configured");
    }

    @Override
    protected List<Class<?>> createInstance() {

        // Build and configure scanner
        final AnnotatedClassScanner scanner = this.createScanner();
        scanner.setResourceLoader(this.resourceLoader);
        if (this.resourcePattern != null)
            scanner.setResourcePattern(this.resourcePattern);
        if (this.includeFilters != null) {
            for (TypeFilter filter : this.includeFilters)
                scanner.addIncludeFilter(filter);
        }
        if (this.excludeFilters != null) {
            for (TypeFilter filter : this.excludeFilters)
                scanner.addExcludeFilter(filter);
        }

        // Scan for classes
        final ArrayList<String> classNames = scanner.scanForClasses(this.basePackages);

        // Load those classes
        final ArrayList<Class<?>> classes = new ArrayList<>();
        for (String name : classNames) {
            try {
                classes.add(Class.forName(name, false, this.resourceLoader.getClassLoader()));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("failed to load class `" + name + "'", e);
            }
        }

        // Done
        return classes;
    }

    @Override
    public Class<?> getObjectType() {
        return List.class;
    }

    abstract AnnotatedClassScanner createScanner();
}

