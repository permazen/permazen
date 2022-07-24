
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import com.google.common.base.Preconditions;

import io.permazen.util.ApplicationClassLoader;

import java.lang.annotation.Annotation;
import java.util.ArrayList;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.env.Environment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/**
 * Scans the classpath for classes having one or more configured type annotations.
 */
public class AnnotatedClassScanner extends ClassPathScanningCandidateComponentProvider {

    // This stupid hack is required because of JLS stupidity that doesn't allow initializing final fields prior to super()
    private static final ThreadLocal<ArrayList<Class<? extends Annotation>>> INIT_HACK = new ThreadLocal<>();

    private final ArrayList<Class<? extends Annotation>> annotationTypes;

    /**
     * Constructor. Enables use of default filters and uses a {@link org.springframework.core.env.StandardEnvironment}.
     *
     * @param loader {@link ClassLoader} to use for finding classes
     * @param annotationTypes type annotations to search for
     * @throws IllegalArgumentException if {@code annotationTypes} is null, empty, or contains any non-annotation types
     */
    public AnnotatedClassScanner(ClassLoader loader, Class<?>... annotationTypes) {
        super(AnnotatedClassScanner.initHack(true, annotationTypes));
        this.initializeResourceLoader(loader);
        this.annotationTypes = INIT_HACK.get();
        INIT_HACK.remove();
    }

    /**
     * Constructor.
     *
     * @param loader {@link ClassLoader} to use for finding classes
     * @param useDefaultFilters whether to register the default filters for the specified {@code annotationTypes}
     * @param environment environment to use
     * @param annotationTypes type annotations to search for
     * @throws IllegalArgumentException if {@code annotationTypes} is null, empty, or contains any non-annotation types
     */
    public AnnotatedClassScanner(ClassLoader loader, boolean useDefaultFilters, Environment environment,
      Class<?>... annotationTypes) {
        super(AnnotatedClassScanner.initHack(useDefaultFilters, annotationTypes), environment);
        this.initializeResourceLoader(loader);
        this.annotationTypes = INIT_HACK.get();
        INIT_HACK.remove();
    }

    private static boolean initHack(boolean rtn, Class<?>... annotationTypes) {
        Preconditions.checkArgument(annotationTypes != null && annotationTypes.length > 0, "null/empty annotationTypes");
        final ArrayList<Class<? extends Annotation>> list = new ArrayList<>();
        for (Class<?> cl : annotationTypes) {
            try {
                list.add(cl.asSubclass(Annotation.class));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(cl + " is not an annotation type");
            }
        }
        INIT_HACK.set(list);
        return rtn;
    }

    /**
     * Locate annotated classes on the classpath. Does not actually load the classes.
     *
     * @param basePackages one or more base pacakge names
     * @return list of names of classes having any of the configured type annotations
     * @throws IllegalArgumentException if {@code basePackages} is empty
     */
    public ArrayList<String> scanForClasses(String... basePackages) {
        Preconditions.checkArgument(basePackages != null && basePackages.length > 0, "at least one base package name is required");
        final ArrayList<String> nameList = new ArrayList<>();
        for (String basePackage : basePackages) {
            for (BeanDefinition candidate : this.findCandidateComponents(basePackage))
                nameList.add(candidate.getBeanClassName());
        }
        return nameList;
    }

    /**
     * Register default include/exclude filters.
     *
     * <p>
     * Overridden in {@link AnnotatedClassScanner} to set the default filters to be ones that match classes
     * annotated with any of the annotation types provided to the constructor.
     */
    @Override
    protected void registerDefaultFilters() {
        for (Class<? extends Annotation> annotationType : this.annotationTypes != null ? this.annotationTypes : INIT_HACK.get())
            this.addIncludeFilter(new AnnotationTypeFilter(annotationType));
    }

    protected void initializeResourceLoader(ClassLoader loader) {
        this.setResourceLoader(new DefaultResourceLoader(ApplicationClassLoader.getInstance(loader)));
    }
}
