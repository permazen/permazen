
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;

import org.dellroad.stuff.vaadin7.PropertyDef;
import org.dellroad.stuff.vaadin7.PropertyExtractor;
import org.dellroad.stuff.vaadin7.ProvidesPropertyScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty}{@code(}{@link
 * JObjectContainer#REFERENCE_LABEL_PROPERTY}{@code )} methods for Java classes.
 */
final class ReferenceMethodInfoCache {

    static final PropertyInfo<Void> NOT_FOUND = new PropertyInfo<Void>(null, null);

    private static final ReferenceMethodInfoCache INSTANCE = new ReferenceMethodInfoCache();

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final LoadingCache<Class<?>, PropertyInfo<?>> cache = CacheBuilder.newBuilder().weakKeys().build(
      new CacheLoader<Class<?>, PropertyInfo<?>>() {
        @Override
        public PropertyInfo<?> load(Class<?> type) {
            return ReferenceMethodInfoCache.this.findReferenceLablePropertyInfo(type);
        }
    });

    private ReferenceMethodInfoCache() {
    }

    /**
     * Get the singleton instance.
     */
    public static ReferenceMethodInfoCache getInstance() {
        return ReferenceMethodInfoCache.INSTANCE;
    }

    /**
     * Get property definition and extractor for the reference label method in the specified class, if any.
     *
     * @throws IllegalArgumentException if {@code type} is null
     */
    @SuppressWarnings("unchecked")
    public <T> PropertyInfo<T> getReferenceMethodInfo(Class<T> type) {
        return (PropertyInfo<T>)this.cache.getUnchecked(type);
    }

    @SuppressWarnings("unchecked")
    private <T> PropertyInfo<T> findReferenceLablePropertyInfo(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final ProvidesPropertyScanner<T> scanner = new ProvidesPropertyScanner<T>(type);
        final PropertyDef<?> propertyDef = Iterables.find(scanner.getPropertyDefs(), new Predicate<PropertyDef<?>>() {
            @Override
            public boolean apply(PropertyDef<?> propertyDef) {
                return propertyDef.getName().equals(JObjectContainer.REFERENCE_LABEL_PROPERTY);
            }
        }, null);
        return propertyDef != null ? new PropertyInfo<T>(propertyDef, scanner.getPropertyExtractor()) : (PropertyInfo<T>)NOT_FOUND;
    }

// PropertyInfo

    public static class PropertyInfo<T> {

        private final PropertyDef<?> propertyDef;
        private final PropertyExtractor<T> propertyExtractor;

        PropertyInfo(PropertyDef<?> propertyDef, PropertyExtractor<T> propertyExtractor) {
            this.propertyDef = propertyDef;
            this.propertyExtractor = propertyExtractor;
        }

        public PropertyDef<?> getPropertyDef() {
            return this.propertyDef;
        }

        public PropertyExtractor<T> getPropertyExtractor() {
            return this.propertyExtractor;
        }
    }
}

