
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.wiring.BeanWiringInfo;
import org.springframework.beans.factory.wiring.BeanWiringInfoResolver;
import org.springframework.util.ClassUtils;

/**
 * {@link org.springframework.beans.factory.wiring.BeanWiringInfoResolver} that
 * uses the {@link VaadinConfigurable @VaadinConfigurable} annotation to determine autowiring needs.
 *
 * <p>
 * This implementation is derived from Spring's {@code AnnotationBeanWiringInfoResolver}
 * implementation and therefore shares its license (also Apache).
 * </p>
 *
 * @see VaadinConfigurable
 */
class VaadinConfigurableBeanWiringInfoResolver implements BeanWiringInfoResolver {

    public BeanWiringInfo resolveWiringInfo(Object bean) {
        VaadinConfigurable annotation = bean.getClass().getAnnotation(VaadinConfigurable.class);
        return annotation != null ? this.buildWiringInfo(bean, annotation) : null;
    }

    private BeanWiringInfo buildWiringInfo(Object bean, VaadinConfigurable annotation) {
        if (!Autowire.NO.equals(annotation.autowire()))
            return new BeanWiringInfo(annotation.autowire().value(), annotation.dependencyCheck());
        if (annotation.value().length() > 0)
            return new BeanWiringInfo(annotation.value(), false);
        return new BeanWiringInfo(getDefaultBeanName(bean), true);
    }

    private String getDefaultBeanName(Object bean) {
        return ClassUtils.getUserClass(bean).getName();
    }
}

