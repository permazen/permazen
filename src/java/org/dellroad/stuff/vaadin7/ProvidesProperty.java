
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a Java field or method that provides a Vaadin {@link com.vaadin.data.Property} value.
 *
 * <p>
 * Classes with {@link ProvidesProperty @ProvidesProperty}-annotated fields and methods can be used to automatically
 * generate a {@link PropertyExtractor} and a list of {@link PropertyDef}s by using a {@link PropertyReader}.
 * </p>
 *
 * <p>
 * All of the {@link com.vaadin.data.Container} classes in this package that are configured with a {@link PropertyExtractor}
 * and a list of {@link PropertyDef}s also have a constructor taking a {@link ProvidesProperty @ProvidesProperty}-annotated
 * Java class.
 * </p>
 *
 * <p>
 * Only non-void methods taking zero parameters are supported.
 * </p>
 *
 * @see PropertyReader
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.FIELD })
@Documented
@Inherited
public @interface ProvidesProperty {

    /**
     * Get the name of the Vaadin property. If this is left unset (empty string), then the name of the
     * annotated field, or the bean property name of the annotated "getter" method is used.
     */
    String value() default "";
}

