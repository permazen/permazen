
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <b>Deprecated.</b> This annotation has been renamed to {@link OnValidate &#64;OnValidate}.
 *
 * @deprecated Replaced by {@link OnValidate &#64;OnValidate}
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Deprecated
public @interface Validate {

    /**
     * Specify the validation group(s) for which the annotated method should be invoked.
     *
     * @return validation group(s) to use for validation; if empty, {@link javax.validation.groups.Default} is assumed
     */
    Class<?>[] groups() default {};
}

