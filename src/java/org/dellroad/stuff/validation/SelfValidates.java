
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;

/**
 * Validation constraint that when applied to classes that implement the {@link SelfValidating} interface
 * allows custom validation by the class itself via {@link SelfValidating#checkValid SelfValidating.checkValid()}.
 */
@Documented
@Constraint(validatedBy = SelfValidatingValidator.class)
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SelfValidates {

    String message() default "{org.dellroad.stuff.validation.SelfValidates.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}

