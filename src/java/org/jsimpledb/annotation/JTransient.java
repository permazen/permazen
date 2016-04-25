
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Eliminates the annotated method from consideration for JSimpleDB field auto-generation.
 *
 * <p>
 * This annotation is ignored on methods that also have a {@link JField &#64;JField}, {@link JSetField &#64;JSetField},
 * {@link JListField &#64;JListField}, or {@link JMapField &#64;JMapField} annotation.
 *
 * <p>
 * It is only useful on non-abstract methods in classes for which both {@link JSimpleClass#autogenFields}
 * and {@link JSimpleClass#autogenNonAbstract} are true.
 *
 * @see JSimpleClass#autogenFields
 * @see JSimpleClass#autogenNonAbstract
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface JTransient {
}

