
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for classes that define custom {@link org.jsimpledb.parse.ParseContext} functions.
 *
 * <p>
 * Annotated classes must extend {@link AbstractFunction}
 * and have a public constructor taking either zero parameters or a single {@link org.jsimpledb.parse.ParseSession} parameter.
 * </p>
 *
 * @see AbstractFunction
 * @see SimpleFunction
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface Function {

    /**
     * Determine whether the annotated {@link org.jsimpledb.parse.func.AbstractFunction} works properly in Core API mode,
     * i.e., when the there are no {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated Java model
     * classes defined and the core {@link org.jsimpledb.core.Database} API is used.
     */
    boolean worksInCoreAPIMode() default true;
}

