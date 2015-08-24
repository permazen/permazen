
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.jsimpledb.SessionMode;

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
     * Get the {@link SessionMode}s supported by the annotated {@link org.jsimpledb.parse.func.AbstractFunction}.
     *
     * @return supported {@link SessionMode}s
     */
    SessionMode[] modes() default { SessionMode.CORE_API, SessionMode.JSIMPLEDB };
}

