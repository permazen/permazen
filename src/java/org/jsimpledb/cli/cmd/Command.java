
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.cmd;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for classes that define custom {@link org.jsimpledb.cli.CliSession} commands.
 *
 * <p>
 * Annotated classes must extend {@link AbstractCommand}
 * and have a public constructor taking either zero parameters or a single {@link org.jsimpledb.cli.CliSession} parameter.
 * </p>
 *
 * @see AbstractCommand
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface Command {

    /**
     * Determine whether the annotated {@link org.jsimpledb.cli.cmd.AbstractCommand} works properly in Core API mode,
     * i.e., when the there are no {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated Java model
     * classes defined and the core {@link org.jsimpledb.core.Database} API is used.
     *
     * @return true if the command works in core API mode
     */
    boolean worksInCoreAPIMode() default true;
}

