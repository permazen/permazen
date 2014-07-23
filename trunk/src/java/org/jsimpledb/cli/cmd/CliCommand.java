
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
 * Java annotation for classes that define custom {@link org.jsimpledb.JSimpleDB} command line interface (CLI) commands.
 *
 * <p>
 * Annotated classes must extend {@link org.jsimpledb.cli.cmd.Command}
 * and have a public constructor taking either zero parameters or a single {@link org.jsimpledb.cli.Session} parameter.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface CliCommand {

    /**
     * Determine whether the annotated {@link org.jsimpledb.cli.cmd.Command} works properly in JSimpleDB mode,
     * i.e., when the there are {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated Java model
     * classes defined and the {@link org.jsimpledb.JSimpleDB} API is used.
     */
    boolean worksInJSimpleDBMode() default true;

    /**
     * Determine whether the annotated {@link org.jsimpledb.cli.cmd.Command} works properly in Core API mode,
     * i.e., when the there are no {@link org.jsimpledb.annotation.JSimpleClass &#64;JSimpleClass}-annotated Java model
     * classes defined and the core {@link org.jsimpledb.core.Database} API is used.
     */
    boolean worksInCoreAPIMode() default true;
}

