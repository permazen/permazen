
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.jsimpledb.SessionMode;

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
     * Get the {@link SessionMode}s supported by the annotated {@link org.jsimpledb.cli.cmd.AbstractCommand}.
     *
     * @return supported {@link SessionMode}s
     */
    SessionMode[] modes() default { SessionMode.CORE_API, SessionMode.JSIMPLEDB };
}

