
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates JSimpleDB model class instance methods that are to be invoked to create a GUI object that is
 * suitable for representing a reference to the instance.
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static) and take zero parameters. It does not need to be public.
 * If the returned object is a {@link com.vaadin.ui.Component}, then it will be used directly,
 * otherwise the {@link String} value of the returned object is used.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface ProvidesReferenceLabel {
}

