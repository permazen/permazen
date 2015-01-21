
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Configures whether and how objects are enqueued for validation in a {@link JTransaction}.
 *
 * @see org.jsimpledb.JTransaction#validate
 * @see org.jsimpledb.JObject#revalidate
 * @see org.jsimpledb.annotation.Validate
 */
public enum ValidationMode {

    /**
     * Validation is disabled. No objects are enqueued for validation, even if {@link JObject#revalidate} is invoked.
     */
    DISABLED,

    /**
     * Objects are enqueued for validation only when {@link JObject#revalidate} is explicitly invoked.
     */
    MANUAL,

    /**
     * Objects are enqueued for validation automatically.
     *
     * <p>
     * In this mode, objects are enqueued for validation whenever {@link JObject#revalidate} is invoked, or automatically when:
     * <ul>
     *  <li>An instance is {@linkplain org.jsimpledb.JTransaction#create created}, and the Java model type (or any super-type)
     *      has a JSR 303 or {@link org.jsimpledb.annotation.Validate &#64;Validate} annotation on itself or a public method</li>
     *  <li>An instance is {@linkplain org.jsimpledb.JObject#upgrade upgraded}, and the Java model type (or any super-type)
     *      has a JSR 303 or {@link org.jsimpledb.annotation.Validate &#64;Validate} annotation on itself or a public method</li>
     *  <li>A database field is modified, and the corresponding Java model `getter' method has any JSR 303 annotations</li>
     * </ul>
     * </p>
     *
     * <p>
     * Note that the presence of a {@link org.jsimpledb.annotation.Validate &#64;Validate} annotation on some method
     * does <b>not</b> in itself result in automatic validation when any field changes. To achieve that, add an
     * {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated method that invokes
     * {@link org.jsimpledb.JObject#revalidate this.revalidate()}.
     * </p>
     */
    AUTOMATIC;
}

