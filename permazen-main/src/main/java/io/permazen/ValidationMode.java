
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.OnValidate;

import jakarta.validation.groups.Default;

/**
 * Configures whether and how objects are enqueued for validation in a {@link PermazenTransaction}.
 *
 * @see PermazenTransaction#validate
 * @see PermazenObject#revalidate
 * @see OnValidate
 */
public enum ValidationMode {

    /**
     * Validation is disabled. No objects are enqueued for validation, even if {@link PermazenObject#revalidate} is invoked.
     */
    DISABLED,

    /**
     * Objects are enqueued for validation only when {@link PermazenObject#revalidate} is explicitly invoked.
     */
    MANUAL,

    /**
     * Objects are enqueued for validation automatically (if necessary) when they are modified.
     *
     * <p>
     * In this mode, objects are enqueued for validation whenever {@link PermazenObject#revalidate} is invoked; in addition,
     * objects are enqueued for validation automatically as if by {@link PermazenObject#revalidate} when:
     * <ul>
     *  <li>An instance is {@linkplain PermazenTransaction#create created}, and the Java model type (or any super-type)
     *      has a JSR 303 annotated public method or {@link OnValidate &#64;OnValidate} annoted method</li>
     *  <li>An instance is {@linkplain PermazenObject#migrateSchema migrated}, and the Java model type (or any super-type)
     *      has a JSR 303 annotated public method or {@link OnValidate &#64;OnValidate} annoted method</li>
     *  <li>An instance field is modified, and the corresponding Java model `getter' method has any JSR 303 annotations</li>
     * </ul>
     *
     * <p>
     * Note that the presence of a {@link OnValidate &#64;OnValidate} annotation on a method
     * does <b>not</b> in itself result in automatic validation when any field changes (it merely specifies an action
     * to take when validation occurs). To trigger revalidation after field changes, add
     * {@link OnChange &#64;OnChange}-annotated method(s) that invoke {@link PermazenObject#revalidate this.revalidate()}.
     *
     * <p>
     * Note that {@link #AUTOMATIC} enqueues as if by {@link PermazenObject#revalidate}, i.e., the
     * {@link Default} validation group applies. Therefore, if a constraint has explicit {@code groups()},
     * and none extend {@link Default}, then it will not be applied automatically.
     */
    AUTOMATIC;
}
