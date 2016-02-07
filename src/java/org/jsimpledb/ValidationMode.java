
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

/**
 * Configures whether and how objects are enqueued for validation in a {@link JTransaction}.
 *
 * @see org.jsimpledb.JTransaction#validate
 * @see org.jsimpledb.JObject#revalidate
 * @see org.jsimpledb.annotation.OnValidate
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
     * Objects are enqueued for validation automatically (if necessary) when they are modified.
     *
     * <p>
     * In this mode, objects are enqueued for validation whenever {@link JObject#revalidate} is invoked; in addition,
     * objects are enqueued for validation automatically as if by {@link JObject#revalidate} when:
     * <ul>
     *  <li>An instance is {@linkplain org.jsimpledb.JTransaction#create created}, and the Java model type (or any super-type)
     *      has a JSR 303 annotated public method or {@link org.jsimpledb.annotation.OnValidate &#64;OnValidate} annoted method</li>
     *  <li>An instance is {@linkplain org.jsimpledb.JObject#upgrade upgraded}, and the Java model type (or any super-type)
     *      has a JSR 303 annotated public method or {@link org.jsimpledb.annotation.OnValidate &#64;OnValidate} annoted method</li>
     *  <li>An instance field is modified, and the corresponding Java model `getter' method has any JSR 303 annotations</li>
     * </ul>
     *
     * <p>
     * Note that the presence of a {@link org.jsimpledb.annotation.OnValidate &#64;OnValidate} annotation on a method
     * does <b>not</b> in itself result in automatic validation when any field changes (it merely specifies an action
     * to take when validation occurs). To trigger revalidation after field changes, add
     * {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated method(s) that invoke
     * {@link org.jsimpledb.JObject#revalidate this.revalidate()}.
     *
     * <p>
     * Note that {@link #AUTOMATIC} enqueues as if by {@link JObject#revalidate}, i.e., the
     * {@link javax.validation.groups.Default} validation group applies. Therefore, if a constraint has explicit {@code groups()},
     * and none extend {@link javax.validation.groups.Default}, then it will not be applied automatically.
     */
    AUTOMATIC;
}

