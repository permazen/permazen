
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines a predicate that matches certain combined values of a list of simple fields.
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ValuesList {

    /**
     * Get the matching criteria for each field in the list.
     *
     * <p>
     * The i<super>th</super> {@link Values &#64;Values} in the returned array corresponds to the i<super>th</super> field
     * in the list. In order for the list of fields to match, <b>all</b> of the fields in the list must match their
     * respective {@link Values &#64;Values} annotation.
     *
     * @return list of field matching criteria
     */
    Values[] value();
}
