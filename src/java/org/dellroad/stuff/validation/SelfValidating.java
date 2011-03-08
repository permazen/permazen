
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.validation;

import javax.validation.ConstraintValidatorContext;

/**
 * Implemented by classes that can validate themselves. Such classes should be annotated with
 * the {@link SelfValidates @SelfValidates} constraint.
 *
 * @see SelfValidates
 */
public interface SelfValidating {

    /**
     * Validate this instance.
     *
     * @throws SelfValidationException to indicate this instance is invalid
     */
    void checkValid(ConstraintValidatorContext context) throws SelfValidationException;
}

