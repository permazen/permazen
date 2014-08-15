
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * {@link PersistentObject} implementation of Spring's {@link PersistenceExceptionTranslator} interface.
 */
@SuppressWarnings("serial")
public class PersistentObjectExceptionTranslator implements PersistenceExceptionTranslator {

    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException e) {
        if (e instanceof PersistentObjectVersionException)
            return new OptimisticLockingFailureException(null, e);
        if (e instanceof PersistentObjectValidationException)
            return new DataIntegrityViolationException(null, e);
        return null;
    }
}

