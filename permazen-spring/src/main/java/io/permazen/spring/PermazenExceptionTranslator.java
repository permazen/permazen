
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import io.permazen.ValidationException;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.InvalidSchemaException;
import io.permazen.core.ReferencedObjectException;
import io.permazen.core.RollbackOnlyTransactionException;
import io.permazen.core.SchemaMismatchException;
import io.permazen.core.StaleTransactionException;
import io.permazen.kv.KVTransactionTimeoutException;
import io.permazen.kv.RetryKVTransactionException;
import io.permazen.kv.StaleKVTransactionException;

import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * Permazen implementation of Spring's {@link PersistenceExceptionTranslator} interface.
 *
 * @see io.permazen.spring
 */
@SuppressWarnings("serial")
public class PermazenExceptionTranslator implements PersistenceExceptionTranslator {

    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException e0) {
        if (e0 == null)
            return null;
        final String message = e0.getMessage() != null ? e0.getMessage() : "wrapped exception";
        if (e0 instanceof DeletedObjectException) {
            final DeletedObjectException e = (DeletedObjectException)e0;
            return new EmptyResultDataAccessException(String.format("object %s not found", e.getId()), 1, e);
        }
        if (e0 instanceof SchemaMismatchException)
            return new DataIntegrityViolationException(message, e0);
        if (e0 instanceof InvalidSchemaException)
            return new InvalidDataAccessResourceUsageException(message, e0);
        if (e0 instanceof ReferencedObjectException)
            return new DataIntegrityViolationException(message, e0);
        if (e0 instanceof RollbackOnlyTransactionException)
            return new InvalidDataAccessApiUsageException(message, e0);
        if (e0 instanceof KVTransactionTimeoutException)
            return new QueryTimeoutException(message, e0);
        if (e0 instanceof StaleTransactionException || e0 instanceof StaleKVTransactionException)
            return new InvalidDataAccessApiUsageException(message, e0);
        if (e0 instanceof RetryKVTransactionException)
            return new ConcurrencyFailureException(message, e0);
        if (e0 instanceof ValidationException)
            return new DataIntegrityViolationException(message, e0);
        return null;
    }
}
