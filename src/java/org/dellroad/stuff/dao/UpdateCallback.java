
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.dao;

import javax.persistence.Query;

/**
 * Helper class for JPA bulk updates.
 */
public abstract class UpdateCallback extends QueryCallback<Integer> {

    /**
     * Execute the query. This method invokes {@link Query#executeUpdate} and returns the result.
     */
    @Override
    protected final Integer executeQuery(Query query) {
        return query.executeUpdate();
    }
}

