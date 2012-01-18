
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.Collections;
import java.util.List;

import org.dellroad.stuff.schema.AbstractSpringSchemaUpdate;
import org.dellroad.stuff.schema.DatabaseAction;

/**
 * Support superclass for Spring-enabled {@link org.dellroad.stuff.schema.SchemaUpdate}s for use with
 * a {@link PersistentObjectSchemaUpdater}. Instances include a single {@link DatabaseAction} (namely, themselves).
 *
 * @param <T> type of the persistent object
 */
public abstract class SpringPersistentObjectSchemaUpdate<T> extends AbstractSpringSchemaUpdate<PersistentFileTransaction>
  implements DatabaseAction<PersistentFileTransaction> {

    /**
     * Apply this update to the given transaction.
     */
    @Override
    public abstract void apply(PersistentFileTransaction transaction);

    @Override
    public final List<SpringPersistentObjectSchemaUpdate<T>> getDatabaseActions() {
        return Collections.singletonList(this);
    }
}

