
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.List;
import java.util.Set;

/**
 * A one-time database schema update. Instances typically perform changes to the database schema, though non-schema updates
 * that just manipulate the data in the database are perfectly valid as well.
 *
 * <p>
 * Each {@link SchemaUpdate} has a unique name among all updates ever applied to a single database, and zero
 * or more required predecessors, which are other dependent updates that must be applied first.
 * </p>
 *
 * <p>
 * Once an update has been applied to a database, it must not be changed; otherwise, inconsistencies
 * can exist between databases that were updated using the old version vs. databases that were
 * updated using the new version. If an update has been applied but had the wrong behavior, instead of
 * changing the update, it's better to create a new update that depends on the first as a predecessor
 * and corrects the mistake.
 * </p>
 *
 * <p>
 * If you do have to change an update after it has been applied, then those databases that have
 * already seen the previous version of the update must be manually corrected so they are in the
 * same state that the new version of the update would have left them.
 * </p>
 *
 * @param <C> database connection type
 */
public interface SchemaUpdate<C> {

    /**
     * Get the unique name of this update. This name must be unique among all updates ever applied to the database
     * and must never change once this update has been applied to any database.
     *
     * @return the name of this update; must not be the empty string
     */
    String getName();

    /**
     * Get the the other updates (if any) that must be applied <b>before</b> this update may be applied.
     *
     * @return set of zero or more other updates
     * @see #getName
     */
    Set<SchemaUpdate<C>> getRequiredPredecessors();

    /**
     * Get the action(s) that comprise this update. Ideally, individual actions should be atomic database operations, i.e.,
     * each one should either finish completely, or else leave the database in a state where it can be tried again.
     * In any case, each action will be applied within its own transaction when transactions are supported by the database
     * unless {@link #isSingleAction} returns true.
     *
     * @return a list of zero or more actions to apply
     * @see #isSingleAction
     */
    List<DatabaseAction<C>> getDatabaseActions();

    /**
     * Determine whether, if this instance contains multiple individual actions, should they be applied in a single
     * transaction and recorded as a single update. Normally this is false. If true, partially completed updates
     * can result if one of the action fails.
     */
    boolean isSingleAction();
}

