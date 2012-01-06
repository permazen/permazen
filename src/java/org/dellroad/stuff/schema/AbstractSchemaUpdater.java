
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.dellroad.stuff.graph.TopologicalSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the initialization and schema maintenance of a database.
 *
 * <p>
 * In this class, a <b>database</b> is some stateful object whose structure and/or content may need to change over time.
 * <b>Updates</b> are uniquely named objects capable of making such changes. Databases are also capable of storing the
 * names of the already-applied updates.
 *
 * <p>
 * Given a database and a set of current updates, this class will ensure that a database is initialized if necessary
 * and up-to-date with respect to the updates.
 *
 * <p>
 * The primary method is {@link #initializeAndUpdateDatabase initializeAndUpdateDatabase()}, which will:
 * <ul>
 * <li>Initialize an {@linkplain #databaseNeedsInitialization empty} database (if necessary);</li>
 * <li>Apply any outstanding {@link SchemaUpdate}s as needed, ordered properly according to
 *      their {@linkplain SchemaUpdate#getRequiredPredecessors predecessor constraints}; and</li>
 * <li>Keep track of which {@link SchemaUpdate}s have already been applied across restarts.</li>
 * </ul>
 * </p>
 *
 * @param <D> database type
 * @param <C> database connection type
 */
public abstract class AbstractSchemaUpdater<D, C> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private Collection<SchemaUpdate<C>> updates;
    private boolean ignoreUnrecognizedUpdates;

    /**
     * Get the configured updates. This property is required.
     *
     * @return configured updates
     * @see #setUpdates setUpdates()
     */
    public Collection<SchemaUpdate<C>> getUpdates() {
        return this.updates;
    }

    /**
     * Configure the updates.
     * This should be the set of all updates that may need to be applied to the database.
     *
     * <p>
     * For any given application, ideally this set should be "write only" in the sense that once an update is added to the set
     * and applied to one or more actual databases, the update and its name should thereafter never change. Otherwise,
     * it would be possible for different databases to have inconsistent schemas even though the same updates were recorded.
     *
     * <p>
     * Furthermore, if not configured to {@linkplain #setIgnoreUnrecognizedUpdates ignore unrecognized updates already applied}
     * (the default behavior), then updates must never be removed from this set as the application evolves;
     * see {@link #setIgnoreUnrecognizedUpdates} for more information on the rationale.
     *
     * @param updates all updates; each update must have a unique {@link SchemaUpdate#getName name}.
     * @see #getUpdates
     * @see #setIgnoreUnrecognizedUpdates
     */
    public void setUpdates(Collection<SchemaUpdate<C>> updates) {
        this.updates = updates;
    }

    /**
     * Determine whether unrecognized updates are ignored or cause an exception.
     *
     * @return true if unrecognized updates should be ignored, false if they should cause an exception to be thrown
     * @see #setIgnoreUnrecognizedUpdates setIgnoreUnrecognizedUpdates()
     */
    public boolean isIgnoreUnrecognizedUpdates() {
        return this.ignoreUnrecognizedUpdates;
    }

    /**
     * Configure behavior when an unknown update is registered as having already been applied in the database.
     *
     * <p>
     * The default behavior is <code>false</code>, which results in an exception being thrown. This protects against
     * accidental downgrades (i.e., running older code against a database with a newer schema), which are not supported.
     * However, this also requires that all updates that might ever possibly have been applied to the database be
     * present in the set of configured updates.
     *
     * <p>
     * Setting this to <code>true</code> will result in unrecognized updates simply being ignored.
     * This setting loses the downgrade protection but allows obsolete schema updates to be dropped over time.
     *
     * @param ignoreUnrecognizedUpdates whether to ignore unrecognized updates
     * @see #isIgnoreUnrecognizedUpdates
     */
    public void setIgnoreUnrecognizedUpdates(boolean ignoreUnrecognizedUpdates) {
        this.ignoreUnrecognizedUpdates = ignoreUnrecognizedUpdates;
    }

    /**
     * Perform database schema initialization and updates.
     *
     * <p>
     * This method applies the following logic: if the {@linkplain #databaseNeedsInitialization database needs initialization},
     * then {@linkplain #initializeDatabase initialize the database} and {@linkplain #recordUpdateApplied record} each update
     * as having been applied; then, apply any unapplied updates as needed.
     *
     * <p>
     * Note this implies the database initialization must initialize the database to its current, up-to-date state
     * (with respect to the set of updates), not its original, pre-update state.
     *
     * <p>
     * The database initialization step, and each of the update steps, is performed within its own transaction.
     *
     * @param database the database to initialize (if necessary) and update
     * @throws Exception if an update fails
     * @throws IllegalStateException if this instance is not configured to {@linkplain #setIgnoreUnrecognizedUpdates ignore
     * unrecognized updates} and an unrecognized update is registered in the update table as having already been applied
     * @throws IllegalArgumentException if two configured updates have the same name
     * @throws IllegalArgumentException if any configured update has a required predecessor which is not also a configured update
     *  (i.e., if the updates are not transitively closed under predecessors)
     */
    public synchronized void initializeAndUpdateDatabase(D database) throws Exception {

        // Log
        this.log.info("verifying database");

        // First, initialize if necessary
        this.applyToDatabase(database, new DatabaseAction<C>() {
            @Override
            public void apply(C connection) throws Exception {
                AbstractSchemaUpdater.this.applyInTransaction(connection, new DatabaseAction<C>() {

                    @Override
                    public void apply(C connection) throws Exception {

                        // Already initialized?
                        if (!AbstractSchemaUpdater.this.databaseNeedsInitialization(connection)) {
                            AbstractSchemaUpdater.this.log.debug("detected initialized database");
                            return;
                        }

                        // Initialize database
                        AbstractSchemaUpdater.this.log.info("uninitialized database detected - initializing now");
                        AbstractSchemaUpdater.this.initializeDatabase(connection);

                        // Record all schema updates as having already been applied
                        ArrayList<SchemaUpdate<C>> updateList = new ArrayList<SchemaUpdate<C>>(
                          AbstractSchemaUpdater.this.getUpdates());
                        Collections.sort(updateList, new UpdateByNameComparator());
                        for (SchemaUpdate<C> update : updateList) {
                            for (String name : AbstractSchemaUpdater.this.getUpdateNames(update))
                                AbstractSchemaUpdater.this.recordUpdateApplied(connection, name);
                        }
                    }
                });
            }
        });

        // Next, apply any new updates
        this.applyToDatabase(database, new DatabaseAction<C>() {
            @Override
            public void apply(C connection) throws Exception {
                AbstractSchemaUpdater.this.applySchemaUpdates(connection);
            }
        });

        // Done
        this.log.info("database verification complete");
    }

    /**
     * Determine if the database needs initialization.
     *
     * <p>
     * If so, {@link #initializeDatabase} will eventually be invoked.
     *
     * @param connection connection to the database
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract boolean databaseNeedsInitialization(C connection) throws Exception;

    /**
     * Initialize an uninitialized database.
     *
     * @param connection to the database
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void initializeDatabase(C connection) throws Exception;

    /**
     * Open a connection to the database. The returned connection will always eventually be {@linkplain #closeConnection closed}.
     *
     * @param database the database
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract C openConnection(D database) throws Exception;

    /**
     * Close a connection to the database.
     *
     * @param connection the connection to close
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void closeConnection(C connection) throws Exception;

    /**
     * Begin a transaction on the given connection.
     * The connection will always eventually either be
     * {@linkplain #commitTransaction committed} or {@linkplain #rollbackTransaction rolled back}.
     *
     * @param connection the connection on which to open the transaction
     * @return transaction handle
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract Object openTransaction(C connection) throws Exception;

    /**
     * Commit a previously opened transaction.
     *
     * @param handle transaction handle returned from {@link #openTransaction openTransaction()}.
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void commitTransaction(C connection, Object handle) throws Exception;

    /**
     * Abort a previously opened transaction.
     * This method will also be invoked if {@link #commitTransaction commitTransaction()} throws an exception.
     *
     * @param handle transaction handle returned from {@link #openTransaction openTransaction()}.
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void rollbackTransaction(C connection, Object handle) throws Exception;

    /**
     * Determine which updates have already been applied to the database.
     *
     * @param connection database connection
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract Set<String> getAppliedUpdateNames(C connection) throws Exception;

    /**
     * Record an update as having been applied to the database.
     *
     * @param connection database connection
     * @param name update name
     * @throws IllegalStateException if the update has already been recorded in the database
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void recordUpdateApplied(C connection, String name) throws Exception;

    /**
     * Determine the preferred ordering of two updates that do not have any predecessor constraints
     * (including implied indirect constraints) between them.
     *
     * <p>
     * The {@link Comparator} returned by the implementation in {@link AbstractSchemaUpdater} simply sorts updates by name.
     * Subclasses may override if necessary.
     *
     * @return a {@link Comparator} that sorts incomparable updates in the order they should be applied
     */
    protected Comparator<SchemaUpdate<C>> getOrderingTieBreaker() {
        return new UpdateByNameComparator();
    }

    /**
     * Generate the update name for one action within a multi-action update.
     *
     * <p>
     * The implementation in {@link AbstractSchemaUpdater} just adds a suffix using {@code index + 1}, padded to
     * 5 digits, producing names like {@code name-00001}, {@code name-00002}, etc.
     *
     * @param update the schema update
     * @param index the index of the action (zero based)
     * @see SchemaUpdate#isSingleAction
     */
    protected String generateMultiUpdateName(SchemaUpdate update, int index) {
        return String.format("%s-%05d", update.getName(), index + 1);
    }

    /**
     * Execute a database action. All database operations in {@link AbstractSchemaUpdater} are performed via this method;
     * subclasses should follow this pattern.
     *
     * <p>
     * The implementation in {@link AbstractSchemaUpdater} simply invokes {@link DatabaseAction#apply action.apply()};
     * subclasses may override if desired.
     *
     * @throws Exception if an error occurs while accessing the database
     */
    protected void apply(C connection, DatabaseAction<C> action) throws Exception {
        action.apply(connection);
    }

    /**
     * Execute a database action. A new connection will be created, used, and closed.
     * Delegates to {@link #apply apply()}.
     *
     * @throws Exception if an error occurs while accessing the database
     */
    protected void applyToDatabase(D database, DatabaseAction<C> action) throws Exception {
        C connection = this.openConnection(database);
        try {
            this.apply(connection, action);
        } finally {
            this.closeConnection(connection);
        }
    }

    /**
     * Execute the given database action within a transaction, using the given connection.
     * A new transaction will be created, used, and closed.
     * Delegates to {@link #apply apply()}.
     *
     * @throws Exception if an error occurs while accessing the database
     */
    protected void applyInTransaction(C connection, final DatabaseAction<C> action) throws Exception {
        Object handle = AbstractSchemaUpdater.this.openTransaction(connection);
        boolean success = false;
        try {
            this.apply(connection, action);
            AbstractSchemaUpdater.this.commitTransaction(connection, handle);
            success = true;
        } finally {
            if (!success)
                AbstractSchemaUpdater.this.rollbackTransaction(connection, handle);
        }
    }

    /**
     * Apply schema updates to an initialized database.
     */
    private void applySchemaUpdates(final C connection) throws Exception {

        // Sanity check
        final HashSet<SchemaUpdate<C>> allUpdates = new HashSet<SchemaUpdate<C>>(this.getUpdates());
        if (allUpdates == null)
            throw new IllegalArgumentException("no updates configured");

        // Create mapping from update name to update; multiple updates will have multiple names
        TreeMap<String, SchemaUpdate<C>> updateMap = new TreeMap<String, SchemaUpdate<C>>();
        for (SchemaUpdate<C> update : allUpdates) {
            for (String updateName : this.getUpdateNames(update)) {
                if (updateMap.put(updateName, update) != null)
                    throw new IllegalArgumentException("duplicate schema update name `" + updateName + "'");
            }
        }
        this.log.debug("these are all known schema updates: " + updateMap.keySet());

        // Verify updates are transitively closed under predecessor constraints
        for (SchemaUpdate<C> update : allUpdates) {
            for (SchemaUpdate<C> predecessor : update.getRequiredPredecessors()) {
                if (!allUpdates.contains(predecessor)) {
                    throw new IllegalArgumentException("schema update `" + update.getName()
                      + "' has a required predecessor named `" + predecessor.getName() + "' that is not a configured update");
                }
            }
        }

        // Sort updates in the order we should to apply them
        List<SchemaUpdate<C>> updateList = new TopologicalSorter<SchemaUpdate<C>>(allUpdates,
          new SchemaUpdateEdgeLister<C>(), this.getOrderingTieBreaker()).sortEdgesReversed();

        // Determine which updates have already been applied
        Set<String> appliedUpdateNames = this.getAppliedUpdateNames(connection);
        this.log.debug("these are the already-applied schema updates: " + appliedUpdateNames);

        // Check whether any unknown updates have been applied
        TreeSet<String> unknownUpdateNames = new TreeSet<String>(appliedUpdateNames);
        unknownUpdateNames.removeAll(updateMap.keySet());
        if (!unknownUpdateNames.isEmpty()) {
            if (!this.isIgnoreUnrecognizedUpdates()) {
                throw new IllegalStateException(unknownUpdateNames.size()
                  + " unrecognized update(s) have already been applied: " + unknownUpdateNames);
            }
            this.log.info("ignoring " + unknownUpdateNames.size()
              + " unrecognized update(s) already applied: " + unknownUpdateNames);
        }

        // Remove the already-applied updates
        updateMap.keySet().removeAll(appliedUpdateNames);
        HashSet<SchemaUpdate<C>> remainingUpdates = new HashSet<SchemaUpdate<C>>(updateMap.values());
        for (Iterator<SchemaUpdate<C>> i = updateList.iterator(); i.hasNext(); ) {
            if (!remainingUpdates.contains(i.next()))
                i.remove();
        }

        // Now are any updates needed?
        if (updateList.isEmpty()) {
            this.log.info("no schema updates are required");
            return;
        }

        // Log which updates we're going to apply
        final LinkedHashSet<String> remainingUpdateNames = new LinkedHashSet<String>(updateMap.size());
        for (SchemaUpdate<C> update : updateList) {
            ArrayList<String> updateNames = this.getUpdateNames(update);
            updateNames.removeAll(appliedUpdateNames);
            remainingUpdateNames.addAll(updateNames);
        }
        this.log.info("applying " + remainingUpdateNames.size() + " schema update(s): " + remainingUpdateNames);

        // Apply updates
        for (SchemaUpdate<C> nextUpdate : updateList) {
            UpdateHandler updateHandler = new UpdateHandler(nextUpdate) {
                @Override
                protected void handleEmptyUpdate(SchemaUpdate<C> update) throws Exception {
                    assert remainingUpdateNames.contains(update.getName());
                    AbstractSchemaUpdater.this.applyAndRecordUpdate(connection, update.getName(), null);
                }
                @Override
                protected void handleSingleUpdate(SchemaUpdate<C> update, DatabaseAction<C> action) throws Exception {
                    assert remainingUpdateNames.contains(update.getName());
                    AbstractSchemaUpdater.this.applyAndRecordUpdate(connection, update.getName(), action);
                }
                @Override
                protected void handleSingleMultiUpdate(SchemaUpdate<C> update, final List<DatabaseAction<C>> actions)
                  throws Exception {
                    assert remainingUpdateNames.contains(update.getName());
                    AbstractSchemaUpdater.this.applyAndRecordUpdate(connection, update.getName(), new DatabaseAction<C>() {
                        @Override
                        public void apply(C connection) throws Exception {
                            for (DatabaseAction<C> action : actions)
                                AbstractSchemaUpdater.this.apply(connection, action);
                        }
                    });
                }
                @Override
                protected void handleMultiUpdate(SchemaUpdate<C> update, DatabaseAction<C> action, int index) throws Exception {
                    String updateName = AbstractSchemaUpdater.this.generateMultiUpdateName(update, index);
                    if (!remainingUpdateNames.contains(updateName))                 // a partially completed multi-update
                        return;
                    AbstractSchemaUpdater.this.applyAndRecordUpdate(connection, updateName, action);
                }
            };
            updateHandler.process();
        }
    }

    // Get all update names, expanding multi-updates as necessary
    private ArrayList<String> getUpdateNames(SchemaUpdate<C> update) throws Exception {
        final ArrayList<String> names = new ArrayList<String>();
        UpdateHandler updateHandler = new UpdateHandler(update) {
            @Override
            protected void handleSingleUpdate(SchemaUpdate<C> update, DatabaseAction<C> action) {
                names.add(update.getName());
            }
            @Override
            protected void handleMultiUpdate(SchemaUpdate<C> update, DatabaseAction<C> action, int index) {
                names.add(AbstractSchemaUpdater.this.generateMultiUpdateName(update, index));
            }
        };
        updateHandler.process();
        return names;
    }

    // Apply and record an update, all within a single transaction
    private void applyAndRecordUpdate(C connection, final String name, final DatabaseAction<C> action) throws Exception {
        this.applyInTransaction(connection, new DatabaseAction<C>() {
            @Override
            public void apply(C connection) throws Exception {
                if (action != null) {
                    AbstractSchemaUpdater.this.log.info("applying schema update `" + name + "'");
                    AbstractSchemaUpdater.this.apply(connection, action);
                } else
                    AbstractSchemaUpdater.this.log.info("recording empty schema update `" + name + "'");
                AbstractSchemaUpdater.this.recordUpdateApplied(connection, name);
            }
        });
    }

    // Adapter class for handling updates of various types
    private class UpdateHandler {

        private final SchemaUpdate<C> update;
        private final List<DatabaseAction<C>> actions;

        public UpdateHandler(SchemaUpdate<C> update) {
            this.update = update;
            this.actions = update.getDatabaseActions();
        }

        public final void process() throws Exception {
            switch (this.actions.size()) {
            case 0:
                this.handleEmptyUpdate(this.update);
                break;
            case 1:
                this.handleSingleUpdate(this.update, this.actions.get(0));
                break;
            default:
                if (update.isSingleAction()) {
                    this.handleSingleMultiUpdate(this.update, actions);
                    break;
                } else {
                    int index = 0;
                    for (DatabaseAction<C> action : this.actions)
                        this.handleMultiUpdate(this.update, action, index++);
                }
                break;
            }
        }

        protected void handleEmptyUpdate(SchemaUpdate<C> update) throws Exception {
            this.handleSingleUpdate(update, null);
        }

        protected void handleSingleUpdate(SchemaUpdate<C> update, DatabaseAction<C> action) throws Exception {
        }

        protected void handleSingleMultiUpdate(SchemaUpdate<C> update, List<DatabaseAction<C>> actions) throws Exception {
            this.handleSingleUpdate(update, null);
        }

        protected void handleMultiUpdate(SchemaUpdate<C> update, DatabaseAction<C> action, int index) throws Exception {
        }
    }

    // Sorts updates by name
    private class UpdateByNameComparator implements Comparator<SchemaUpdate<C>> {

        @Override
        public int compare(SchemaUpdate<C> update1, SchemaUpdate<C> update2) {
            return update1.getName().compareTo(update2.getName());
        }
    }
}

