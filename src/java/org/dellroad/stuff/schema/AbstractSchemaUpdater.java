
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
 * @param <T> database transaction type
 */
public abstract class AbstractSchemaUpdater<D, T> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private Collection<? extends SchemaUpdate<T>> updates;
    private boolean ignoreUnrecognizedUpdates;

    /**
     * Get the configured updates. This property is required.
     *
     * @return configured updates
     * @see #setUpdates setUpdates()
     */
    public Collection<? extends SchemaUpdate<T>> getUpdates() {
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
    public void setUpdates(Collection<? extends SchemaUpdate<T>> updates) {
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
     * as having been applied; otherwise, {@linkplain #apply apply} any {@linkplain #getAppliedUpdateNames unapplied updates}
     * as needed.
     *
     * <p>
     * Note this implies the database initialization must initialize the database to its current, up-to-date state
     * (with respect to the set of all available updates), not its original, pre-update state.
     *
     * <p>
     * The database initialization step, and each of the update steps, is {@linkplain #applyInTransaction performed within
     * its own transaction}.
     *
     * @param database the database to initialize (if necessary) and update
     * @return true if successful, false if database initialization was needed but not applied
     * @throws Exception if an update fails
     * @throws IllegalStateException if this instance is not configured to {@linkplain #setIgnoreUnrecognizedUpdates ignore
     * unrecognized updates} and an unrecognized update has already been applied
     * @throws IllegalArgumentException if two configured updates have the same name
     * @throws IllegalArgumentException if any configured update has a required predecessor which is not also a configured update
     *  (i.e., if the updates are not transitively closed under predecessors)
     */
    public synchronized boolean initializeAndUpdateDatabase(final D database) throws Exception {

        // Log
        this.log.info("verifying database " + database);

        // First, initialize if necessary
        final boolean[] initialized = new boolean[1];
        this.applyInTransaction(database, new DatabaseAction<T>() {
            @Override
            public void apply(T transaction) throws Exception {

                // Already initialized?
                if (!AbstractSchemaUpdater.this.databaseNeedsInitialization(transaction)) {
                    AbstractSchemaUpdater.this.log.debug("detected already-initialized database " + database);
                    initialized[0] = true;
                    return;
                }

                // Initialize database
                AbstractSchemaUpdater.this.log.info("uninitialized database detected; initializing " + database);
                initialized[0] = AbstractSchemaUpdater.this.initializeDatabase(transaction);
                if (!initialized[0])
                    return;

                // Record all schema updates as having already been applied
                for (String updateName : AbstractSchemaUpdater.this.getAllUpdateNames())
                    AbstractSchemaUpdater.this.recordUpdateApplied(transaction, updateName);
            }
        });

        // Was database initialized?
        if (!initialized[0]) {
            this.log.info("database verification aborted because database was not initialized: " + database);
            return false;
        }

        // Next, apply any new updates
        this.applySchemaUpdates(database);

        // Done
        this.log.info("database verification completed for " + database);
        return true;
    }

    /**
     * Determine if the given schema update name is valid. Valid names are non-empty and
     * have no leading or trailing whitespace.
     */
    public static boolean isValidUpdateName(String updateName) {
        return updateName.length() > 0 && updateName.trim().length() == updateName.length();
    }

    /**
     * Determine if the database needs initialization.
     *
     * <p>
     * If so, {@link #initializeDatabase} will eventually be invoked.
     *
     * @param transaction open transaction
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract boolean databaseNeedsInitialization(T transaction) throws Exception;

    /**
     * Initialize an uninitialized database. This should create and initialize the database schema and content,
     * including whatever portion of that is used to track schema updates.
     *
     * @param transaction open transaction
     * @return true if database was initialized, false otherwise
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract boolean initializeDatabase(T transaction) throws Exception;

    /**
     * Begin a transaction on the given database.
     * The transaction will always eventually either be
     * {@linkplain #commitTransaction committed} or {@linkplain #rollbackTransaction rolled back}.
     *
     * @param database database
     * @return transaction handle
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract T openTransaction(D database) throws Exception;

    /**
     * Commit a previously opened transaction.
     *
     * @param transaction open transaction previously returned from {@link #openTransaction openTransaction()}
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void commitTransaction(T transaction) throws Exception;

    /**
     * Roll back a previously opened transaction.
     * This method will also be invoked if {@link #commitTransaction commitTransaction()} throws an exception.
     *
     * @param transaction open transaction previously returned from {@link #openTransaction openTransaction()}
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void rollbackTransaction(T transaction) throws Exception;

    /**
     * Determine which updates have already been applied to the database.
     *
     * @param transaction open transaction
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract Set<String> getAppliedUpdateNames(T transaction) throws Exception;

    /**
     * Record an update as having been applied to the database.
     *
     * @param transaction open transaction
     * @param name update name
     * @throws IllegalStateException if the update has already been recorded in the database
     * @throws Exception if an error occurs while accessing the database
     */
    protected abstract void recordUpdateApplied(T transaction, String name) throws Exception;

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
    protected Comparator<SchemaUpdate<T>> getOrderingTieBreaker() {
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
    protected String generateMultiUpdateName(SchemaUpdate<T> update, int index) {
        return String.format("%s-%05d", update.getName(), index + 1);
    }

    /**
     * Get the names of all updates including multi-action updates.
     */
    protected List<String> getAllUpdateNames() throws Exception {
        ArrayList<SchemaUpdate<T>> updateList = new ArrayList<SchemaUpdate<T>>(this.getUpdates());
        ArrayList<String> updateNameList = new ArrayList<String>(updateList.size());
        Collections.sort(updateList, new UpdateByNameComparator());
        for (SchemaUpdate<T> update : updateList)
            updateNameList.addAll(this.getUpdateNames(update));
        return updateNameList;
    }

    /**
     * Execute a database action within an existing transaction.
     *
     * <p>
     * All database operations in {@link AbstractSchemaUpdater} are performed via this method;
     * subclasses are encouraged to follow this pattern.
     *
     * <p>
     * The implementation in {@link AbstractSchemaUpdater} simply invokes {@link DatabaseAction#apply action.apply()};
     * subclasses may override if desired.
     *
     * @throws Exception if an error occurs while accessing the database
     */
    protected void apply(T transaction, DatabaseAction<T> action) throws Exception {
        action.apply(transaction);
    }

    /**
     * Execute a database action. A new transaction will be created, used, and closed.
     * Delegates to {@link #apply apply()} for the actual execution of the action.
     *
     * <p>
     * If the action or {@link #commitTransaction commitTransaction()} fails, the transaction
     * is {@linkplain #rollbackTransaction rolled back}.
     *
     * @throws Exception if an error occurs while accessing the database
     */
    protected void applyInTransaction(D database, DatabaseAction<T> action) throws Exception {
        T transaction = this.openTransaction(database);
        boolean success = false;
        try {
            this.apply(transaction, action);
            this.commitTransaction(transaction);
            success = true;
        } finally {
            if (!success)
                this.rollbackTransaction(transaction);
        }
    }

    /**
     * Apply schema updates to an initialized database.
     */
    private void applySchemaUpdates(D database) throws Exception {

        // Sanity check
        final HashSet<SchemaUpdate<T>> allUpdates = new HashSet<SchemaUpdate<T>>(this.getUpdates());
        if (allUpdates == null)
            throw new IllegalArgumentException("no updates configured");

        // Create mapping from update name to update; multiple updates will have multiple names
        TreeMap<String, SchemaUpdate<T>> updateMap = new TreeMap<String, SchemaUpdate<T>>();
        for (SchemaUpdate<T> update : allUpdates) {
            for (String updateName : this.getUpdateNames(update)) {
                if (!isValidUpdateName(updateName))
                    throw new IllegalArgumentException("illegal schema update name `" + updateName + "'");
                if (updateMap.put(updateName, update) != null)
                    throw new IllegalArgumentException("duplicate schema update name `" + updateName + "'");
            }
        }
        this.log.debug("these are all known schema updates: " + updateMap.keySet());

        // Verify updates are transitively closed under predecessor constraints
        for (SchemaUpdate<T> update : allUpdates) {
            for (SchemaUpdate<T> predecessor : update.getRequiredPredecessors()) {
                if (!allUpdates.contains(predecessor)) {
                    throw new IllegalArgumentException("schema update `" + update.getName()
                      + "' has a required predecessor named `" + predecessor.getName() + "' that is not a configured update");
                }
            }
        }

        // Sort updates in the order we should to apply them
        List<SchemaUpdate<T>> updateList = new TopologicalSorter<SchemaUpdate<T>>(allUpdates,
          new SchemaUpdateEdgeLister<T>(), this.getOrderingTieBreaker()).sortEdgesReversed();

        // Determine which updates have already been applied
        final HashSet<String> appliedUpdateNames = new HashSet<String>();
        this.applyInTransaction(database, new DatabaseAction<T>() {
            @Override
            public void apply(T transaction) throws Exception {
                appliedUpdateNames.addAll(AbstractSchemaUpdater.this.getAppliedUpdateNames(transaction));
            }
        });
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
        HashSet<SchemaUpdate<T>> remainingUpdates = new HashSet<SchemaUpdate<T>>(updateMap.values());
        for (Iterator<SchemaUpdate<T>> i = updateList.iterator(); i.hasNext(); ) {
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
        for (SchemaUpdate<T> update : updateList) {
            ArrayList<String> updateNames = this.getUpdateNames(update);
            updateNames.removeAll(appliedUpdateNames);
            remainingUpdateNames.addAll(updateNames);
        }
        this.log.info("applying " + remainingUpdateNames.size() + " schema update(s): " + remainingUpdateNames);

        // Apply and record each unapplied update
        for (SchemaUpdate<T> nextUpdate : updateList) {
            final RecordingUpdateHandler updateHandler = new RecordingUpdateHandler(nextUpdate, remainingUpdateNames);
            this.applyInTransaction(database, new DatabaseAction<T>() {
                @Override
                public void apply(T transaction) throws Exception {
                    updateHandler.process(transaction);
                }
            });
        }
    }

    // Get all update names, expanding multi-updates as necessary
    private ArrayList<String> getUpdateNames(SchemaUpdate<T> update) throws Exception {
        final ArrayList<String> names = new ArrayList<String>();
        UpdateHandler updateHandler = new UpdateHandler(update) {
            @Override
            protected void handleSingleUpdate(T transaction, DatabaseAction<T> action) {
                names.add(this.update.getName());
            }
            @Override
            protected void handleMultiUpdate(T transaction, DatabaseAction<T> action, int index) {
                names.add(AbstractSchemaUpdater.this.generateMultiUpdateName(this.update, index));
            }
        };
        updateHandler.process(null);
        return names;
    }

    // Apply and record an update, all within a single transaction
    private void applyAndRecordUpdate(T transaction, String name, final DatabaseAction<T> action) throws Exception {
        if (action != null) {
            this.log.info("applying schema update `" + name + "'");
            this.apply(transaction, action);
        } else
            this.log.info("recording empty schema update `" + name + "'");
        this.recordUpdateApplied(transaction, name);
    }

    private class RecordingUpdateHandler extends UpdateHandler {

        private final Set<String> remainingUpdateNames;

        public RecordingUpdateHandler(SchemaUpdate<T> update, Set<String> remainingUpdateNames) {
            super(update);
            this.remainingUpdateNames = remainingUpdateNames;
        }

        @Override
        protected void handleEmptyUpdate(T transaction) throws Exception {
            assert this.remainingUpdateNames.contains(this.update.getName());
            AbstractSchemaUpdater.this.applyAndRecordUpdate(transaction, this.update.getName(), null);
        }

        @Override
        protected void handleSingleUpdate(T transaction, DatabaseAction<T> action) throws Exception {
            assert this.remainingUpdateNames.contains(this.update.getName());
            AbstractSchemaUpdater.this.applyAndRecordUpdate(transaction, this.update.getName(), action);
        }

        @Override
        protected void handleSingleMultiUpdate(T transaction, final List<? extends DatabaseAction<T>> actions)
          throws Exception {
            assert this.remainingUpdateNames.contains(this.update.getName());
            AbstractSchemaUpdater.this.applyAndRecordUpdate(transaction, this.update.getName(), new DatabaseAction<T>() {
                @Override
                public void apply(T transaction) throws Exception {
                    for (DatabaseAction<T> action : actions)
                        AbstractSchemaUpdater.this.apply(transaction, action);
                }
            });
        }

        @Override
        protected void handleMultiUpdate(T transaction, DatabaseAction<T> action, int index) throws Exception {
            String updateName = AbstractSchemaUpdater.this.generateMultiUpdateName(this.update, index);
            if (!this.remainingUpdateNames.contains(updateName))                 // a partially completed multi-update
                return;
            AbstractSchemaUpdater.this.applyAndRecordUpdate(transaction, updateName, action);
        }
    }

    // Adapter class for handling updates of various types
    private class UpdateHandler {

        protected final SchemaUpdate<T> update;

        private final List<? extends DatabaseAction<T>> actions;

        public UpdateHandler(SchemaUpdate<T> update) {
            this.update = update;
            this.actions = update.getDatabaseActions();
        }

        public final void process(T transaction) throws Exception {
            switch (this.actions.size()) {
            case 0:
                this.handleEmptyUpdate(transaction);
                break;
            case 1:
                this.handleSingleUpdate(transaction, this.actions.get(0));
                break;
            default:
                if (update.isSingleAction()) {
                    this.handleSingleMultiUpdate(transaction, actions);
                    break;
                } else {
                    int index = 0;
                    for (DatabaseAction<T> action : this.actions)
                        this.handleMultiUpdate(transaction, action, index++);
                }
                break;
            }
        }

        protected void handleEmptyUpdate(T transaction) throws Exception {
            this.handleSingleUpdate(transaction, null);
        }

        protected void handleSingleUpdate(T transaction, DatabaseAction<T> action) throws Exception {
        }

        protected void handleSingleMultiUpdate(T transaction, List<? extends DatabaseAction<T>> actions) throws Exception {
            this.handleSingleUpdate(transaction, null);
        }

        protected void handleMultiUpdate(T transaction, DatabaseAction<T> action, int index) throws Exception {
        }
    }

    // Sorts updates by name
    private class UpdateByNameComparator implements Comparator<SchemaUpdate<T>> {

        @Override
        public int compare(SchemaUpdate<T> update1, SchemaUpdate<T> update2) {
            return update1.getName().compareTo(update2.getName());
        }
    }
}

