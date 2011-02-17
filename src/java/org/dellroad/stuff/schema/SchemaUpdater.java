
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.dellroad.stuff.graph.TopologicalSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database schema updater that manages initializing and updates to a database schema.
 *
 * <p>
 * The primary method is {@link #initializeAndUpdateDatabase initializeAndUpdateDatabase()}, which will:
 * <ul>
 * <li> Automatically initialize an empty database when necessary;</li>
 * <li> Automatically apply configured {@link SchemaUpdate}s as needed, ordered properly according to
 *      their predecessor constraints; and</li>
 * <li> Automatically keep track of which {@link SchemaUpdate}s have already been applied across restarts.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Required properties are the {@link #setDatabaseInitialization database initialization},
 * {@link #setUpdateTableInitialization update table initialization}, and the {@link #setUpdates updates} themselves.
 * </p>
 *
 * <p>
 * Applied updates are recorded in a special <i>update table</i>, which contains two columns: one for the unique
 * {@link SchemaUpdate#getName update name} and one for a timestamp. The update table and column names
 * are configurable via {@link #setUpdateTableName setUpdateTableName()},
 * {@link #setUpdateTableNameColumn setUpdateTableNameColumn()}, and {@link #setUpdateTableTimeColumn setUpdateTableTimeColumn()}.
 * </p>
 *
 * <p>
 * By default, this class detects a completely uninitialized database by the absence of the update table itself
 * in the schema (see {@link #databaseNeedsInitialization databaseNeedsInitialization()}).
 * When an uninitialized database is encountered, the configured {@link #setDatabaseInitialization database initialization}
 * and {@link #setUpdateTableInitialization update table initialization} actions are applied first to initialize
 * the database schema.
 * </p>
 */
public class SchemaUpdater {

    /**
     * Default nefault name of the table that tracks schema updates, <code>{@value}</code>.
     */
    public static final String DEFAULT_UPDATE_TABLE_NAME = "SchemaUpdate";

    /**
     * Default name of the column in the updates table holding the unique update name, <code>{@value}</code>.
     */
    public static final String DEFAULT_UPDATE_TABLE_NAME_COLUMN = "updateName";

    /**
     * Default name of the column in the updates table holding the update's time applied, <code>{@value}</code>.
     */
    public static final String DEFAULT_UPDATE_TABLE_TIME_COLUMN = "updateTime";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String updateTableName = DEFAULT_UPDATE_TABLE_NAME;
    private String updateTableNameColumn = DEFAULT_UPDATE_TABLE_NAME_COLUMN;
    private String updateTableTimeColumn = DEFAULT_UPDATE_TABLE_TIME_COLUMN;

    private Collection<SchemaUpdate> updates;
    private DatabaseAction databaseInitialization;
    private DatabaseAction updateTableInitialization;
    private boolean ignoreUnrecognizedUpdates;

    /**
     * Get the name of the table that keeps track of applied updates.
     *
     * @see #setUpdateTableName setUpdateTableName()
     */
    public String getUpdateTableName() {
        return this.updateTableName;
    }

    /**
     * Set the name of the table that keeps track of applied updates.
     * Default value is {@link #DEFAULT_UPDATE_TABLE_NAME}.
     *
     * <p>
     * This name must be consistent with the {@link #setUpdateTableInitialization update table initialization}.
     */
    public void setUpdateTableName(String updateTableName) {
        this.updateTableName = updateTableName;
    }

    /**
     * Get the name of the update name column in the table that keeps track of applied updates.
     *
     * @see #setUpdateTableNameColumn setUpdateTableNameColumn()
     */
    public String getUpdateTableNameColumn() {
        return this.updateTableNameColumn;
    }

    /**
     * Set the name of the update name column in the table that keeps track of applied updates.
     * Default value is {@link #DEFAULT_UPDATE_TABLE_NAME_COLUMN}.
     *
     * <p>
     * This name must be consistent with the {@link #setUpdateTableInitialization update table initialization}.
     */
    public void setUpdateTableNameColumn(String updateTableNameColumn) {
        this.updateTableNameColumn = updateTableNameColumn;
    }

    /**
     * Get the name of the update timestamp column in the table that keeps track of applied updates.
     *
     * @see #setUpdateTableTimeColumn setUpdateTableTimeColumn()
     */
    public String getUpdateTableTimeColumn() {
        return this.updateTableTimeColumn;
    }

    /**
     * Set the name of the update timestamp column in the table that keeps track of applied updates.
     * Default value is {@link #DEFAULT_UPDATE_TABLE_TIME_COLUMN}.
     *
     * <p>
     * This name must be consistent with the {@link #setUpdateTableInitialization update table initialization}.
     */
    public void setUpdateTableTimeColumn(String updateTableTimeColumn) {
        this.updateTableTimeColumn = updateTableTimeColumn;
    }

    /**
     * Get the update table initialization.
     *
     * @see #setUpdateTableInitialization setUpdateTableInitialization()
     */
    public DatabaseAction getUpdateTableInitialization() {
        return this.updateTableInitialization;
    }

    /**
     * Configure how the update table itself gets initialized. This update is run when no update table found,
     * which (we assume) implies an empty database with no tables or content. This is a required property.
     *
     * <p>
     * This initialization should create the update table where the name column is the primary key.
     * The name column must have a length limit greater than or equal to the longest schema update name.
     *
     * <p>
     * The table and column names must be consistent with the values configured via
     * {@link #setUpdateTableName setUpdateTableName()}, {@link #setUpdateTableNameColumn setUpdateTableNameColumn()},
     * and {@link #setUpdateTableTimeColumn setUpdateTableTimeColumn()}.
     *
     * @param updateTableInitialization update table schema initialization
     * @see #setUpdateTableName setUpdateTableName()
     * @see #setUpdateTableNameColumn setUpdateTableNameColumn()
     * @see #setUpdateTableTimeColumn setUpdateTableTimeColumn()
     */
    public void setUpdateTableInitialization(DatabaseAction updateTableInitialization) {
        this.updateTableInitialization = updateTableInitialization;
    }

    /**
     * Get the empty database initialization.
     *
     * @see #setDatabaseInitialization setDatabaseInitialization()
     */
    public DatabaseAction getDatabaseInitialization() {
        return this.databaseInitialization;
    }

    /**
     * Configure how an empty database gets initialized. This is a required property.
     *
     * <p>
     * This update is run when no update table found, which (we assume) implies an empty database with no tables or content.
     * Typically this contains the SQL script that gets automatically generated by your favorite schema generation tool.
     *
     * <p>
     * This script is expected to initialize the database schema (i.e., creating all the tables) so that
     * when completed the database is "up to date" with respect to the configured schema updates.
     * That is, when this action completes, we assume all updates have already been (implicitly) applied
     * (and they will be recorded as such).
     *
     * <p>
     * Note this script is <i>not</i> expected to create the update table that tracks schema updates;
     * that function is handled by the {@link #setUpdateTableInitialization update table initialization}.
     *
     * @param databaseInitialization application database schema initialization
     */
    public void setDatabaseInitialization(DatabaseAction databaseInitialization) {
        this.databaseInitialization = databaseInitialization;
    }

    /**
     * Get the configured schema updates.
     *
     * @return configured schema updates
     * @see #setUpdates setUpdates()
     */
    public Collection<SchemaUpdate> getUpdates() {
        return this.updates;
    }

    /**
     * Configure the schema updates.
     * This should be the set of all updates that may need to be applied to the database.
     *
     * <p>
     * For any given application, ideally this set should be "write only" in the sense that once an update is added to the set
     * and applied to one or more actual databases, the update and its name should thereafter never change. Otherwise,
     * it would be possible for different databases to have inconsistent schemas even though the same updates were recorded.
     *
     * <p>
     * Furthermore, if not configured to {@link #setIgnoreUnrecognizedUpdates ignore unrecognized updates already applied}
     * (the default behavior), then updates must never be removed from this set as the application evolves;
     * see {@link #setIgnoreUnrecognizedUpdates} for more information on the rationale.
     *
     * @param updates all schema updates; each update must have a unique {@link SchemaUpdate#getName name}.
     * @see #getUpdates
     * @see #setIgnoreUnrecognizedUpdates
     */
    public void setUpdates(Collection<SchemaUpdate> updates) {
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
     * This method applies the following logic: if the {@link #databaseNeedsInitialization database needs initialization},
     * then {@link #initializeDatabase initialize} the database; then, {@link #applySchemaUpdates apply schema updates} as needed.
     * The database initialization step and each schema update is applied within its own transaction.
     *
     * @throws SQLException if an update fails
     * @throws IllegalStateException if the database needs initialization and either the
     *  {@link #setDatabaseInitialization database initialization} or
     *  the {@link #setUpdateTableInitialization update table initialization} has not been configured
     * @throws IllegalStateException if this instance is not configured to {@link #setIgnoreUnrecognizedUpdates ignore
     * unrecognized updates} and an unrecognized update is registered in the update table as having already been applied
     * @throws IllegalArgumentException if two configured updates have the same name
     * @throws IllegalArgumentException if any configured update has a required predecessor which is not also a configured update
     *  (i.e., if the updates are not transitively closed under predecessors)
     */
    public void initializeAndUpdateDatabase(DataSource dataSource) throws SQLException {
        log.info("verifying database");
        if (this.databaseNeedsInitialization(dataSource))
            this.initializeDatabase(dataSource);
        this.applySchemaUpdates(dataSource);
        log.info("database verification complete");
    }

    /**
     * Determine if the database needs initialization.
     *
     * <p>
     * The implementation in {@link SchemaUpdater} simply invokes <code>SELECT COUNT(*) FROM <i>UPDATETABLE</i></code>
     * and checks for success or failure. Subclasses may wish to override with a more discriminating implementation.
     * This implementation does not distguish {@link SQLException}s caused by a missing update table from other causes.
     */
    public boolean databaseNeedsInitialization(DataSource dataSource) throws SQLException {
        Connection c = dataSource.getConnection();
        try {
            Statement s = c.createStatement();
            try {
                ResultSet resultSet = s.executeQuery("SELECT COUNT(*) FROM " + this.getUpdateTableName());
                long numUpdates = resultSet.getLong(1);
                log.info("detected already initialized database, with " + numUpdates + " update(s) already applied");
                return false;
            } finally {
                s.close();
            }
        } catch (SQLException e) {
            log.warn("detected uninitialized database: update table `" + this.getUpdateTableName() + "' not found: " + e);
            return true;
        } finally {
            c.close();
        }
    }

    /**
     * Apply a {@link DatabaseAction} to a {@link DataSource}.
     *
     * <p>
     * The implementation in {@link SchemaUpdater} invokes {@link ActionUtils#applyAction}.
     */
    protected void applyAction(DataSource dataSource, DatabaseAction action) throws SQLException {
        ActionUtils.applyAction(dataSource, action);
    }

    /**
     * Apply a {@link DatabaseAction} to a {@link DataSource} within a transaction.
     *
     * <p>
     * The implementation in {@link SchemaUpdater} invokes {@link ActionUtils#applyInTransaction}.
     */
    protected void applyInTransaction(DataSource dataSource, DatabaseAction action) throws SQLException {
        ActionUtils.applyInTransaction(dataSource, action);
    }

    /**
     * Record an update as having been applied.
     *
     * <p>
     * The implementation in {@link SchemaUpdater} does the standard JDBC thing using an INSERT statement
     * into the update table.
     */
    protected void recordUpdateApplied(Connection c, SchemaUpdate update) throws SQLException {
        PreparedStatement s = c.prepareStatement("INSERT INTO " + this.getUpdateTableName()
          + " (" + this.getUpdateTableNameColumn() + ", " + this.getUpdateTableTimeColumn() + ") VALUES (?, ?)");
        try {
            s.setString(1, update.getName());
            s.setDate(2, new java.sql.Date(new Date().getTime()));
            int rows = s.executeUpdate();
            if (rows != 1) {
                throw new IllegalStateException("expected 1 row returned from INSERT but got zero for update `"
                  + update.getName() + "'");
            }
        } finally {
            s.close();
        }
    }

    /**
     * Determine which updates have already been applied.
     *
     * <p>
     * The implementation in {@link SchemaUpdater} does the standard JDBC thing using a SELECT statement
     * from the update table.
     */
    protected HashSet<String> getAppliedUpdateNames(Connection c) throws SQLException {
        Statement s = c.createStatement();
        try {
            HashSet<String> updateNames = new HashSet<String>();
            ResultSet resultSet = s.executeQuery("SELECT " + this.getUpdateTableNameColumn()
              + " FROM " + this.getUpdateTableName());
            while (resultSet.next())
                updateNames.add(resultSet.getString(1));
            return updateNames;
        } finally {
            s.close();
        }
    }

    /**
     * Get the preferred ordering of two updates that do not have any predecessor constraints
     * (including implied indirect constraints) between them.
     *
     * <p>
     * The {@link Comparator} returned by the implementation in {@link SchemaUpdater} simply sorts updates by name.
     */
    protected Comparator<SchemaUpdate> getOrderingTieBreaker() {
        return new Comparator<SchemaUpdate>() {
            @Override
            public int compare(SchemaUpdate update1, SchemaUpdate update2) {
                return update1.getName().compareTo(update2.getName());
            }
        };
    }

    // Initialize the database
    private void initializeDatabase(final DataSource dataSource) throws SQLException {

        // Sanity check
        if (this.getDatabaseInitialization() == null)
            throw new IllegalArgumentException("database needs initialization but no database initialization is configured");
        if (this.getUpdateTableInitialization() == null)
            throw new IllegalArgumentException("database needs initialization but no update table initialization is configured");

        // Initialize schema within a transaction
        this.applyInTransaction(dataSource, new DatabaseAction() {

            @Override
            public void apply(Connection c) throws SQLException {

                // Initialize application schema
                SchemaUpdater.this.log.info("intializing database schema");
                SchemaUpdater.this.applyAction(dataSource, SchemaUpdater.this.getDatabaseInitialization());

                // Initialize update table
                SchemaUpdater.this.log.info("intializing update table");
                SchemaUpdater.this.applyAction(dataSource, SchemaUpdater.this.getUpdateTableInitialization());

                // Record all schema updates as having already been applied
                for (SchemaUpdate update : SchemaUpdater.this.getUpdates())
                    SchemaUpdater.this.recordUpdateApplied(c, update);
            }
        });
    }

    // Apply schema updates
    private void applySchemaUpdates(DataSource dataSource) throws SQLException {

        // Sanity check
        if (this.getUpdates() == null)
            throw new IllegalArgumentException("schema updates set is not configured");

        // Create map of updates by name
        HashMap<String, SchemaUpdate> updateMap = new HashMap<String, SchemaUpdate>(this.getUpdates().size());
        for (SchemaUpdate update : this.getUpdates()) {
            if (updateMap.put(update.getName(), update) != null)
                throw new IllegalArgumentException("duplicate schema update name `" + update.getName() + "'");
        }

        // Verify updates are transitively closed under predecessor constraints
        for (SchemaUpdate update : this.getUpdates()) {
            for (SchemaUpdate predecessor : update.getRequiredPredecessors()) {
                if (!this.getUpdates().contains(predecessor)) {
                    throw new IllegalArgumentException("schema update `" + update.getName()
                      + "' has a required predecessor `" + predecessor.getName() + "' that is not a configured update");
                }
            }
        }

        // Determine which updates have already been applied
        Set<String> appliedUpdates;
        Connection c = dataSource.getConnection();
        try {
            appliedUpdates = this.getAppliedUpdateNames(c);
        } finally {
            c.close();
        }

        // Check whether any unknown updates have been applied; remove already-applied updates from our update map
        HashSet<String> unknown = new HashSet<String>();
        for (String updateName : appliedUpdates) {
            if (updateMap.remove(updateName) == null)
                unknown.add(updateName);
        }
        if (!unknown.isEmpty()) {
            if (!this.isIgnoreUnrecognizedUpdates())
                throw new IllegalStateException(unknown.size() + " unrecognized update(s) have already been applied: " + unknown);
            this.log.info("ignoring " + unknown.size() + " unrecognized update(s) already applied");
        }

        // Any updates needed?
        if (updateMap.isEmpty()) {
            this.log.info("no schema updates are required");
            return;
        }

        // Log result
        if (updates.isEmpty()) {
            this.log.info("no schema updates are required");
            return;
        }

        // Sort updates in the order we want to apply them
        List<SchemaUpdate> updateList = new TopologicalSorter<SchemaUpdate>(
          updateMap.values(), new SchemaUpdateEdgeLister(), this.getOrderingTieBreaker()).sortEdgesReversed();

        // Show the updates we're going to apply (in the right order)
        List<String> updateNames = new ArrayList<String>(updateList.size());
        for (SchemaUpdate update : updateList)
            updateNames.add(update.getName());
        this.log.info("applying required schema updates: " + updateNames);

        // Apply updates
        for (SchemaUpdate update0 : updateList) {
            final SchemaUpdate update = update0;
            this.applyInTransaction(dataSource, new DatabaseAction() {

                @Override
                public void apply(Connection c) throws SQLException {
                    SchemaUpdater.this.log.info("applying schema update `" + update.getName() + "'");
                    update.apply(c);
                    SchemaUpdater.this.recordUpdateApplied(c, update);
                }
            });
        }
    }
}

