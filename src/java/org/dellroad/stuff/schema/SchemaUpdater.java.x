
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database schema updater that manages initializing and updates to a database schema, automatically
 * initialzing the database schema when empty and ordering and applying updates according to their
 * predecessor constraints, keeping track of which updates have already been applied.
 *
 * <p>
 * Updates that have been applied are recorded in a special <i>update table</i>, which contains
 * two columns for the unique update name and application timestamp. The table and column names
 * are configurable via {@link #setUpdateTableName setUpdateTableName()}, {@link #setUpdateNameColumn setUpdateNameColumn()},
 * and {@link #setUpdateTimeColumn setUpdateTimeColumn()}.
 *
 * <p>
 * This class detects a completely uninitialized database by the absence of the update table.
 * With an uninitialized database, the supplied {@link #setDatabaseInitialization database initialization}
 * and {@link #setUpdateTableInitialization update table initialization} updates are applied first.
 */
public class SchemaUpdater implements InitializingBean {

    /**
     * Defalt nefault name of the table that tracks schema updates, <code>{@value}</code>.
     */
    public static final String DEFAULT_UPDATE_TABLE_NAME = "SchemaUpdate";

    /**
     * Defalt name of the column in {@link #TABLE_NAME} holding the unique update name, <code>{@value}</code>.
     */
    public static final String DEFAULT_UPDATE_NAME_COLUMN = "updateName";

    /**
     * Defalt name of the column in the updates table holding the update's time applied, <code>{@value}</code>.
     */
    public static final String DEFAULT_UPDATE_TIME_COLUMN = "updateTime";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String updateTableName = DEFAULT_UPDATE_TABLE_NAME;
    private String updateNameColumn = DEFAULT_UPDATE_NAME_COLUMN;
    private String updateTimeColumn = DEFAULT_UPDATE_TIME_COLUMN;

    private SchemaModification databaseInitialization;
    private SchemaModification updateTableInitialization;
    private boolean ignoreUnrecognizedUpdates;

    private Set<SchemaUpdate> updates;
    private Resource initScriptLocation;
    private Resource customizationScriptLocation;

    /**
     * Get the name of the table that keeps track of applied updates.
     *
     * @see setUpdateTableName
     */
    public void setUpdateTableName(String updateTableName) {
        this.updateTableName = updateTableName;
    }

    /**
     * Set the name of the table that keeps track of applied updates.
     * Default value is {@link #DEFAULT_UPDATE_TABLE_NAME}.
     *
     * <p>
     * This name must be consistent with the {@link #setUpdateTableInitialization update table initialization}.
     *
     * @see setUpdateTableInitialization
     */
    public void setUpdateTableName(String updateTableName) {
        this.updateTableName = updateTableName;
    }

    /**
     * Set the name of the update name column in the table that keeps track of applied updates.
     * Default value is {@link #DEFAULT_UPDATE_NAME_COLUMN}.
     *
     * <p>
     * This name must be consistent with the {@link #setUpdateTableInitialization update table initialization}.
     *
     * @see setUpdateTableInitialization
     */
    public void setUpdateNameColumn(String updateNameColumn) {
        this.updateNameColumn = updateNameColumn;
    }

    /**
     * Set the name of the update timestamp column in the table that keeps track of applied updates.
     * Default value is {@link #DEFAULT_UPDATE_TIME_COLUMN}.
     *
     * <p>
     * This name must be consistent with the {@link #setUpdateTableInitialization update table initialization}.
     *
     * @see setUpdateTableInitialization
     */
    public void setUpdateTimeColumn(String updateTimeColumn) {
        this.updateTimeColumn = updateTimeColumn;
    }

    /**
     * Get the update table initialization.
     *
     * @return update table initialization
     * @see #setUpdateTableInitialization setUpdateTableInitialization()
     */
    public SchemaModification getUpdateTableInitialization() {
        return this.updateTableInitialization;
    }

    /**
     * Configure how the update table itself gets initialized. This update is run when no update table found,
     * which (we assume) implies an empty database with no tables or content.
     *
     * <p>
     * This initialization should create the update table where the name column is the primary key.
     * The name column must have a length limit greater than or equal to the longest schema update name.
     *
     * @see #getUpdateTableInitialization
     */
    public void setUpdateTableInitialization(SchemaModification updateTableInitialization) {
        this.updateTableInitialization = updateTableInitialization;
    }

    /**
     * Get the empty database initialization.
     *
     * @return empty database schema initialization
     * @see #setDatabaseInitialization setDatabaseInitialization()
     */
    public SchemaModification getDatabaseInitialization() {
        return this.databaseInitialization;
    }

    /**
     * Configure how an empty database gets initialized. This update is run when no update table found,
     * which (we assume) implies an empty database with no tables or content.
     *
     * <p>
     * This script is expected to initialize the database (i.e., creating all the tables) and
     * is assumed to be "up to date" with respect to the configured schema updates, i.e.,
     * when the script completes we assum all updates have already been (implicitly) applied.
     *
     * <p>
     * Note this script is <i>not</i> expected to create the update table that tracks schema updates;
     * that function is handled by the {@link #setUpdateTableInitialization update table initialization}.
     *
     * @see #getDatabaseInitialization
     */
    public void setDatabaseInitialization(SchemaModification databaseInitialization) {
        this.databaseInitialization = databaseInitialization;
    }

    /**
     * Get the schema updates.
     *
     * @return configured schema updates
     * @see #setUpdates setUpdates()
     */
    public Set<SchemaUpdate> getUpdates() {
        return this.updates;
    }

    /**
     * Configure the schema updates.
     * This should be the set of all updates that may need to or should have ever needed to be applied to the database.
     *
     * <p>
     * For any given application, ideally this set should be "write only" in the sense that once an update is added to the set,
     * the update and its name should never change. Furthermore, if not configured to {@link #setIgnoreUnrecognizedUpdates ignore
     * unrecognized updates already applied} (the default behavior), then the update must never be removed from the set.
     *
     * @param updates all schema updates
     * @see #getUpdates
     */
    public void setUpdates(Set<SchemaUpdate> updates) {
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
     * Configure behavior when an unknown update is found and registered as having already been applied in the database.
     *
     * <p>
     * The default behavior is <code>false</code>, which results in an exception being thrown to protect against
     * accidental downgrades (i.e., running older code against a database with a newer schema), which are not supported.
     *
     * <p>
     * Setting this to <code>true</code> will result in unrecognized updates simply being ignored.
     * This setting loses the downgrade protection but allows ancient (obsolete) schema updates to drop off the list.
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
     * @throws SQLException if an update fails
     * @throws IllegalStateException if this instance is not configured to {@link #setIgnoreUnrecognizedUpdates ignore
     * unrecognized updates} and an unrecognized update is registered in the update table as having already been applied
     */
    public void initializeAndUpdateDatabase(DataSource dataSource) throws SQLException {
        log.info("verifying database");
        if (this.databaseNeedsInitialization(dataSource))
            this.initializeDatabase(c);
        this.applySchemaUpdatesAsNeeded(c);
        log.info("database verification complete");
    }

    /**
     * Determine if the database needs initialization.
     *
     * <p>
     * The implementation in {@link SchemaUpdater} simply invokes <code>SELECT COUNT(*) FROM <i>UPDATETABLE</i></code>
     * and checks for success or failure. Subclasses may wish to override with a more discriminating implementation.
     */
    public boolean databaseNeedsInitialization(DataSource dataSource) throws SQLException {
        try {
            long numUpdates = new JdbcTemplate(dataSource).queryForLong("SELECT COUNT(*) FROM " + this.updateTableName);
            log.info("detected database is already initialized, with " + numUpdates + " update(s) already applied");
            return false;
        } catch (BadSqlGrammarException e) {
            log.warn("detected uninitializaed database, table `" + this.updateTableName + "' not found: " + e);
            return true;
        }
    }

    // Initialize the database
    private void initializeDatabase() throws SQLException {


%%%%%%%%%%%
        // Nope: assume it's because we need to initialize database
        log.warn("table `" + this.updateTableName + "' not found, initializing database schema: " + e);
        if (this.initScriptLocation == null)
            throw new SQLException("database needs initialization but no init script provided", e);
        String initSQL;
        try {
            initSQL = readResourceAsString(this.initScriptLocation);
        } catch (IOException e2) {
            throw new SQLException("can't read init script from " + this.initScriptLocation, e2);
        }
        applySqlScript(initSQL);

        // Create our schema tracking table
        applySqlScript(buildCreateTableSQL());

        // Now record all schema updates as having already been applied (assume init script is up to date)
        TreeMap<String, String> updates = new TreeMap<String, String>(this.schemaUpdates);
        log.info("recording " + updates.size() + " schema updates as having already been applied");
        for (Map.Entry<String, String> entry : updates.entrySet())
            recordUpdateApplied(entry.getKey());
    }

    /**
     * Build the SQL statement that creates the schema update tracking table.
     */
    public String buildCreateTableSQL() {
        return "CREATE TABLE `" + this.updateTableName + "` ("
          + " `" + this.updateNameColumn + "` VARCHAR(64) NOT NULL,"
          + " `" + this.updateTimeColumn + "` DATETIME NOT NULL,"
          + " PRIMARY KEY(`" + this.updateNameColumn + "`)"
          + ") ENGINE=InnoDB default charset=utf8 collate=utf8_bin";
    }

    private void applySchemaUpdatesAsNeeded() throws SQLException {

        // Determine which updates have already been applied
        List<String> appliedUpdates = getSimpleJdbcTemplate()
          .query("SELECT `" + this.updateNameColumn + "` FROM `" + this.updateTableName + "`", new RowMapper<String>() {
              @Override
              public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                  return rs.getString(1);
              }
          });

        // Verify all applied are known to us, and filter them out from our list
        TreeMap<String, String> updates = new TreeMap<String, String>(this.schemaUpdates);
        for (String update : appliedUpdates) {
            if (updates.remove(update) == null)
                throw new SQLException("an unknown update `" + update + "' has already been applied");
        }

        // Log
        if (updates.isEmpty())
            log.info("no schema updates are required");
        else
            log.info("applying required schema updates: " + updates.keySet());

        // Apply reamining unapplied updates (they're already sorted)
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            String updateName = entry.getKey();
            String sql = entry.getValue();
            log.info("applying schema update `" + updateName + "'");
            applySqlScript(sql);
            recordUpdateApplied(updateName);
        }
    }

    private void applyCustomizationScript() throws SQLException {
        if (this.customizationScriptLocation == null) {
            log.info("no database customization script configured so none will be run");
            return;
        }
        String customizationSQL;
        try {
            customizationSQL = readResourceAsString(this.customizationScriptLocation);
        } catch (IOException e2) {
            throw new SQLException("can't read customization script from " + this.customizationScriptLocation, e2);
        }
        log.info("applying database customization script " + this.customizationScriptLocation);
        applySqlScript(customizationSQL);
    }

    private void applySqlScript(String script) {
        final String[] sqls = script.split("; *\\n[\\s]*");
        getSimpleJdbcTemplate().getJdbcOperations().execute(new ConnectionCallback<Void>() {
            @Override
            public Void doInConnection(Connection connection) throws SQLException {
                for (int i = 0; i < sqls.length; i++) {
                    String sql = sqls[i].trim();
                    if (sql.length() == 0)
                        continue;
                    Statement statement = connection.createStatement();
                    String sep = sql.indexOf('\n') != -1 ? "\n" : " ";
                    SchemaUpdatingDataSource.this.log.info("executing SQL:" + sep + sql);
                    try {
                        statement.execute(sql);
                    } catch (SQLException e) {
                        SchemaUpdatingDataSource.this.log.error("SQL failed: " + sql, e);
                        throw e;
                    } finally {
                        statement.close();
                    }
                }
                return null;
            }
        });
    }

    private void recordUpdateApplied(String updateName) {
        getSimpleJdbcTemplate().update(
          "INSERT INTO `" + this.updateTableName + "` (`" + this.updateNameColumn + "`, `" + this.updateTimeColumn + "`) VALUES (?, ?)",
          updateName, new Date());
    }

    private static String readResourceAsString(Resource resource) throws IOException {
        InputStreamReader in = new InputStreamReader(resource.getInputStream());
        try {
            StringWriter sw = new StringWriter();
            char[] buf = new char[1024];
            for (int r; (r = in.read(buf)) != -1; )
                sw.write(buf, 0, r);
            sw.close();
            return sw.toString();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}

