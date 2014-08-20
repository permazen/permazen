
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.ant;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;

import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Path;

/**
 * Ant task for JPA 2.1 schema generation.
 *
 * @see <a href="https://blogs.oracle.com/arungupta/entry/jpa_2_1_schema_generation">JPA 2.1 Schema Generation (TOTD #187)</a>
 */
public class JPASchemaGeneratorTask extends Task {

    private boolean create;
    private boolean drop;
    private boolean schemas;
    private String unit;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String databaseProductName;
    private int databaseMajorVersion;
    private int databaseMinorVersion;
    private File file;
    private Path classPath;

    public void setCreate(boolean create) {
        this.create = create;
    }

    public void setDrop(boolean drop) {
        this.drop = drop;
    }

    public void setSchemas(boolean schemas) {
        this.schemas = schemas;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setURL(String url) {
        this.url = url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDatabaseProductName(String databaseProductName) {
        this.databaseProductName = databaseProductName;
    }

    public void setDatabaseMajorVersion(int databaseMajorVersion) {
        this.databaseMajorVersion = databaseMajorVersion;
    }

    public void setDatabaseMinorVersion(int databaseMinorVersion) {
        this.databaseMinorVersion = databaseMinorVersion;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public Path createClasspath() {
        this.classPath = new Path(this.getProject());
        return this.classPath;
    }

    public void setClasspath(Path classPath) {
        this.classPath = classPath;
    }

    /**
     * @throws BuildException
     */
    @Override
    public void execute() {

        // Sanity check
        if (this.unit == null)
            throw new BuildException("`unit' attribute is required specifying persistence unit name");
        if (this.file == null)
            throw new BuildException("`file' attribute is required specifying output file");

        // Create directory containing file
        if (this.file.getParent() != null && !this.file.getParentFile().exists() && !this.file.getParentFile().mkdirs())
            throw new BuildException("error creating directory `" + this.file.getParentFile() + "'");

        // Get URL of output file
        final URL fileURL;
        try {
            fileURL = this.file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new BuildException("unexpected exception: " + e, e);
        }

        // Set up properties
        final HashMap<String, Object> map = new HashMap<>();
        map.put("javax.persistence.schema-generation.scripts.create-target", fileURL.toString());
        map.put("javax.persistence.schema-generation.scripts.drop-target", fileURL.toString());
        map.put("javax.persistence.schema-generation.scripts.create-source", "metadata");
        map.put("javax.persistence.schema-generation.scripts.drop-source", "metadata");
        map.put("javax.persistence.schema-generation.database.action", "none");
        map.put("javax.persistence.schema-generation.scripts.action",
          this.create && this.drop ? "drop-and-create" : this.create ? "create" : "drop");
        map.put("javax.persistence.schema-generation.create-database-schemas", this.schemas);

        // Set up mysterious classloader stuff
        final AntClassLoader loader = this.getProject().createClassLoader(this.classPath);
        final ClassLoader currentLoader = this.getClass().getClassLoader();
        if (currentLoader != null)
            loader.setParent(currentLoader);
        loader.setThreadContextLoader();
        try {

            // Use connection to get database info, or use explicitly provided info
            Connection con = null;
            if (this.url != null && this.url.length() > 0) {
                if (this.driver != null && this.driver.length() > 0) {
                    try {
                        Class.forName(this.driver);
                    } catch (Exception e) {
                        throw new BuildException("can't load database driver class `" + this.driver + "': " + e, e);
                    }
                }
                try {
                    con = DriverManager.getConnection(this.url, this.username, this.password);
                } catch (Exception e) {
                    throw new BuildException("can't database connection to " + this.url, e);
                }
                map.put("javax.persistence.schema-generation.connection", con);
            } else {
                if (databaseProductName == null) {
                    throw new BuildException("must specify database type using \"databaseProductName\", \"databaseMajorVersion\","
                      + " and \"databaseMinorVersion\" attributes, or using \"driver\", \"url\", \"username\", and \"password\""
                      + " attributes");
                }
                map.put("javax.persistence.database-product-name", this.databaseProductName);
                map.put("javax.persistence.database-major-version", this.databaseMajorVersion);
                map.put("javax.persistence.database-minor-version", this.databaseMinorVersion);
            }

            // Debug
            this.log("Properties for persistence unit `" + this.unit + "' JPA schema generation: " + map, Project.MSG_VERBOSE);

            // Generate schema
            try {
                Persistence.generateSchema(this.unit, map);
            } catch (PersistenceException e) {
                throw new BuildException("schema generation for persistence unit `" + this.unit + "' failed: " + e, e);
            } finally {
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        } finally {
            loader.resetThreadContextLoader();
            loader.cleanup();
        }
    }
}

