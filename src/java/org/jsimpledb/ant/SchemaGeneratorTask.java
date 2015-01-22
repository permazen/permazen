
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.ant;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Path;
import org.apache.tools.ant.types.Reference;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.spring.JSimpleDBClassScanner;

/**
 * Ant task that scans the configured classpath for {@link org.jsimpledb.annotation.JSimpleClass}-annotated
 * classes and writes the corresponding schema to an XML file.
 *
 * @see org.jsimpledb.JSimpleDB
 * @see org.jsimpledb.schema.SchemaModel
 */
public class SchemaGeneratorTask extends Task {

    public static final String MODE_VERIFY = "verify";
    public static final String MODE_GENERATE = "generate";

    private String packages;
    private String mode = MODE_VERIFY;
    private boolean matchNames;
    private File file;
    private Path classPath;

    public void setPackages(String packages) {
        this.packages = packages;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setMatchNames(boolean matchNames) {
        this.matchNames = matchNames;
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

    public void setClasspathRef(Reference ref) {
        this.classPath = (Path)ref.getReferencedObject(getProject());
    }

    /**
     * @throws BuildException
     */
    @Override
    public void execute() {

        // Sanity check
        if (this.file == null)
            throw new BuildException("`file' attribute is required specifying output file");
        if (!this.mode.equalsIgnoreCase(MODE_VERIFY) && !this.mode.equalsIgnoreCase(MODE_GENERATE))
            throw new BuildException("`mode' attribute must be `" + MODE_VERIFY + "' or `" + MODE_GENERATE + "'");
        final boolean generate = this.mode.equalsIgnoreCase(MODE_GENERATE);
        if (this.packages == null)
            throw new BuildException("`packages' attribute is required specifying packages to scan for Java model classes");

        // Create directory containing file
        if (generate && this.file.getParent() != null && !this.file.getParentFile().exists() && !this.file.getParentFile().mkdirs())
            throw new BuildException("error creating directory `" + this.file.getParentFile() + "'");

        // Set up mysterious classloader stuff
        final AntClassLoader loader = this.getProject().createClassLoader(this.classPath);
        final ClassLoader currentLoader = this.getClass().getClassLoader();
        if (currentLoader != null)
            loader.setParent(currentLoader);
        loader.setThreadContextLoader();
        try {

            // Scan for classes
            final HashSet<Class<?>> classes = new HashSet<>();
            for (String className : new JSimpleDBClassScanner().scanForClasses(this.packages.split("[\\s,]"))) {
                this.log("adding JSimpleDB schema class " + className);
                try {
                    classes.add(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
                } catch (ClassNotFoundException e) {
                    throw new BuildException("failed to load class `" + className + "'", e);
                }
            }

            // Build schema model
            this.log("generating JSimpleDB schema from schema classes");
            final SchemaModel schemaModel;
            try {
                schemaModel = new JSimpleDB(classes).getSchemaModel();
            } catch (Exception e) {
                throw new BuildException("schema generation failed: " + e, e);
            }

            // Verify or generate
            if (generate) {

                // Write schema model to file
                this.log("writing JSimpleDB schema to `" + this.file + "'");
                try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(this.file))) {
                    schemaModel.toXML(output, true);
                } catch (IOException e) {
                    throw new BuildException("error writing schema to `" + this.file + "': " + e, e);
                }
            } else {

                // Read file
                this.log("verifying JSimpleDB schema matches `" + this.file + "'");
                final SchemaModel verifyModel;
                try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(this.file))) {
                    verifyModel = SchemaModel.fromXML(input);
                } catch (IOException e) {
                    throw new BuildException("error reading schema from `" + this.file + "': " + e, e);
                }

                // Compare
                if (!(matchNames ? schemaModel.equals(verifyModel) : schemaModel.isCompatibleWith(verifyModel)))
                    throw new BuildException("schema verification failed");
            }
        } finally {
            loader.resetThreadContextLoader();
            loader.cleanup();
        }
    }
}

