
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.ant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.internal.Formatter;

/**
 * Ant task that users Hibernate to formats DDL statements.
 */
public class HibernateDDLFormatterTask extends Task {

    private File inputFile;
    private File outputFile;
    private String delimiter = ";";

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * @throws BuildException
     */
    @Override
    public void execute() {

        // Sanity check
        if (this.inputFile == null)
            throw new BuildException("`inputFile' attribute is required");
        if (this.outputFile == null)
            throw new BuildException("`outputFile' attribute is required");

        // Create directory containing output file
        if (this.outputFile.getParent() != null && !this.outputFile.getParentFile().exists()
          && !this.outputFile.getParentFile().mkdirs())
            throw new BuildException("error creating directory `" + this.outputFile.getParentFile() + "'");

        // Format DDL
        final Formatter formatter = FormatStyle.DDL.getFormatter();
        try (final BufferedReader reader = new BufferedReader(
              new InputStreamReader(new FileInputStream(this.inputFile), Charset.defaultCharset()));
            final PrintWriter writer = new PrintWriter(new FileWriter(this.outputFile))) {
            for (String line; (line = reader.readLine()) != null; ) {
                line = line.trim();
                final String[] sqls = (formatter.format(line) + this.delimiter).split("\n");
                for (String sql : sqls) {
                    sql = sql.replaceAll("\\s+$", "").replaceAll("\\t", "    ");
                    writer.println(sql);
                }
            }
        } catch (IOException e) {
            throw new BuildException("error reading file`" + this.inputFile + "'", e);
        }
    }
}

