
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;

import org.dellroad.stuff.string.DateEncoder;

/**
 * CSV file output stream that ensures values are matched to the correct columns.
 * This class requires the <a href="http://opencsv.sourceforge.net/">OpenCSV</a> library.
 *
 * @see <a href="http://opencsv.sourceforge.net/">OpenCSV</a>
 */
public class CSVOutput {

    private final CSVWriter writer;
    private final String[] columns;

    /**
     * Constructor.
     *
     * <p>
     * The column headers will be written out automatically.
     * </p>
     *
     * @param writer destination for the CSV output
     * @param columns CSV columns names in their desired order
     * @throws IllegalArgumentException if {@code writer} is null
     * @throws IllegalArgumentException if {@code columns} is null
     * @throws IllegalArgumentException if {@code columns} contains a duplicate column name
     */
    public CSVOutput(Writer writer, String... columns) {
        this(writer, Arrays.asList(columns));
    }

    /**
     * Constructor.
     *
     * <p>
     * The column headers will be written out automatically.
     * </p>
     *
     * @param writer destination for the CSV output
     * @param columns CSV columns names, iterated in their desired order
     * @throws IllegalArgumentException if {@code writer} is null
     * @throws IllegalArgumentException if {@code columns} is null
     * @throws IllegalArgumentException if {@code columns} contains a duplicate column name
     */
    public CSVOutput(Writer writer, Iterable<String> columns) {
        this(new CSVWriter(new BufferedWriter(writer)), columns);
        if (writer == null)
            throw new IllegalArgumentException("null writer");
    }

    /**
     * Constructor.
     *
     * <p>
     * The column headers will be written out automatically.
     * </p>
     *
     * @param writer CSV output object
     * @param columns CSV columns names, iterated in their desired order
     * @throws IllegalArgumentException if {@code writer} is null
     * @throws IllegalArgumentException if {@code columns} is null
     * @throws IllegalArgumentException if {@code columns} contains a duplicate column name
     */
    public CSVOutput(CSVWriter writer, Iterable<String> columns) {
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        if (columns == null)
            throw new IllegalArgumentException("null columns");
        this.writer = writer;
        LinkedHashSet<String> columnSet = new LinkedHashSet<String>();
        for (String column : columns) {
            if (!columnSet.add(column))
                throw new IllegalArgumentException("duplicate column name `" + column + "'");
        }
        this.columns = columnSet.toArray(new String[columnSet.size()]);

        // Output header
        this.writer.writeNext(this.columns);
    }

    /**
     * Output a CSV row.
     *
     * @param row mapping from column name to value; missing or values are treated as null
     * @throws IllegalArgumentException if {@code row} contains an unknown column name
     */
    public void writeRow(Map<String, ?> row) {

        // Sanity check column names
        HashSet<String> unknowns = new HashSet<String>(row.keySet());
        unknowns.removeAll(Arrays.asList(this.columns));
        if (!unknowns.isEmpty())
            throw new IllegalArgumentException("row contains unknown column(s): " + unknowns);

        // Format columns
        String[] values = new String[this.columns.length];
        for (int i = 0; i < this.columns.length; i++)
            values[i] = this.formatObject(this.columns[i], row.get(this.columns[i]));

        // Output row
        this.writer.writeNext(values);
    }

    /**
     * Flush output.
     */
    public void flush() throws IOException {
        this.writer.flush();
    }

    /**
     * Close this instance and the underlying output.
     */
    public void close() throws IOException {
        this.writer.close();
    }

    /**
     * Format a CSV column value.
     *
     * <p>
     * The implementation in {@link CSVOutput} applies the following logic:
     * <ul>
     * <li>{@code null} values are output as the empty string</li>
     * <li>{@link Boolean} values are output as {@code 1} or {@code 0}</li>
     * <li>{@link Date} values are output by delegating to {@link #formatDate formatDate()}</li>
     * <li>All other objects are output using {@link String#valueOf}</li>
     * </ul>
     * </p>
     *
     * <p>
     * Subclasses should override as needed.
     * </p>
     *
     * @param columnName name of the column
     * @param value column value; will be null if no value was present in the {@link Map} parameter to {@link #writeRow}
     */
    protected String formatObject(String columnName, Object value) {
        if (value == null)
            return "";
        if (value instanceof Boolean)
            return ((Boolean)value).booleanValue() ? "1" : "0";
        if (value instanceof Date)
            return this.formatDate(columnName, (Date)value);
        return String.valueOf(value);
    }

    /**
     * Format a {@link Date} value.
     *
     * <p>
     * The implementation in {@link CSVOutput} delegates to {@link DateEncoder#encode DateEncoder.encode()}.
     * </p>
     *
     * <p>
     * Subclasses should override as needed.
     * </p>
     *
     * @param columnName name of the column
     * @param date column value
     * @throws IllegalArgumentException if {@code date} is null
     */
    protected String formatDate(String columnName, Date date) {
        if (date == null)
            throw new IllegalArgumentException("null date");
        return DateEncoder.encode(date);
    }
}

