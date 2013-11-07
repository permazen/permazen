
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Merges multiple {@link ResultSet}s into a single {@link ResultSet}. The merge is performed in an online fashion
 * and supports preserving sort order for sorted {@link ResultSet}s.
 *
 * <p>
 * The given {@link ResultSet}s must have the same column geometry. If the {@link ResultSet}s are sorted, then in order
 * to preserve that sorting the {@link ResultSet}s must be sorted on the same columns, and the sort columns and corresponding
 * sort orderings must be provided to the appropriate constructor.
 * </p>
 *
 * <p>
 * This class provides a {@link ResultSet} of type {@link #TYPE_FORWARD_ONLY} and concurrency mode {@link #CONCUR_READ_ONLY}.
 * Non-row specific information, such as is returned by {@link #getMetaData}, {@link #getStatement}, {@link #getFetchSize},
 * {@link #getHoldability}, and {@link #findColumn findColumn()} is derived from the first {@link ResultSet} provided.
 * </p>
 */
public class MergedResultSet implements ResultSet {

    private final ResultSet representative;
    private final ArrayList<ResultSet> resultSets;
    private final ArrayList<ResultSet> remaining;
    private final Comparator<ResultSet> comparator;

    private int currentRow;
    private boolean closed;

    /**
     * Constructor. No sort order is assumed
     *
     * @param resultSets the {@link ResultSet}s to be merged
     * @throws IllegalArgumentException if {@code resultSets} is null
     * @throws IllegalArgumentException if {@code resultSets} is empty or contains a null element
     */
    public MergedResultSet(ResultSet... resultSets) {
        this(Arrays.<ResultSet>asList(resultSets));
    }

    /**
     * Constructor. No sort order is assumed
     *
     * @param resultSets the {@link ResultSet}s to be merged
     * @throws IllegalArgumentException if {@code resultSets} is null
     * @throws IllegalArgumentException if {@code resultSets} is empty or contains a null element
     */
    public MergedResultSet(List<ResultSet> resultSets) {
        this(resultSets, new int[0], new boolean[0]);
    }

    /**
     * Constructor.
     *
     * @param resultSets the {@link ResultSet}s to be merged
     * @param sortColumns the names of the columns on which the {@link ResultSet}s are sorted (ascending order assumed)
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code resultSets} is empty or contains a null element
     */
    public MergedResultSet(List<ResultSet> resultSets, String[] sortColumns) {
        this(resultSets, sortColumns, new boolean[0]);
    }

    /**
     * Constructor.
     *
     * @param resultSets the {@link ResultSet}s to be merged
     * @param sortColumns the indicies (1-based) of the columns on which the {@link ResultSet}s are sorted (ascending order assumed)
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code resultSets} is empty or contains a null element
     */
    public MergedResultSet(List<ResultSet> resultSets, int[] sortColumns) {
        this(resultSets, sortColumns, new boolean[0]);
    }

    /**
     * Constructor.
     *
     * @param resultSets the {@link ResultSet}s to be merged
     * @param sortColumns the names of the columns on which the {@link ResultSet}s are sorted
     * @param sortOrders the ordering corresponding to {@code sortColumns} (true for ascending); if {@code sortOrders}
     *  is shorter than {@code sortColumns}, then ascending is assumed for the unspecified columns
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code resultSets} is empty or contains a null element
     */
    public MergedResultSet(List<ResultSet> resultSets, String[] sortColumns, boolean[] sortOrders) {
        this(resultSets, MergedResultSet.determineSortColumns(resultSets, sortColumns), sortOrders);
    }

    /**
     * Constructor.
     *
     * @param resultSets the {@link ResultSet}s to be merged
     * @param sortColumns the indicies (1-based) of the columns on which the {@link ResultSet}s are sorted
     * @param sortOrders the ordering corresponding to {@code sortColumns} (true for ascending); if {@code sortOrders}
     *  is shorter than {@code sortColumns}, then ascending is assumed for the unspecified columns
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code resultSets} is empty or contains a null element
     */
    public MergedResultSet(List<ResultSet> resultSets, final int[] sortColumns, boolean[] sortOrders) {
        if (resultSets == null)
            throw new IllegalArgumentException("null resultSets");
        if (resultSets.isEmpty())
            throw new IllegalArgumentException("empty resultSets");
        for (ResultSet rs : resultSets) {
            if (rs == null)
                throw new IllegalArgumentException("null resultSet");
        }
        if (sortColumns == null)
            throw new IllegalArgumentException("null sortColumns");
        if (sortOrders == null)
            throw new IllegalArgumentException("null sortOrders");
        if (sortOrders.length < sortColumns.length) {
            final boolean[] newSortOrders = new boolean[sortColumns.length];
            Arrays.fill(newSortOrders, true);
            System.arraycopy(sortOrders, 0, newSortOrders, 0, Math.min(sortOrders.length, newSortOrders.length));
            sortOrders = newSortOrders;
        }
        this.resultSets = new ArrayList<ResultSet>(resultSets);
        this.remaining = new ArrayList<ResultSet>(this.resultSets.size());
        this.representative = this.resultSets.get(0);
        final boolean[] sortOrders2 = sortOrders;
        this.comparator = sortColumns.length == 0 ? null : new Comparator<ResultSet>() {
            @Override
            @SuppressWarnings({ "unchecked" })
            public int compare(ResultSet rs1, ResultSet rs2) {
                for (int i = 0; i < sortColumns.length; i++) {
                    Object value1;
                    Object value2;
                    try {
                        value1 = rs1.getObject(sortColumns[i]);
                        value2 = rs2.getObject(sortColumns[i]);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    if (value1 == null && value2 != null)
                        return -1;
                    if (value1 != null && value2 == null)
                        return 1;
                    if (value1 == null && value2 == null)
                        return 0;
                    int diff = ((Comparable)value1).compareTo(value2);
                    if (diff != 0)
                        return sortOrders2[i] ? diff : -diff;
                }
                return 0;
            }
        };
    }

    // Convert column names into column indexes
    private static int[] determineSortColumns(List<ResultSet> resultSets, String[] sortColumnNames) {
        if (resultSets.isEmpty())
            throw new IllegalArgumentException("empty resultSets");
        final int[] sortColumnNums = new int[sortColumnNames.length];
        for (int i = 0; i < sortColumnNums.length; i++) {
            try {
                sortColumnNums[i] = resultSets.get(0).findColumn(sortColumnNames[i]);
            } catch (SQLException e) {
                throw new IllegalArgumentException("column `" + sortColumnNames[i] + "' not found", e);
            }
        }
        return sortColumnNums;
    }

    private ResultSet getCurrent() throws SQLException {
        this.checkClosed();
        if (this.remaining.isEmpty())
            throw new SQLException("there is no current row");
        return this.remaining.get(0);
    }

    private void checkClosed() throws SQLException {
        if (this.closed)
            throw new SQLException("ResultSet is closed");
    }

    @Override
    public boolean next() throws SQLException {
        if (this.currentRow == 0) {             // we are before the first row

            // Perform the initial load
            this.remaining.addAll(this.resultSets);
            for (Iterator<ResultSet> i = this.remaining.iterator(); i.hasNext(); ) {
                if (!i.next().next())
                    i.remove();
            }
        } else {                                // we are somewhere in the middle or past the last row

            // Discard the current row, if any
            if (!this.remaining.isEmpty() && !this.remaining.get(0).next())
                this.remaining.remove(0);
        }

        // Are all ResultSets completely exhausted?
        if (this.remaining.isEmpty()) {
            this.currentRow = -1;               // indicates we are past the last row
            return false;
        }

        // Re-sort the remaining ResultSets
        if (this.comparator != null) {
            try {
                Collections.sort(this.remaining, this.comparator);
            } catch (RuntimeException e) {
                if (e.getCause() instanceof SQLException)
                    throw (SQLException)e.getCause();
                throw e;
            }
        }

        // Done
        this.currentRow++;
        return true;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
        for (ResultSet rs : this.resultSets)
            rs.close();
        this.remaining.clear();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.getCurrent().wasNull();
    }

    @Override
    public String getString(int column) throws SQLException {
        return this.getCurrent().getString(column);
    }

    @Override
    public boolean getBoolean(int column) throws SQLException {
        return this.getCurrent().getBoolean(column);
    }

    @Override
    public byte getByte(int column) throws SQLException {
        return this.getCurrent().getByte(column);
    }

    @Override
    public short getShort(int column) throws SQLException {
        return this.getCurrent().getShort(column);
    }

    @Override
    public int getInt(int column) throws SQLException {
        return this.getCurrent().getInt(column);
    }

    @Override
    public long getLong(int column) throws SQLException {
        return this.getCurrent().getLong(column);
    }

    @Override
    public float getFloat(int column) throws SQLException {
        return this.getCurrent().getFloat(column);
    }

    @Override
    public double getDouble(int column) throws SQLException {
        return this.getCurrent().getDouble(column);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int column, int scale) throws SQLException {
        return this.getCurrent().getBigDecimal(column, scale);
    }

    @Override
    public byte[] getBytes(int column) throws SQLException {
        return this.getCurrent().getBytes(column);
    }

    @Override
    public Date getDate(int column) throws SQLException {
        return this.getCurrent().getDate(column);
    }

    @Override
    public Time getTime(int column) throws SQLException {
        return this.getCurrent().getTime(column);
    }

    @Override
    public Timestamp getTimestamp(int column) throws SQLException {
        return this.getCurrent().getTimestamp(column);
    }

    @Override
    public InputStream getAsciiStream(int column) throws SQLException {
        return this.getCurrent().getAsciiStream(column);
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int column) throws SQLException {
        return this.getCurrent().getUnicodeStream(column);
    }

    @Override
    public InputStream getBinaryStream(int column) throws SQLException {
        return this.getCurrent().getBinaryStream(column);
    }

    @Override
    public String getString(String column) throws SQLException {
        return this.getCurrent().getString(column);
    }

    @Override
    public boolean getBoolean(String column) throws SQLException {
        return this.getCurrent().getBoolean(column);
    }

    @Override
    public byte getByte(String column) throws SQLException {
        return this.getCurrent().getByte(column);
    }

    @Override
    public short getShort(String column) throws SQLException {
        return this.getCurrent().getShort(column);
    }

    @Override
    public int getInt(String column) throws SQLException {
        return this.getCurrent().getInt(column);
    }

    @Override
    public long getLong(String column) throws SQLException {
        return this.getCurrent().getLong(column);
    }

    @Override
    public float getFloat(String column) throws SQLException {
        return this.getCurrent().getFloat(column);
    }

    @Override
    public double getDouble(String column) throws SQLException {
        return this.getCurrent().getDouble(column);
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(String column, int scale) throws SQLException {
        return this.getCurrent().getBigDecimal(column, scale);
    }

    @Override
    public byte[] getBytes(String column) throws SQLException {
        return this.getCurrent().getBytes(column);
    }

    @Override
    public Date getDate(String column) throws SQLException {
        return this.getCurrent().getDate(column);
    }

    @Override
    public Time getTime(String column) throws SQLException {
        return this.getCurrent().getTime(column);
    }

    @Override
    public Timestamp getTimestamp(String column) throws SQLException {
        return this.getCurrent().getTimestamp(column);
    }

    @Override
    public InputStream getAsciiStream(String column) throws SQLException {
        return this.getCurrent().getAsciiStream(column);
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String column) throws SQLException {
        return this.getCurrent().getUnicodeStream(column);
    }

    @Override
    public InputStream getBinaryStream(String column) throws SQLException {
        return this.getCurrent().getBinaryStream(column);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.getCurrent().getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (!this.remaining.isEmpty())
            this.getCurrent().clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.representative.getMetaData();
    }

    @Override
    public Object getObject(int column) throws SQLException {
        return this.getCurrent().getObject(column);
    }

    @Override
    public Object getObject(String column) throws SQLException {
        return this.getCurrent().getObject(column);
    }

    @Override
    public int findColumn(String column) throws SQLException {
        return this.representative.findColumn(column);
    }

    @Override
    public Reader getCharacterStream(int column) throws SQLException {
        return this.getCurrent().getCharacterStream(column);
    }

    @Override
    public Reader getCharacterStream(String column) throws SQLException {
        return this.getCurrent().getCharacterStream(column);
    }

    @Override
    public BigDecimal getBigDecimal(int column) throws SQLException {
        return this.getCurrent().getBigDecimal(column);
    }

    @Override
    public BigDecimal getBigDecimal(String column) throws SQLException {
        return this.getCurrent().getBigDecimal(column);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        this.checkClosed();
        return this.currentRow == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        this.checkClosed();
        return this.currentRow == -1;
    }

    @Override
    public boolean isFirst() throws SQLException {
        this.checkClosed();
        return this.currentRow == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public int getRow() throws SQLException {
        this.checkClosed();
        return this.currentRow > 0 ? this.currentRow : 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean relative(int row) throws SQLException {
        throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public void setFetchDirection(int dir) throws SQLException {
        this.checkClosed();
        if (dir != FETCH_FORWARD)
            throw new SQLException("this ResultSet is TYPE_FORWARD_ONLY");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        this.checkClosed();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int size) throws SQLException {
        for (ResultSet rs : this.resultSets)
            rs.setFetchSize(size);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.representative.getFetchSize();
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNull(int column) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBoolean(int column, boolean value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateByte(int column, byte value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateShort(int column, short value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateInt(int column, int value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateLong(int column, long value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateFloat(int column, float value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateDouble(int column, double value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBigDecimal(int column, BigDecimal value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateString(int column, String value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBytes(int column, byte[] value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateDate(int column, Date value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateTime(int column, Time value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateTimestamp(int column, Timestamp value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateAsciiStream(int column, InputStream value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBinaryStream(int column, InputStream value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateCharacterStream(int column, Reader value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateObject(int column, Object value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateObject(int column, Object value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNull(String value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBoolean(String column, boolean value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateByte(String column, byte value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateShort(String column, short value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateInt(String column, int value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateLong(String column, long value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateFloat(String column, float value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateDouble(String column, double value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBigDecimal(String column, BigDecimal value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateString(String column, String value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBytes(String column, byte[] value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateDate(String column, Date value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateTime(String column, Time value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateTimestamp(String column, Timestamp value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateAsciiStream(String column, InputStream value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBinaryStream(String column, InputStream value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateCharacterStream(String column, Reader value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateObject(String column, Object value, int length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateObject(String column, Object value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void insertRow() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateRow() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void deleteRow() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void refreshRow() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.representative.getStatement();
    }

    @Override
    public Object getObject(int column, Map<String, Class<?>> map) throws SQLException {
        return this.getCurrent().getObject(column, map);
    }

    @Override
    public Ref getRef(int column) throws SQLException {
        return this.getCurrent().getRef(column);
    }

    @Override
    public Blob getBlob(int column) throws SQLException {
        return this.getCurrent().getBlob(column);
    }

    @Override
    public Clob getClob(int column) throws SQLException {
        return this.getCurrent().getClob(column);
    }

    @Override
    public Array getArray(int column) throws SQLException {
        return this.getCurrent().getArray(column);
    }

    @Override
    public Object getObject(String column, Map<String, Class<?>> map) throws SQLException {
        return this.getCurrent().getObject(column, map);
    }

    @Override
    public Ref getRef(String column) throws SQLException {
        return this.getCurrent().getRef(column);
    }

    @Override
    public Blob getBlob(String column) throws SQLException {
        return this.getCurrent().getBlob(column);
    }

    @Override
    public Clob getClob(String column) throws SQLException {
        return this.getCurrent().getClob(column);
    }

    @Override
    public Array getArray(String column) throws SQLException {
        return this.getCurrent().getArray(column);
    }

    @Override
    public Date getDate(int column, Calendar calendar) throws SQLException {
        return this.getCurrent().getDate(column);
    }

    @Override
    public Date getDate(String column, Calendar calendar) throws SQLException {
        return this.getCurrent().getDate(column, calendar);
    }

    @Override
    public Time getTime(int column, Calendar calendar) throws SQLException {
        return this.getCurrent().getTime(column, calendar);
    }

    @Override
    public Time getTime(String column, Calendar calendar) throws SQLException {
        return this.getCurrent().getTime(column, calendar);
    }

    @Override
    public Timestamp getTimestamp(int column, Calendar calendar) throws SQLException {
        return this.getCurrent().getTimestamp(column, calendar);
    }

    @Override
    public Timestamp getTimestamp(String column, Calendar calendar) throws SQLException {
        return this.getCurrent().getTimestamp(column, calendar);
    }

    @Override
    public URL getURL(int column) throws SQLException {
        return this.getCurrent().getURL(column);
    }

    @Override
    public URL getURL(String column) throws SQLException {
        return this.getCurrent().getURL(column);
    }

    @Override
    public void updateRef(int column, Ref value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateRef(String column, Ref value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(int column, Blob value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(String column, Blob value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(int column, Clob value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(String column, Clob value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateArray(int column, Array value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateArray(String column, Array value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public RowId getRowId(int column) throws SQLException {
        return this.getCurrent().getRowId(column);
    }

    @Override
    public RowId getRowId(String column) throws SQLException {
        return this.getCurrent().getRowId(column);
    }

    @Override
    public void updateRowId(int column, RowId value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateRowId(String column, RowId value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.representative.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void updateNString(int column, String value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNString(String column, String value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(int column, NClob value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(String column, NClob value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public NClob getNClob(int column) throws SQLException {
        return this.getCurrent().getNClob(column);
    }

    @Override
    public NClob getNClob(String column) throws SQLException {
        return this.getCurrent().getNClob(column);
    }

    @Override
    public SQLXML getSQLXML(int column) throws SQLException {
        return this.getCurrent().getSQLXML(column);
    }

    @Override
    public SQLXML getSQLXML(String column) throws SQLException {
        return this.getCurrent().getSQLXML(column);
    }

    @Override
    public void updateSQLXML(int column, SQLXML value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateSQLXML(String column, SQLXML value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public String getNString(int column) throws SQLException {
        return this.getCurrent().getNString(column);
    }

    @Override
    public String getNString(String column) throws SQLException {
        return this.getCurrent().getNString(column);
    }

    @Override
    public Reader getNCharacterStream(int column) throws SQLException {
        return this.getCurrent().getNCharacterStream(column);
    }

    @Override
    public Reader getNCharacterStream(String column) throws SQLException {
        return this.getCurrent().getNCharacterStream(column);
    }

    @Override
    public void updateNCharacterStream(int column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNCharacterStream(String column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateAsciiStream(int column, InputStream value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBinaryStream(int column, InputStream value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateCharacterStream(int column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateAsciiStream(String column, InputStream value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBinaryStream(String column, InputStream value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateCharacterStream(String column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(int column, InputStream value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(String column, InputStream value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(int column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(String column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(int column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(String column, Reader value, long length) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNCharacterStream(int column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNCharacterStream(String column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateAsciiStream(int column, InputStream value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBinaryStream(int column, InputStream value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateCharacterStream(int column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateAsciiStream(String column, InputStream value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBinaryStream(String column, InputStream value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateCharacterStream(String column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(int column, InputStream value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateBlob(String column, InputStream value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(int column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateClob(String column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(int column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public void updateNClob(String column, Reader value) throws SQLException {
        this.checkClosed();
        throw new SQLException("this ResultSet is CONCUR_READ_ONLY");
    }

    @Override
    public <T> T getObject(int column, Class<T> type) throws SQLException {
        return this.getCurrent().getObject(column, type);
    }

    @Override
    public <T> T getObject(String column, Class<T> type) throws SQLException {
        return this.getCurrent().getObject(column, type);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("not a wrapper for " + iface);
    }
}

