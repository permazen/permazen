
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Outputs the result of an SQL query as XML.
 *
 * <p>
 * For example, the query <code>SELECT ID, LAST_NAME, FIRST_NAME FROM EMPLOYEE</code> might become:
 * <pre>
 *  &lt;result-set&gt;
 *      &lt;query&gt;&lt;![CDATA[SELECT ID, LAST_NAME, FIRST_NAME FROM EMPLOYEE]]&gt;&lt;/query&gt;
 *      &lt;columns&gt;
 *          &lt;column index="1" name="ID" precision="20" type="BIGINT" typeName="BIGINT" nullable="false"/&gt;
 *          &lt;column index="2" name="LAST_NAME" precision="255" type="VARCHAR" typeName="VARCHAR" nullable="false"/&gt;
 *          &lt;column index="3" name="FIRST_NAME" precision="255" type="VARCHAR" typeName="VARCHAR" nullable="false"/&gt;
 *      &lt;/columns&gt;
 *      &lt;data&gt;
 *          &lt;row&gt;
 *              &lt;column index="1"&gt;1302&lt;/column&gt;
 *              &lt;column index="2"&gt;Washington&lt;/column&gt;
 *              &lt;column index="3"&gt;George&lt;/column&gt;
 *          &lt;/row&gt;
 *          &lt;row&gt;
 *              &lt;column index="1"&gt;1303&lt;/column&gt;
 *              &lt;column index="2"&gt;Lincoln&lt;/column&gt;
 *              &lt;column index="3"&gt;Abraham&lt;/column&gt;
 *          &lt;/row&gt;
 *          ...
 *      &lt;/data&gt;
 *  &lt;/result-set&gt;
 * </pre>
 * </p>
 */
public class XMLResultSetWriter {

    /**
     * Mapping from result set {@link Types} values to their corresponding names.
     */
    public static final Map<Integer, String> TYPE_NAMES;
    static {
        HashMap<Integer, String> map = new HashMap<Integer, String>();
        for (Field field : Types.class.getDeclaredFields()) {
            if (field.getType() != Integer.TYPE || field.getModifiers() != (Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL))
                continue;
            try {
                map.put(field.getInt(null), field.getName());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        TYPE_NAMES = Collections.unmodifiableMap(map);
    }

    private static final String DATE_FORMAT = "dd-MMM-yyyy";
    private static final String TIMESTAMP_FORMAT = DATE_FORMAT + " HH:mm:ss";

    private final XMLStreamWriter writer;
    private final String indentSpace;

    private State state = State.LAST_CLOSE;
    private int nesting;

    // Tracks state of the output
    private enum State {
        LAST_OPEN,
        LAST_CLOSE,
        LAST_TEXT
    };

    /**
     * Constructor.
     *
     * @param writer where to write the XML
     * @param indent number of spaces for indentation
     * @throws IllegalArgumentException if {@code writer} is null
     */
    public XMLResultSetWriter(XMLStreamWriter writer, int indent) {
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        this.writer = writer;
        char[] spaces = new char[indent];
        Arrays.fill(spaces, ' ');
        this.indentSpace = new String(spaces);
    }

    /**
     * Write the given {@link ResultSet} out within a <code>&lt;result-set&gt;</code> XML element.
     *
     * <p>
     * This is a convenience method, equivalent to:
     *  <blockquote><code>
     *      write(null, resultSet)
     *  </code></blockquote>
     * element is included.
     */
    public void write(ResultSet resultSet) throws SQLException, XMLStreamException {
        this.write(null, resultSet);
    }

    /**
     * Write the given {@link ResultSet} out within a <code>&lt;result-set&gt;</code> XML element including the query.
     * The given SQL query will be present as a <code>&lt;query&gt;</code> first child element,
     * followed by a <code>&lt;columns&gt;</code> element containing column meta-data, and finally
     * the <code>&lt;data&gt;</code> element containing each returned row.
     *
     * <p>
     * The result set is iterated in a forward direction only.
     *
     * @param query the SQL query, or null to omit
     * @param resultSet result of the query
     * @throws XMLStreamException if there is an error writing out the XML
     * @throws SQLException if there is an error reading from {@code resultSet}
     */
    public void write(String query, ResultSet resultSet) throws SQLException, XMLStreamException {

        // Start result set
        this.openTag("result-set");

        // Emit query
        if (query != null) {
            this.openTag("query");
            this.emitCData(query);
            this.closeTag();
        }

        // Emit column info
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int numColumns = metaData.getColumnCount();
        this.openTag("columns");
        for (int col = 1; col <= numColumns; col++) {
            this.openTag("column");
            this.writer.writeAttribute("index", "" + col);
            this.writer.writeAttribute("name", metaData.getColumnName(col));
            int precision = metaData.getPrecision(col);
            if (precision != 0)
                this.writer.writeAttribute("precision", "" + precision);
            int scale = metaData.getScale(col);
            if (scale != 0)
                this.writer.writeAttribute("scale", "" + scale);
            int type = metaData.getColumnType(col);
            final String typeName = XMLResultSetWriter.TYPE_NAMES.get(type);
            this.writer.writeAttribute("type", typeName != null ? typeName : "" + type);
            this.writer.writeAttribute("typeName", metaData.getColumnTypeName(col));
            String nullableDesc;
            switch (metaData.isNullable(col)) {
            case ResultSetMetaData.columnNullable:
                nullableDesc = "true";
                break;
            case ResultSetMetaData.columnNoNulls:
                nullableDesc = "false";
                break;
            default:
                nullableDesc = "unknown";
                break;
            }
            this.writer.writeAttribute("nullable", nullableDesc);
            this.closeTag();
        }
        this.closeTag();

        // Emit result data
        this.openTag("data");
        while (resultSet.next()) {
            this.openTag("row");
            for (int col = 1; col <= numColumns; col++)
                this.writeDataColumn(resultSet, metaData.getColumnType(col), col);
            this.closeTag();
        }
        this.closeTag();

        // End result set
        this.closeTag();
    }

    /**
     * Flush the underlying {@link XMLStreamWriter}.
     *
     * @throws XMLStreamException if there is an error flushing the underlying {@link XMLStreamWriter}
     */
    public void flush() throws XMLStreamException {
        this.writer.flush();
    }

    /**
     * Close the underlying {@link XMLStreamWriter}.
     *
     * @throws XMLStreamException if there is an error closing the underlying {@link XMLStreamWriter}
     */
    public void close() throws XMLStreamException {
        this.writer.writeEndDocument();
        this.writer.flush();
        this.writer.close();
    }

// Writing a data row

    private void writeDataColumn(ResultSet resultSet, int columnType, int col) throws SQLException, XMLStreamException {
        Object value = null;
        switch (columnType) {
        case Types.BIT:
        case Types.BOOLEAN:
        {
            boolean b = resultSet.getBoolean(col);
            if (!resultSet.wasNull())
                value = "" + b;
            break;
        }
        case Types.CLOB:
        case Types.NCLOB:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
        {
            Reader reader = resultSet.getCharacterStream(col);
            if (reader == null)
                break;
            int len;
            char[] buf = new char[1024];
            this.openColumnTag(col);
            try {
                while ((len = reader.read(buf)) != -1)
                    this.emitText(buf, 0, len);
                reader.close();
            } catch (IOException e) {
                throw (SQLException)new SQLException("error reading from CLOB").initCause(e);
            }
            this.closeTag();
            return;
        }
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.REAL:
        case Types.NUMERIC:
            value = resultSet.getBigDecimal(col);
            break;
        case Types.INTEGER:
        case Types.TINYINT:
        case Types.SMALLINT:
            value = resultSet.getInt(col);
            break;
        case Types.DATE:
        {
            Date date = resultSet.getDate(col);
            if (date != null)
                value = new SimpleDateFormat(DATE_FORMAT).format(date);
            break;
        }
        case Types.TIME:
            value = resultSet.getTime(col);
            break;
        case Types.TIMESTAMP:
        {
            Timestamp timestamp = resultSet.getTimestamp(col);
            if (timestamp != null)
                value = new SimpleDateFormat(TIMESTAMP_FORMAT).format(timestamp);
            break;
        }
        case Types.BLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        {
            InputStream input = resultSet.getBinaryStream(col);
            if (input == null)
                break;
            this.openColumnTag(col);
            try {
                this.writeBinary(input);
                input.close();
            } catch (IOException e) {
                throw (SQLException)new SQLException("error reading from BLOB").initCause(e);
            }
            this.closeTag();
            return;
        }
        case Types.ROWID:
        {
            try {
                value = resultSet.getRowId(col);
            } catch (AbstractMethodError e) {
                value = resultSet.getObject(col);
            }
            break;
        }
        // Unhandled cases...
        case Types.ARRAY:
        case Types.DATALINK:
        case Types.DISTINCT:
        case Types.JAVA_OBJECT:
        case Types.NULL:
        case Types.OTHER:
        case Types.REF:
        case Types.SQLXML:
        case Types.STRUCT:
        default:
            value = resultSet.getObject(col);
            break;
        }

        // Handle null by omitting the tag entirely
        if (value == null || resultSet.wasNull())
            return;

        // Emit value
        this.openColumnTag(col);
        this.emitText(value.toString());
        this.closeTag();
    }

    private void writeBinary(InputStream input) throws IOException, XMLStreamException {
        int len;
        byte[] buf = new byte[1024];
        char[] ch = new char[2];
        while ((len = input.read(buf)) != -1) {
            for (int i = 0; i < len; i++) {
                int val = buf[i] & 0xff;
                ch[0] = Character.forDigit(val >> 4, 16);
                ch[1] = Character.forDigit(val & 0xf, 16);
                this.emitText(ch, 0, 2);
            }
        }
    }

// XML output formatting

    private void openColumnTag(int col) throws XMLStreamException {
        this.openTag("column");
        this.emitAttr("index", "" + col);
    }

    private void openTag(String name) throws XMLStreamException {
        switch (this.state) {
        case LAST_OPEN:
            this.writer.writeCharacters("\n");
            break;
        case LAST_CLOSE:
            break;
        case LAST_TEXT:
        default:
            throw new RuntimeException("mixed element content not supported");
        }
        for (int i = 0; i < this.nesting; i++)
            this.writer.writeCharacters(this.indentSpace);
        this.writer.writeStartElement(name);
        this.nesting++;
        this.state = State.LAST_OPEN;
    }

    private void emitAttr(String name, String value) throws XMLStreamException {
        this.writer.writeAttribute(name, value);
    }

    private void emitText(String content) throws XMLStreamException {
        this.writer.writeCharacters(content);
        this.state = State.LAST_TEXT;
    }

    private void emitText(char[] buf, int off, int len) throws XMLStreamException {
        this.writer.writeCharacters(buf, off, len);
        this.state = State.LAST_TEXT;
    }

    private void emitCData(String content) throws XMLStreamException {
        this.writer.writeCData(content);
        this.state = State.LAST_TEXT;
    }

    private void closeTag() throws XMLStreamException {
        this.nesting--;
        if (this.state == State.LAST_CLOSE) {
            for (int i = 0; i < this.nesting; i++)
                this.writer.writeCharacters(this.indentSpace);
        }
        this.writer.writeEndElement();
        this.writer.writeCharacters("\n");
        this.state = State.LAST_CLOSE;
    }
}

