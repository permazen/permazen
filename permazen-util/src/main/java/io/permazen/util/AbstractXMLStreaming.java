
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import java.util.Arrays;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * Support superclass for classes that serialize and deserialize via XML.
 */
public abstract class AbstractXMLStreaming {

    protected AbstractXMLStreaming() {
    }

// INPUT METHODS

    /**
     * Scan forward until we see an opening or closing tag.
     * If opening tag is seen, it must match one of {@code names} and then we return true, if not or if {@code names}
     * is empty throw an exception; if a closing tag is seen, return false if {@code closingOK}, else throw exception.
     *
     * @param reader XML input
     * @param closingOK true if a closing tag is OK, otherwise false
     * @param names expected opening tag, or null if we expected a closing tag
     * @return true if matching opening tag seen, false otherwise
     * @throws XMLStreamException if something unexpected is encountered
     */
    protected boolean expect(XMLStreamReader reader, boolean closingOK, QName... names) throws XMLStreamException {
        while (true) {
            if (!reader.hasNext())
                throw this.newInvalidInputException(reader, "unexpected end of input");
            final int eventType = reader.next();
            if (eventType == XMLStreamConstants.START_ELEMENT)
                break;
            if (eventType == XMLStreamConstants.END_ELEMENT) {
                if (!closingOK) {
                    throw this.newInvalidInputException(reader,
                      "expected %s but found closing <%s> tag instead",
                      this.description(names), reader.getName());
                }
                return false;
            }
        }
        if (!Arrays.asList(names).contains(reader.getName())) {
            throw this.newInvalidInputException(reader,
              "expected %s but found <%s> instead",
              this.description(names), reader.getName());
        }
        return true;
    }

    /**
     * Skip forward until either the next opening tag is reached, or the currently open tag is closed.
     *
     * @param reader XML input
     * @return the XML opening tag found, or null if a closing tag was seen first
     * @throws XMLStreamException if no opening tag is found before the current tag closes
     * @throws XMLStreamException if something unexpected is encountered
     */
    protected QName next(XMLStreamReader reader) throws XMLStreamException {
        while (true) {
            if (!reader.hasNext())
                throw this.newInvalidInputException(reader, "unexpected end of input");
            final int eventType = reader.next();
            if (eventType == XMLStreamConstants.END_ELEMENT)
                return null;
            if (eventType == XMLStreamConstants.START_ELEMENT)
                return reader.getName();
        }
    }

    /**
     * Skip over the remainder of the current XML element, including any nested elements,
     * until the closing XML tag is seen and consumed.
     *
     * @param reader XML input
     * @throws XMLStreamException if something unexpected is encountered
     */
    protected void skip(XMLStreamReader reader) throws XMLStreamException {
        for (int depth = 1; depth > 0; ) {
            if (!reader.hasNext())
                throw this.newInvalidInputException(reader, "unexpected end of input");
            switch (reader.next()) {
            case XMLStreamConstants.START_ELEMENT:
                depth++;
                break;
            case XMLStreamConstants.END_ELEMENT:
                depth--;
                break;
            default:
                break;
            }
        }
    }

    /**
     * Scan forward expecting to see a closing tag.
     *
     * <p>
     * Equivalant to: {@link #expect expect}{@code (reader, true)}.
     *
     * @param reader XML input
     * @throws XMLStreamException if something other than a closing tag is encountered
     */
    protected void expectClose(XMLStreamReader reader) throws XMLStreamException {
        this.expect(reader, true);
    }

    /**
     * Get an attribute from the current element.
     *
     * @param reader XML input
     * @param name attribute name
     * @param required whether attribute must be present
     * @return attribute value, or null if not {@code required} and no attribute is present
     * @throws IllegalStateException if the current event is not a start element event
     * @throws XMLStreamException if {@code required} is true and no such attribute is found
     */
    protected String getAttr(XMLStreamReader reader, QName name, boolean required) throws XMLStreamException {
        final String value = reader.getAttributeValue(name.getNamespaceURI(), name.getLocalPart());
        if (value == null && required) {
            throw this.newInvalidInputException(reader,
              "<%s> element is missing required \"%s\" attribute",
              reader.getName().getLocalPart(), name);
        }
        return value;
    }

    /**
     * Get a requried attribute from the current element. Equivalent to: {@code getAttr(reader, name, true)}.
     *
     * @param reader XML input
     * @param name attribute name
     * @return attribute value
     * @throws IllegalStateException if the current event is not a start element event
     * @throws XMLStreamException if no such attribute is found
     */
    protected String getAttr(XMLStreamReader reader, QName name) throws XMLStreamException {
        return this.getAttr(reader, name, true);
    }

    /**
     * Get an attribute from the current element and parse as a decimal integer value.
     *
     * @param reader XML input
     * @param name attribute name
     * @param required whether attribute must be present
     * @return attribute value, or null if not {@code required} and no attribute is present
     * @throws IllegalStateException if the current event is not a start element event
     * @throws XMLStreamException if {@code required} is true and no such attribute is found
     * @throws XMLStreamException if attribute is not an integer value
     */
    protected Integer getIntAttr(XMLStreamReader reader, QName name, boolean required) throws XMLStreamException {
        final String text = this.getAttr(reader, name, required);
        if (text == null)
            return null;
        try {
            return Integer.parseInt(text, 10);
        } catch (NumberFormatException e) {
            throw newInvalidAttributeException(reader, name, "not a valid integer value", e);
        }
    }

    /**
     * Get an attribute from the current element and parse as a decimal long value.
     *
     * @param reader XML input
     * @param name attribute name
     * @param required whether attribute must be present
     * @return attribute value, or null if not {@code required} and no attribute is present
     * @throws IllegalStateException if the current event is not a start element event
     * @throws XMLStreamException if {@code required} is true and no such attribute is found
     * @throws XMLStreamException if attribute is not an integer value
     */
    protected Long getLongAttr(XMLStreamReader reader, QName name, boolean required) throws XMLStreamException {
        final String text = this.getAttr(reader, name, required);
        if (text == null)
            return null;
        try {
            return Long.parseLong(text, 10);
        } catch (NumberFormatException e) {
            throw newInvalidAttributeException(reader, name, "not a valid long value", e);
        }
    }

    /**
     * Get a requried integer attribute from the current element. Equivalent to: {@code getIntAttr(reader, name, true)}.
     *
     * @param reader XML input
     * @param name attribute name
     * @return attribute value
     * @throws IllegalStateException if the current event is not a start element event
     * @throws XMLStreamException if no such attribute is found
     * @throws XMLStreamException if attribute is not an integer value
     */
    protected int getIntAttr(XMLStreamReader reader, QName name) throws XMLStreamException {
        return this.getIntAttr(reader, name, true);
    }

    /**
     * Get an attribute from the current element and parse as a boolean value.
     *
     * @param reader XML input
     * @param name attribute name
     * @param required whether attribute must be present
     * @return attribute value, or null if not {@code required} and no attribute is present
     * @throws IllegalStateException if the current event is not a start element event
     * @throws XMLStreamException if {@code required} is true and no such attribute is found
     * @throws XMLStreamException if attribute is not {@code "true"} or {@code "false"}
     */
    protected Boolean getBooleanAttr(XMLStreamReader reader, QName name, boolean required) throws XMLStreamException {
        final String text = this.getAttr(reader, name, required);
        if (text == null)
            return null;
        switch (text) {
        case "true":
            return true;
        case "false":
            return false;
        default:
            throw newInvalidAttributeException(reader, name, "expected either \"true\" or \"false\"");
        }
    }

    /**
     * Get a requried boolean attribute from the current element. Equivalent to: {@code getBooleanAttr(reader, name, true)}.
     *
     * @param reader XML input
     * @param name attribute name
     * @return attribute value
     * @throws IllegalStateException if the current event is not a start element event
     * @throws XMLStreamException if no such attribute is found
     * @throws XMLStreamException if attribute is not {@code "true"} or {@code "false"}
     */
    protected boolean getBooleanAttr(XMLStreamReader reader, QName name) throws XMLStreamException {
        return this.getBooleanAttr(reader, name, true);
    }

    /**
     * Build a {@link XMLStreamException} caused by invalid content in an attribute.
     *
     * @param reader XML input
     * @param name attribute name
     * @param description a description of the problem
     * @param cause optional underlying exception for chaining
     * @return exception to throw
     * @throws IllegalArgumentException if more than one {@code cause} is given
     * @throws XMLStreamException if error occurs accessing the attribute value
     */
    protected XMLStreamException newInvalidAttributeException(XMLStreamReader reader,
      QName name, String description, Throwable... cause) throws XMLStreamException {
        final String value = this.getAttr(reader, name, false);
        final String message = String.format("<%s> element attribute \"%s\" %s is invalid: %s",
          reader.getName().getLocalPart(), name, value != null ? "value \"" + value + "\"" : "left unspecified", description);
        switch (cause.length) {
        case 0:
            return this.newInvalidInputException(reader, "%s", message);
        case 1:
            return this.newInvalidInputException(reader, cause[0], "%s", message);
        default:
            throw new IllegalArgumentException("invalid multiple causes");
        }
    }

    /**
     * Build a {@link XMLStreamException} caused by invalid input.
     *
     * @param reader XML input
     * @param format format string for {@link String#format String.format()}
     * @param args format arguments for {@link String#format String.format()}
     * @return exception to throw
     */
    protected XMLStreamException newInvalidInputException(XMLStreamReader reader, String format, Object... args) {
        return this.newInvalidInputException(reader, null, format, args);
    }

    /**
     * Build a {@link XMLStreamException} caused by invalid input.
     *
     * <p>
     * This method exists to help workaround <a href="https://bugs.openjdk.org/browse/JDK-8322027">JDK-8322027</a>.
     *
     * @param reader XML input
     * @param cause optional underlying exception for chaining, or null for none
     * @param format format string for {@link String#format String.format()}
     * @param args format arguments for {@link String#format String.format()}
     * @return exception to throw
     */
    protected XMLStreamException newInvalidInputException(XMLStreamReader reader, Throwable cause, String format, Object... args) {
        final XMLStreamException e = new XMLStreamException(String.format(format, args), reader.getLocation());
        if (cause != null)
            e.initCause(cause);
        return e;
    }

    private String description(QName[] names) {
        switch (names.length) {
        case 0:
            return "closing tag";
        case 1:
            return "opening <" + names[0] + "> tag";
        default:
            final StringBuilder buf = new StringBuilder();
            for (QName name : names) {
                if (buf.length() == 0)
                    buf.append("one of ");
                else
                    buf.append(", ");
                buf.append('<').append(name).append('>');
            }
            return buf.toString();
        }
    }

// OUTPUT METHODS

    /**
     * Write out a simple XML element containing the given content.
     *
     * @param writer XML output
     * @param element element name
     * @param content simple content
     * @throws XMLStreamException if error occurs writing output
     */
    protected void writeElement(XMLStreamWriter writer, QName element, String content) throws XMLStreamException {
        writer.writeStartElement(element.getNamespaceURI(), element.getLocalPart());
        writer.writeCharacters(content);
        writer.writeEndElement();
    }

    /**
     * Start an empty XML element.
     *
     * @param writer XML output
     * @param name element name
     * @throws XMLStreamException if error occurs writing output
     */
    protected void writeEmptyElement(XMLStreamWriter writer, QName name) throws XMLStreamException {
        writer.writeEmptyElement(name.getNamespaceURI(), name.getLocalPart());
    }

    /**
     * Start a non-empty XML element.
     *
     * @param writer XML output
     * @param name element name
     * @throws XMLStreamException if error occurs writing output
     */
    protected void writeStartElement(XMLStreamWriter writer, QName name) throws XMLStreamException {
        writer.writeStartElement(name.getNamespaceURI(), name.getLocalPart());
    }

    /**
     * Write out an attribute.
     *
     * @param writer XML output
     * @param name attribute qualified name
     * @param value attribute value
     * @throws XMLStreamException if error occurs writing output
     */
    protected void writeAttr(XMLStreamWriter writer, QName name, Object value) throws XMLStreamException {
        writer.writeAttribute(name.getNamespaceURI(), name.getLocalPart(), String.valueOf(value));
    }
}
