
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

import com.google.common.base.Preconditions;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.jsimpledb.util.AbstractXMLStreaming;

/**
 * Parses {@link KVImplementation} descriptor files.
 *
 * <p>
 * Example:
 * <blockquote><code>
 *  &lt;kv-implementations&gt;
 *      &lt;kv-implementation class="com.example.MyKVImplementation"/&gt;
 *  &lt;/kv-implementations&gt;
 * </code></blockquote>
 */
public class KVImplementationsReader extends AbstractXMLStreaming {

    public static final QName KV_IMPLEMENTATIONS_TAG = new QName("kv-implementations");
    public static final QName KV_IMPLEMENTATION_TAG = new QName("kv-implementation");
    public static final QName CLASS_ATTR = new QName("class");

    private final InputStream input;

    public KVImplementationsReader(InputStream input) {
        Preconditions.checkArgument(input != null, "null input");
        this.input = input;
    }

    /**
     * Parse the XML input and return the list of {@link KVImplementation} class names.
     *
     * @return list of class names
     */
    public List<String> parse() throws XMLStreamException {
        final ArrayList<String> list = new ArrayList<>();
        final XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(this.input);
        this.expect(reader, false, KV_IMPLEMENTATIONS_TAG);
        while (this.expect(reader, true, KV_IMPLEMENTATION_TAG)) {
            list.add(this.getAttr(reader, CLASS_ATTR, true));
            this.expectClose(reader);
        }
        return list;
    }
}
