
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StreamWriterActionTest extends TestSupport {

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
    private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();

    @Test(dataProvider = "files")
    public void testCopy(String inputResource) throws Exception {
        final File actualFile = File.createTempFile("test1.", "xml");
        final String input = this.readResource(inputResource);
        this.copy(input, actualFile);
        final String actual = this.readResource(actualFile);
        final String expected = this.readResource(inputResource);
        Assert.assertEquals(actual.trim(), expected.trim());

        // Clean up
        actualFile.delete();
    }

    private void copy(String input, File outputFile) throws Exception {
        this.xmlInputFactory.setProperty("http://java.sun.com/xml/stream/properties/report-cdata-event", true);
        final XMLStreamReader reader = this.xmlInputFactory.createXMLStreamReader(
          new ByteArrayInputStream(input.getBytes(Charset.forName("UTF-8"))));
        XMLStreamWriter writer = this.xmlOutputFactory.createXMLStreamWriter(new FileOutputStream(outputFile), "UTF-8");
        writer = new EmptyTagXMLStreamWriter(new IndentXMLStreamWriter(writer, 4));
        while (true) {
            final StreamWriterAction action = StreamWriterAction.of(reader);
            this.log.info("ACTION = " + action);
            action.apply(writer);
            if (!reader.hasNext())
                break;
            reader.next();
        }
        writer.close();
    }

    @DataProvider(name = "files")
    public Object[][] generateFiles() {
        return new Object[][] {
            new Object[] { "action1.xml" },
        };
    }
}

