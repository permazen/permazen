
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.StringReader;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stream.StreamSource;

import org.dellroad.stuff.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EmptyTagXMLStreamWriterTest extends TestSupport {

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
    private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();

    @Test(dataProvider = "files")
    public void testEmptyTag(String inputResource, String expectedResource) throws Exception {
        final File actualFile = File.createTempFile("test1.", "xml");
        final String input = this.readResource(inputResource);
        this.consolidateTags(input, actualFile);
        final String actual = this.readResource(actualFile);
        final String expected = this.readResource(expectedResource);
        Assert.assertEquals(actual.trim(), expected.trim());

        // Clean up
        actualFile.delete();
    }

    private void consolidateTags(String input, File outputFile) throws Exception {
        final XMLStreamWriter writer = new EmptyTagXMLStreamWriter(new IndentXMLStreamWriter(
          this.xmlOutputFactory.createXMLStreamWriter(new FileOutputStream(outputFile), "UTF-8"), 4));
        TransformerFactory.newInstance().newTransformer().transform(
          new StreamSource(new StringReader(input)), new StAXResult(writer));
        writer.close();
    }

    @DataProvider(name = "files")
    public Object[][] generateFiles() {
        return new Object[][] {
            new Object[] { "input1.xml", "output1a.xml" },
            new Object[] { "input4.xml", "output4.xml" },
            new Object[] { "input5.xml", "output5.xml" },
        };
    }
}

