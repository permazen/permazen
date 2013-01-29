
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;

import org.dellroad.stuff.TestSupport;
import org.jibx.extras.DocumentComparator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class UpdatesXMLEventTest extends TestSupport {

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
    private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();

    @Test(dataProvider = "files")
    public void test(String file) throws Exception {

        // Create temp files
        File temp1 = File.createTempFile("test1.", "xml");
        File temp2 = File.createTempFile("test2.", "xml");
        File temp3 = File.createTempFile("test3.", "xml");

        // Copy input file into temp file
        InputStream input = this.getClass().getResourceAsStream(file);
        assertNotNull(input);
        this.copy(input, false, temp1, null);
        input.close();

        // Copy temp1 to temp2 adding updates
        String[] updatesOut = new String[] { "foo", "bar", "happy day" };
        this.copy(temp1, false, temp2, updatesOut);

        // Copy temp2 to temp3 removing updates
        List<String> updatesIn = this.copy(temp2, true, temp3, null);

        // Compare lists
        assertEquals(updatesIn, Arrays.asList(updatesOut));

        // Compare XML documents
        InputStreamReader reader1 = new InputStreamReader(new FileInputStream(temp1), "UTF-8");
        InputStreamReader reader2 = new InputStreamReader(new FileInputStream(temp3), "UTF-8");
        assert new DocumentComparator(System.out, false).compare(reader1, reader2) : "different XML";
        reader1.close();
        reader2.close();

        // Clean up
        temp1.delete();
        temp2.delete();
        temp3.delete();
    }

    private List<String> copy(File file1, boolean readUpdates, File file2, String[] updates)
      throws IOException, XMLStreamException {
        FileInputStream input = new FileInputStream(file1);
        try {
            return this.copy(input, readUpdates, file2, updates);
        } finally {
            input.close();
        }
    }

    private List<String> copy(InputStream input, boolean readUpdates, File file, String[] updates)
      throws IOException, XMLStreamException {
        FileOutputStream output = new FileOutputStream(file);
        XMLEventReader reader = this.xmlInputFactory.createXMLEventReader(input);
        if (readUpdates)
            reader = new UpdatesXMLEventReader(reader);
        XMLEventWriter writer = this.xmlOutputFactory.createXMLEventWriter(output, "UTF-8");
        if (updates != null)
            writer = new UpdatesXMLEventWriter(writer, Arrays.asList(updates));
        writer.add(reader);
        writer.close();
        output.close();
        return readUpdates ? ((UpdatesXMLEventReader)reader).getUpdates() : null;
    }

    @DataProvider(name = "files")
    public Object[][] generateFiles() {
        return new Object[][] {
            new Object[] { "file1.xml" },
            new Object[] { "file2.xml" },
            new Object[] { "file3.xml" }
        };
    }
}

