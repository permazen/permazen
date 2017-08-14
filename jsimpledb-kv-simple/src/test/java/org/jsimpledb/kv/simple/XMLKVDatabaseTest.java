
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.simple;

import java.io.File;
import java.io.IOException;

import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.kv.test.KVDatabaseTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public class XMLKVDatabaseTest extends KVDatabaseTest {

    private XMLKVDatabase xmlKV;
    private File xmlFile;

    @BeforeClass(groups = "configure")
    @Parameters("xmlFilePrefix")
    public void setTestXMLKV(@Optional String xmlFilePrefix) throws IOException {
        if (xmlFilePrefix != null) {
            this.xmlFile = File.createTempFile(xmlFilePrefix, ".xml");
            this.xmlFile.delete();                           // we need the file to not exist at first
            this.xmlFile.deleteOnExit();
            this.xmlKV = new XMLKVDatabase(this.xmlFile, 250, 5000);
        }
    }

    @AfterClass
    public void teardownRemoveXMLFile() throws Exception {
        if (this.xmlFile != null)
            this.xmlFile.delete();
    }

    @Override
    protected KVDatabase getKVDatabase() {
        return this.xmlKV;
    }
}

