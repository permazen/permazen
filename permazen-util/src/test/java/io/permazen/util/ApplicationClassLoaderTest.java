
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import io.permazen.test.TestSupport;

import java.io.File;
import java.io.FileOutputStream;

import org.testng.annotations.Test;

public class ApplicationClassLoaderTest extends TestSupport {

    // "public class xx { }"
    private static final String CLASSFILE = "cafebabe00000034000d0a0003000a07000b07000c0100063c696e69743e0100"
                                          + "03282956010004436f646501000f4c696e654e756d6265725461626c6501000a"
                                          + "536f7572636546696c6501000778782e6a6176610c0004000501000278780100"
                                          + "106a6176612f6c616e672f4f626a656374002100020003000000000001000100"
                                          + "040005000100060000001d00010001000000052ab70001b10000000100070000"
                                          + "000600010000000100010008000000020009";

    @Test
    public void testApplicationClassLoader() throws Exception {
        final ByteData classfile = ByteData.fromHex(CLASSFILE);
        final File tempDir = this.createTempDirectory();
        final File tempFile = new File(tempDir, "xx.class");
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            classfile.writeTo(out);
        }
        final ApplicationClassLoader loader = ApplicationClassLoader.getInstance();
        loader.addURL(tempDir.toURI().toURL());

        // Find "xx"
        Class<?> cl = Class.forName("xx", false, loader);
        this.log.debug("found {}", cl);
        assert cl.getName().equals("xx");

        // Find "java.util.Map"
        cl = Class.forName("java.util.Map", false, loader);
        this.log.debug("found {}", cl);
        assert cl.getName().equals("java.util.Map");

        // Done
        tempFile.delete();
        tempDir.delete();
    }
}
