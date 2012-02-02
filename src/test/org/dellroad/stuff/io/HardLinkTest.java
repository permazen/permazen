
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import java.io.File;
import java.io.IOException;

import org.dellroad.stuff.TestSupport;
import org.testng.annotations.Test;

public class HardLinkTest extends TestSupport {

    private byte[] data;

    @Test
    public void testHardLink() throws Exception {

        // Create files
        File file1 = File.createTempFile("HardLinkTest", ".file1");
        File file2 = File.createTempFile("HardLinkTest", ".file1");
        File file3 = File.createTempFile("HardLinkTest", ".file1");

        // Delete file2 for test
        file2.delete();

        // Do tests
        try {
            HardLink.link(file2, file1);
            assert false;
        } catch (IOException e) {
            // expected: file2 does not exist
        }
        HardLink.link(file1, file2);
        assert file1.exists();
        assert file2.exists();
        assert file3.exists();
        try {
            HardLink.link(file1, file3);
            assert false;
        } catch (IOException e) {
            // expected: file3 already exists
        }

        // Clean up
        file1.delete();
        file2.delete();
        file3.delete();
    }
}

