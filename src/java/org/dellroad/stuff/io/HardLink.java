
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.io;

import com.sun.jna.Library;
import com.sun.jna.Native;

import java.io.File;
import java.io.IOException;

/**
 * Supports hard-linking of files on UNIX systems.
 *
 * <p>
 * This class requires the <a href="https://github.com/twall/jna">JNA</a> library.
 */
public final class HardLink {

    private static final LibC LIBC = (LibC)Native.loadLibrary("c", LibC.class);

    private HardLink() {
    }

    /**
     * Create a hard link from {@code src} to {@code dest}.
     *
     * @param src existing file to be linked to {@code dest}
     * @param dest new file that will link to {@code src}
     * @throws IOException if the operation fails
     */
    public static void link(File src, File dest) throws IOException {
        if (LIBC.link(src.toString(), dest.toString()) != 0)
            throw new IOException(LIBC.strerror(Native.getLastError()));
    }

    /**
     * Command line test method.
     */
    static void main(String[] args) throws Exception {
        HardLink.link(new File(args[0]), new File(args[1]));
    }

    private interface LibC extends Library {
        int link(String from, String to);
        String strerror(int errno);
    }
}

