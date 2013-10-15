
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
 * Supports hard-linking of files.
 *
 * <p>
 * On Java &gt;= 1.7, this class uses {@link java.nio.file.Files#createLink}.
 * On Java &lt; 1.7, this class requires the <a href="https://github.com/twall/jna">JNA</a> library
 * and only works on UNIX systems.
 * </p>
 */
public final class HardLink {

    private static LibC libc;

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

        // Try JDK 1.7 Files.createLink() first
        try {
            final Class<?> path = Class.forName("java.nio.file.Path");
            Class.forName("java.nio.file.Files").getMethod("createLink", path, path).invoke(null,
              dest.getClass().getMethod("toPath").invoke(dest), src.getClass().getMethod("toPath").invoke(src));
            return;
        } catch (Exception e) {
            // ignore
        }

        // Fall-back to JNA
        if (HardLink.getLibC().link(src.toString(), dest.toString()) != 0)
            throw new IOException(HardLink.getLibC().strerror(Native.getLastError()));
    }

    private static LibC getLibC() {
        if (HardLink.libc == null)
            HardLink.libc = (LibC)Native.loadLibrary("c", LibC.class);
        return HardLink.libc;
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

