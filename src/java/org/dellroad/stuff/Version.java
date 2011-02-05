
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Contains library version information.
 */
public final class Version {

    /**
     * The version of this library.
     */
    public static final String DELLROAD_STUFF_VERSION;

    private static final String PROPERTIES_RESOURCE = "/dellroad-stuff.properties";
    private static final String VERSION_PROPERTY_NAME = "dellroad-stuff.version";

    static {
        Properties properties = new Properties();
        InputStream input = Version.class.getResourceAsStream(PROPERTIES_RESOURCE);
        if (input == null)
            throw new RuntimeException("can't find resource " + PROPERTIES_RESOURCE);
        try {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }
        }
        DELLROAD_STUFF_VERSION = properties.getProperty(VERSION_PROPERTY_NAME, "?");
    }

    private Version() {
    }
}
