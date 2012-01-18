
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import javax.xml.namespace.QName;

/**
 * XML constants used by {@link PersistentObject}.
 */
public final class XMLConstants {

    public static final String NAMESPACE_URI = "http://dellroad-stuff.googlecode.com/ns/persistentObject";
    public static final String XML_PREFIX = "pobj";
    public static final QName UPDATES_ELEMENT_NAME = new QName(NAMESPACE_URI, "updates", XML_PREFIX);
    public static final QName UPDATE_ELEMENT_NAME = new QName(NAMESPACE_URI, "update", XML_PREFIX);
    public static final QName XMLNS_ATTRIBUTE_NAME = new QName("http://www.w3.org/2000/xmlns/", XML_PREFIX, "xmlns");

    private XMLConstants() {
    }
}

